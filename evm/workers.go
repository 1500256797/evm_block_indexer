package evm

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"sort"

	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/prisma/prisma-client-go/runtime/transaction"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

var (
	cursor uint64 = 0
)

// ===============================================================
// 消费者 worker
// ===============================================================

type Worker struct {
	Redis    *redis.Client
	Provider *ethclient.Client
	Ctx      *context.Context
	DB       *db.PrismaClient
}

// 用hscan代替hgetall

// ===============================================================
// go 携程并发数量控制
// ===============================================================
type Limit struct {
	number  int
	channel chan struct{}
}

/**
 * @Description: New 方法：创建有限的 go f 函数的 goroutine
 * @param  number int
 * @return *Limit
 */
func New(number int) *Limit {
	return &Limit{
		number:  number,
		channel: make(chan struct{}, number),
	}
}

/**
 * @Description: Run 方法：创建有限的 go f 函数的 goroutine
 * @param  f func()
 */
func (limit *Limit) Run(f func()) {
	limit.channel <- struct{}{}
	go func() {
		f()
		<-limit.channel
	}()
}

/**
 * @Description: NewWorker 新建消费者
 * @param  redis *redis.Client
 * @param  provider *ethclient.Client
 * @param  ctx *context.Context
 * @param  db *db.PrismaClient
 * @return *Worker
 */
func NewWorker(redis *redis.Client, provider *ethclient.Client, ctx *context.Context, db *db.PrismaClient) *Worker {
	return &Worker{
		Redis:    redis,
		Provider: provider,
		Ctx:      ctx,
		DB:       db,
	}
}

// 更新provider
func (w *Worker) UpdateProvider(provider *ethclient.Client) {
	w.Provider = provider
}

// 检查db是否连续
func (w *Worker) CheckDBIsContinue() {
	// 将num 映射到结构体中
	var missBlockNumbers []struct {
		Num int `json:"num"` // 区块号字段要大写开头 坑
	}
	findMissBlockNumbersSql := `
	WITH RECURSIVE missing_numbers AS (
		SELECT MIN(block_number) AS num FROM "EVMBlcockNumber" -- 找到最小的数字作为起始
		UNION ALL
		SELECT num + 1 FROM missing_numbers WHERE num + 1 <= (SELECT MAX(block_number) FROM "EVMBlcockNumber") -- 递归生成连续数字序列
	  )
	  SELECT num FROM missing_numbers
	  LEFT JOIN "EVMBlcockNumber" ON missing_numbers.num = "EVMBlcockNumber".block_number
	  WHERE "EVMBlcockNumber".block_number IS NULL
	  ORDER BY num
	`

	err := w.DB.Prisma.QueryRaw(findMissBlockNumbersSql).Exec(*w.Ctx, &missBlockNumbers)
	if err != nil {
		log.Fatal("Sorry ,Cannot get the missing block number in the database.please check the database connection.", err)
	}
	// 如果missBlockNumbers为空 说明数据库中的区块号是连续的
	if len(missBlockNumbers) == 0 {
		fmt.Println(time.Now().Format(TIME_FORMAT), "Congratulations! The block number in the database is continuous. 🎉")
		return
	}
	// 如果缺失的元素为0 说明数据库中的区块号是连续的
	if len(missBlockNumbers) == 1 && missBlockNumbers[0].Num == 0 {
		fmt.Println(time.Now().Format(TIME_FORMAT), "Congratulations! The block number in the database is continuous. 🎉")
		return
	}
	// 如果missBlockNumbers不为空 说明数据库中的区块号是不连续的
	fmt.Println(time.Now().Format(TIME_FORMAT), "Dot worry ,The block number in the database is not continuous,but we will repair it. 🎉")
	// 将missBlockNumbers中的区块号重到db中
	ops := make([]transaction.Param, 0)
	for _, blockNumber := range missBlockNumbers {
		// 利用transaction事务批量写入
		txInsert := w.DB.EVMBlcockNumber.CreateOne(
			db.EVMBlcockNumber.BlockNumber.Set(blockNumber.Num),
			db.EVMBlcockNumber.UpdatedAt.Set(time.Now().UTC()),
			db.EVMBlcockNumber.CreatedAt.Set(time.Now().UTC()),
		).Tx()
		// 将transaction.Param对象添加到数组中
		ops = append(ops, txInsert)
	}
	ctx := context.Background()
	if err := w.DB.Prisma.Transaction(ops...).Exec(ctx); err != nil {
		log.Fatal("Sorry ,We cant repair the block number in the database.please check the database connection.", err)
	}
	fmt.Println(time.Now().Format(TIME_FORMAT), "The block number in the database has been repaired. 🎉")
}

// 将redis中的all_block_number中的区块号排序后写入到db这
func (w *Worker) DumpRedisTaskToDBAndSplitTask() {
	// 获取所有的区块
	all_block_numbers, err := w.Redis.SMembers(*w.Ctx, ALL_BLOCK_NUMBER).Result()
	if err != nil {
		log.Fatal("Error getting block number from Redis", err)
	}
	// 如果all_block_numbers为空 则说明redis中没有数据
	if len(all_block_numbers) == 0 {
		return
	}
	// 对并集进行排序
	sort.Strings(all_block_numbers)

	// 获取所有已消费的区块
	consumed_block_numbers, _ := w.Redis.SMembers(*w.Ctx, CONSUMED_BLOCK_NUMBER).Result()
	// 取头尾两个区块
	first_block_number_str := all_block_numbers[0]
	first_block_number, _ := strconv.Atoi(first_block_number_str)
	last_block_number_str := all_block_numbers[len(all_block_numbers)-1]
	last_block_number, _ := strconv.Atoi(last_block_number_str)
	// 比较数据库中最大的区块号和redis中最大的区块号进行比较 然后将二者之间的区块号写入到db中
	maxBlockNumber, err := w.DB.EVMBlcockNumber.FindFirst().OrderBy(
		// 按照区块号降序排列
		db.EVMBlcockNumber.BlockNumber.Order(db.SortOrderDesc),
	).Exec(*w.Ctx)
	if err != nil {
		log.Fatal("Error getting max block number from db", err)
	}
	// 打印db中的最大区块号
	fmt.Println(time.Now().Format(TIME_FORMAT), "maxBlockNumber from db is", maxBlockNumber.BlockNumber)
	// 如果数据库中的最大区块号不为空 则说明数据库中有数据
	if maxBlockNumber.BlockNumber != 0 {
		// 如果数据库中的最大区块号大于redis中的最大区块号 则说明数据库中的数据是最新的
		if maxBlockNumber.BlockNumber >= last_block_number {
			fmt.Println(time.Now().Format(TIME_FORMAT), "maxBlockNumber from db is newer than redis")
			// 删除所有区块 删除未消费区块  删除已经消费区块
			err = w.Redis.Del(*w.Ctx, ALL_BLOCK_NUMBER, NO_CONSUMED_BLOCK_NUMBER, CONSUMED_BLOCK_NUMBER).Err()
			if err != nil {
				log.Fatal("Error removing block number from Redis", err)
			}
			// 从数据库中加载过去的1w个未消费的区块
			unConsumedBlockerNumbers, err := w.DB.EVMBlcockNumber.FindMany().Take(10000).OrderBy(
				// 按照区块号降序排列
				db.EVMBlcockNumber.BlockNumber.Order(db.SortOrderDesc),
			).Exec(*w.Ctx)
			if err != nil {
				log.Fatal("Error getting 10000 unConsumedBlockerNumbers from db", err)
			}
			// 将未消费的区块号用pipeline的方式写入到redis中
			pipe := w.Redis.Pipeline()
			pipe2 := w.Redis.Pipeline()
			for _, blockNumber := range unConsumedBlockerNumbers {
				if blockNumber.ConsumeStatus == 0 {
					pipe.SAdd(*w.Ctx, NO_CONSUMED_BLOCK_NUMBER, blockNumber.BlockNumber)
				}
				pipe2.SAdd(*w.Ctx, ALL_BLOCK_NUMBER, blockNumber.BlockNumber)
			}
			_, err = pipe.Exec(*w.Ctx)
			if err != nil {
				log.Fatal("Error adding block number to Redis", err)
			}
			_, err = pipe2.Exec(*w.Ctx)
			if err != nil {
				log.Fatal("Error adding block number to Redis", err)
			}
			return
		}
		// 如果数据库中的最大区块号小于redis中的最大区块号 则说明redis中的数据是最新的
		if maxBlockNumber.BlockNumber < last_block_number {
			fmt.Println(time.Now().Format(TIME_FORMAT), "maxBlockNumber from db is older than redis")
			// 从数据库中最大的区块号开始写入
			first_block_number = maxBlockNumber.BlockNumber
		}
	}
	// 插入区块
	ops := make([]transaction.Param, 0)
	// 遍历头尾两个区块之间的所有区块 并写入到db中
	for i := first_block_number; i <= last_block_number; i++ {
		// 利用transaction事务批量写入
		txInsert := w.DB.EVMBlcockNumber.UpsertOne(
			// query
			db.EVMBlcockNumber.BlockNumber.Equals(i),
		).Create(
			db.EVMBlcockNumber.BlockNumber.Set(i),
			db.EVMBlcockNumber.UpdatedAt.Set(time.Now().UTC()),
			db.EVMBlcockNumber.CreatedAt.Set(time.Now().UTC()),
		).Update().Tx()
		// 将transaction.Param对象添加到数组中
		ops = append(ops, txInsert)
		if len(ops) == 10000 {
			ctx := context.Background()
			if err := w.DB.Prisma.Transaction(ops...).Exec(ctx); err != nil {
				log.Fatal("Error inserting block number to db  ❌", err)
			}
			// 清空ops
			ops = make([]transaction.Param, 0)
		}
	}
	// 剩余的数据
	if len(ops) > 0 {
		ctx := context.Background()
		if err := w.DB.Prisma.Transaction(ops...).Exec(ctx); err != nil {
			log.Fatal("Error inserting block number to db  ❌", err)
		}
	}
	// 更新区块状态
	updateOps := make([]transaction.Param, 0)
	// 遍历所有已消费的区块 并更新db中的数据
	for _, blockNumber := range consumed_block_numbers {
		consumed_block_number, _ := strconv.Atoi(blockNumber)
		// 利用transaction事务批量写入
		txUpdate := w.DB.EVMBlcockNumber.FindUnique(
			db.EVMBlcockNumber.BlockNumber.Equals(consumed_block_number),
		).Update(
			db.EVMBlcockNumber.ConsumeStatus.Set(1),
		).Tx()
		// 将transaction.Param对象添加到数组中
		updateOps = append(updateOps, txUpdate)
		if len(updateOps) == 10000 {
			ctx := context.Background()
			if err := w.DB.Prisma.Transaction(updateOps...).Exec(ctx); err != nil {
				log.Fatal("Error inserting block number to db  ❌", err)
			}
			// 清空
			updateOps = make([]transaction.Param, 0)
		}
	}
	// 剩余的数据
	if len(updateOps) > 0 {
		ctx := context.Background()
		if err := w.DB.Prisma.Transaction(updateOps...).Exec(ctx); err != nil {
			log.Fatal("Error inserting block number to db  ❌", err)
		}

	}
	// 删除所有区块 删除未消费区块  删除已经消费区块
	err = w.Redis.Del(*w.Ctx, ALL_BLOCK_NUMBER, NO_CONSUMED_BLOCK_NUMBER, CONSUMED_BLOCK_NUMBER).Err()
	if err != nil {
		log.Fatal("Error removing block number from Redis", err)
	}
	// 从数据库中加载过去的1w个区块
	loadBlockerNumbers, err := w.DB.EVMBlcockNumber.FindMany().Take(10000).OrderBy(
		// 按照区块号降序排列
		db.EVMBlcockNumber.BlockNumber.Order(db.SortOrderDesc),
	).Exec(*w.Ctx)
	if err != nil {
		log.Fatal("Error getting 10000 block numbers from db", err)
	}
	// 将未消费的区块号用pipeline的方式写入到redis中
	pipe := w.Redis.Pipeline()
	pipe2 := w.Redis.Pipeline()
	pipe3 := w.Redis.Pipeline()
	for _, blockNumber := range loadBlockerNumbers {
		if blockNumber.ConsumeStatus == 0 {
			pipe.SAdd(*w.Ctx, NO_CONSUMED_BLOCK_NUMBER, blockNumber.BlockNumber)
		} else {
			pipe3.SAdd(*w.Ctx, CONSUMED_BLOCK_NUMBER, blockNumber.BlockNumber)
		}
		pipe2.SAdd(*w.Ctx, ALL_BLOCK_NUMBER, blockNumber.BlockNumber)
	}
	_, err = pipe.Exec(*w.Ctx)
	if err != nil {
		log.Fatal("Error adding block number to Redis", err)
	}
	_, err = pipe2.Exec(*w.Ctx)
	if err != nil {
		log.Fatal("Error adding block number to Redis", err)
	}
	_, err = pipe3.Exec(*w.Ctx)
	if err != nil {
		log.Fatal("Error adding block number to Redis", err)
	}

}

func (w *Worker) ProcessBlock(blockNumber uint64) error {
	// 判断provider是否为空
	if w.Provider == nil {
		return fmt.Errorf("provider is nil")
	}
	block, _ := w.Provider.BlockByNumber(context.Background(), big.NewInt(int64(blockNumber)))
	if block == nil {
		fmt.Println(time.Now().Format(TIME_FORMAT), blockNumber, " : block is nil 🛑")
		// retry do
		return fmt.Errorf("block is nil,%d", blockNumber)
	}
	blockTxs := len(block.Transactions())
	fmt.Println(time.Now().Format(TIME_FORMAT), "start parse blockNumber:", blockNumber, "该区块交易数：", blockTxs)
	// 把1 转为big.Int
	chainID := big.NewInt(1)
	// 获取区块中的交易列表
	for _, tx := range block.Transactions() {
		goFunc := func(tx *types.Transaction, block *types.Block) {
			// 获取from地址
			from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
			if err != nil {
				logx.Info("Failed to get from address: ", err)
			}
			bytes4Data := tx.Data()
			// 如果tx.Data()长度< 4 则不用[:4]
			if len(tx.Data()) > 4 {
				bytes4Data = tx.Data()[:4]
			}
			to := tx.To()
			if to == nil {
				to = &common.Address{}
			}

			txObj := &Transaction{
				Hash:           tx.Hash().Hex(),
				BlockNumber:    blockNumber,
				BlockHash:      block.Hash().Hex(),
				From:           from.Hex(),
				To:             to.Hex(),
				Value:          tx.Value().String(),
				Nonce:          tx.Nonce(),
				Byte4Data:      hex.EncodeToString(bytes4Data),
				Timestamp:      time.Now(),
				Type:           int(tx.Type()),
				ChainID:        int(chainID.Int64()),
				Confirmations:  0,
				BlockTxs:       blockTxs,
				GasPrice:       tx.GasPrice().String(),
				TransactionFee: tx.GasPrice().Mul(big.NewInt(int64(tx.Gas())), big.NewInt(1)).String(),
			}
			// 将Transaction marshal成json字符串
			txJson, _ := json.Marshal(txObj)
			// 保存对象到redis中  hash名为transaction key为tx.Hash
			err = w.Redis.HSet(*w.Ctx, TRANSACTIONS, txObj.Hash, txJson).Err()
			if err != nil {
				logx.Info("hash set error", err)
			}
		}
		goFunc(tx, block)
	}

	// 判断blocknumber是否还在redis中
	isExists := w.Redis.SIsMember(*w.Ctx, CONSUMING_BLOCK_NUMBER, fmt.Sprint(blockNumber)).Val()
	if !isExists {
		return nil
	}
	// 将区块号消费队列中移除 并加入到已消费队列中
	err := w.Redis.SRem(*w.Ctx, CONSUMING_BLOCK_NUMBER, fmt.Sprint(blockNumber)).Err()
	if err != nil {
		logx.Info("Error removing block number from Redis", err)
		return err
	}
	err = w.Redis.SAdd(*w.Ctx, CONSUMED_BLOCK_NUMBER, fmt.Sprint(blockNumber)).Err()
	if err != nil {
		logx.Info("Error adding block number to Redis", err)
		return err
	}
	fmt.Println(time.Now().Format(TIME_FORMAT), "complete parse blockNumber:", blockNumber, "该区块交易数：", blockTxs, "✅")
	return nil
}

// 解析老区块
func (w *Worker) ParseOldBlock(blockNumber uint64) error {
	err := w.ProcessBlock(blockNumber)
	if err != nil {
		return err
	}
	return nil
}

// 项目启动时 优先处理消费队列中的区块
func (w *Worker) ProcessConsumimgBlock() {
	// 从正在消费队列中获取区块号 开启10个goroutine并发处理
	for i := 0; i < 10; i++ {
		go func() {
			for {
				blockNumber, err := w.Redis.SPop(*w.Ctx, CONSUMING_BLOCK_NUMBER).Result()
				// string 转 uint64
				blockNumberUint64, _ := strconv.ParseUint(blockNumber, 10, 64)

				if err != nil {
					logx.Info("Error getting block number from Redis", err)
				}
				if blockNumber == "" {
					break
				}
				err = w.ProcessBlock(blockNumberUint64)
				if err != nil {
					fmt.Println("Error processing block", err)
				}
			}
		}()
	}

}

// 将redis中的数据保存区块号到db只有当下面几个条件同时满足时才会写入。
// 只有当all_block_number = consumed_block_number时才保存 说明当前项目一直在消费新区块 老区块已经消费完毕
// 只有当lastBlockNumber-firstBlockNumber+1 != len(consumedBlockNumbers) ，说明程序运行过程中出现了错误，导致部分区块没有监听到，所以不连续
// 数据库的区块一定是和redis中的区块一致的，redis中的区块一定是最新的而且是连续的 所以只需要插入和更新即可，所以不需要和数据库中的区块进行比较
func (w *Worker) SaveBlockNumberToDB() error {
	// 定时任务 遇到panic不处理  忽略错误 继续执行
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	// 获取所有的区块号
	allBlockNumbers, err := w.Redis.SMembers(*w.Ctx, ALL_BLOCK_NUMBER).Result()
	if err != nil {
		return err
	}
	// 从Consum中获取区块号
	consumedBlockNumbers, err := w.Redis.SMembers(*w.Ctx, CONSUMED_BLOCK_NUMBER).Result()
	if err != nil {
		return err
	}
	// len == 0 return
	if len(allBlockNumbers) == 0 {
		return nil
	}
	// 判断是否相等
	if len(allBlockNumbers) != len(consumedBlockNumbers) {
		return nil
	}

	// 对结果进行排序 并取头尾区块 检查是否连续
	sort.Strings(consumedBlockNumbers)
	// 取头尾两个区块
	firstBlockNumberStr := consumedBlockNumbers[0]
	firstBlockNumber, _ := strconv.Atoi(firstBlockNumberStr)
	// 判断是否连续，尾区块号-头区块号+1=区块数
	lastBlockNumberStr := consumedBlockNumbers[len(consumedBlockNumbers)-1]
	lastBlockNumber, _ := strconv.Atoi(lastBlockNumberStr)
	if lastBlockNumber-firstBlockNumber+1 != len(consumedBlockNumbers) {
		missBlockNumbers := (lastBlockNumber - firstBlockNumber + 1) - len(consumedBlockNumbers)
		// 如果不连续 则不保存
		fmt.Println(time.Now().Format(TIME_FORMAT), "find missed block numbers from consumed block quene is  ", missBlockNumbers, " we will not save block number to db")
		return nil
	}

	// 利用pipeline 批量删除已经写入到db中的区块
	all_block_number_pipeline := w.Redis.Pipeline()
	consumed_block_number_pipeline := w.Redis.Pipeline()
	ops := make([]transaction.Param, 0)
	// 遍历头尾两个区块之间的所有区块 并写入到db中
	for i := firstBlockNumber; i <= lastBlockNumber; i++ {
		// 利用transaction事务批量写入
		txInsert := w.DB.EVMBlcockNumber.UpsertOne(
			// query
			db.EVMBlcockNumber.BlockNumber.Equals(i),
		).Create(
			db.EVMBlcockNumber.BlockNumber.Set(i),
			db.EVMBlcockNumber.UpdatedAt.Set(time.Now().UTC()),
			db.EVMBlcockNumber.CreatedAt.Set(time.Now().UTC()),
			db.EVMBlcockNumber.ConsumeStatus.Set(1),
		).Update(
			db.EVMBlcockNumber.ConsumeStatus.Set(1),
		).Tx()
		// 将transaction.Param对象添加到数组中
		ops = append(ops, txInsert)
		if len(ops) == 10000 {
			ctx := context.Background()
			if err := w.DB.Prisma.Transaction(ops...).Exec(ctx); err != nil {
				logx.Info("transactions insert error ❌: ", err)
			}
			// 清空ops
			ops = make([]transaction.Param, 0)
		}
		// add to pipeline
		all_block_number_pipeline.SRem(*w.Ctx, ALL_BLOCK_NUMBER, i)
		consumed_block_number_pipeline.SRem(*w.Ctx, CONSUMED_BLOCK_NUMBER, i)
	}
	// 剩余的数据
	if len(ops) > 0 {
		ctx := context.Background()
		if err := w.DB.Prisma.Transaction(ops...).Exec(ctx); err != nil {
			logx.Info("transactions insert error ❌: ", err)
		}
	}
	// 执行pipeline
	_, err = all_block_number_pipeline.Exec(*w.Ctx)
	if err != nil {
		logx.Info("Error removing block number from Redis", err)
		return err
	}
	_, err = consumed_block_number_pipeline.Exec(*w.Ctx)
	if err != nil {
		logx.Info("Error removing block number from Redis", err)
		return err
	}
	fmt.Println(time.Now().Format(TIME_FORMAT), "save block number from ", firstBlockNumber, " to ", lastBlockNumber, "to db complete.....🟢")
	return nil
}

// 将transaction保存到数据库中
func (w *Worker) SaveTransactionToDB(rowsNum int) error {
	// 定时任务 遇到panic不处理  忽略错误 继续执行
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	// 开始时间
	data := make(map[string]TransactionHashScan)
	keys, newCursor, err := w.Redis.HScan(*w.Ctx, TRANSACTIONS, cursor, "*", int64(rowsNum)).Result()
	if err != nil {
		return err
	}
	// 分批获取字段的值
	batchSize := rowsNum
	for i := 0; i < len(keys); i += batchSize {
		endIndex := i + batchSize
		if endIndex > len(keys) {
			endIndex = len(keys)
		}
		batchFields := keys[i:endIndex]

		// 使用HMGET命令一次性获取多个字段的值
		values, err := w.Redis.HMGet(*w.Ctx, TRANSACTIONS, batchFields...).Result()
		if err != nil {
			return err
		}

		// 解析JSON字符串到结构体并存储到map中
		for j, field := range batchFields {
			value := values[j]
			var transactionHashScan TransactionHashScan
			if jsonStr, ok := value.(string); ok {
				if err := json.Unmarshal([]byte(jsonStr), &transactionHashScan); err != nil {
					return fmt.Errorf("failed to unmarshal JSON for field %s: %v", field, err)
				}
			}
			data[field] = transactionHashScan
		}
	}

	if len(data) == 0 {
		fmt.Println(time.Now().Format(TIME_FORMAT), "transaction result len = 0")
		// 查不到数据就重置cursor
		cursor = 0
		return nil
	}
	count := 0
	// 使用ops := make([]transaction.Param, 0) 批量插入
	ops := make([]transaction.Param, 0)
	// 使用pipeline 代替for循环删除
	hDelPipeline := w.Redis.Pipeline()
	for _, tx := range data {
		txParam := w.DB.EVMTransaction.UpsertOne(
			// query
			db.EVMTransaction.TxHashBlockNumber(
				db.EVMTransaction.TxHash.Equals(tx.Hash),
				db.EVMTransaction.BlockNumber.Equals(int(tx.BlockNumber)),
			),
		).Create(
			db.EVMTransaction.TxHash.Set(tx.Hash),
			db.EVMTransaction.BlockNumber.Set(int(tx.BlockNumber)),
			db.EVMTransaction.BlockHash.Set(tx.BlockHash),
			db.EVMTransaction.From.Set(tx.From),
			db.EVMTransaction.To.Set(tx.To),
			db.EVMTransaction.TransactionFee.Set(tx.TranactionFee),
			db.EVMTransaction.GasPrice.Set(tx.GasPrice),
			db.EVMTransaction.Value.Set(tx.Value),
			db.EVMTransaction.Nonce.Set(int(tx.Nonce)),
			db.EVMTransaction.Byte4Data.Set(tx.Byte4Data),
			db.EVMTransaction.Timestamp.Set(int(tx.Timestamp.Unix())),
			db.EVMTransaction.Type.Set(tx.Type),
			db.EVMTransaction.ChainID.Set(tx.ChainID),
			db.EVMTransaction.Confirmations.Set(tx.Confirmations),
			db.EVMTransaction.BlockTxs.Set(tx.BlockTxs),
			db.EVMTransaction.UpdatedAt.Set(time.Now().UTC()),
			db.EVMTransaction.CreatedAt.Set(time.Now().UTC()),
		).Update().Tx() // 没有更新就不需要在update()中添加字段

		// 将transaction.Param对象添加到数组中
		ops = append(ops, txParam)
		// 将已经处理的交易hash删除操作添加到pipeline中
		hDelPipeline.HDel(*w.Ctx, TRANSACTIONS, tx.Hash)
		count++
		if count >= rowsNum {
			break
		}
	}
	ctx := context.Background()
	if err := w.DB.Prisma.Transaction(ops...).Exec(ctx); err != nil {
		logx.Info("transactions insert error ❌: ", err)
		panic(err)
	}
	// 插入成功后 删除redis中的数据 只有一个worker在处理 不会发生并发问题
	_, err = hDelPipeline.Exec(*w.Ctx)
	if err != nil {
		logx.Info("Error deleting transaction from Redis", err)
	}
	fmt.Println(time.Now().Format(TIME_FORMAT), "save transaction to db complete.....🟢")
	// 发送cursor到channel中
	cursor = newCursor
	return nil
}
