package evm

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
)

// ===============================================================
// 2. 定义检查者 用来检查消费者是否正常消费
// ===============================================================
type Monitor struct {
	Redis    *redis.Client
	Ctx      *context.Context
	Provider *ethclient.Client
	Watcher  *Watcher
}

/**
 * @Description: NewMonitor 新建检查者
 * @param  redis *redis.Client
 * @param  ctx *context.Context
 * @return *Monitor
 */
func NewMonitor(redis *redis.Client, ctx *context.Context, provider *ethclient.Client) *Monitor {
	return &Monitor{
		Redis:    redis,
		Ctx:      ctx,
		Provider: provider,
		Watcher:  nil,
	}
}

// 自定义的字符串转整型函数
func (m *Monitor) parseInt(str string) int {
	num, _ := strconv.Atoi(str)
	return num
}

// registerWatcher 注册watcher
func (m *Monitor) RegisterWatcher(watcher *Watcher) {
	m.Watcher = watcher
}

// getWatcher 获取watcher
func (m *Monitor) GetWatcher() *Watcher {
	return m.Watcher
}

/**
 * @Description: CheckRedisTaskQueue 检查并重新分配任务
 * @param  nothing，函数名大些开头，说明是公开函数
 */
func (m *Monitor) CheckRedisTaskQueue() {
	// 将所有已消费 未消费 正在消费的区块进行并集
	m.Redis.SUnionStore(*m.Ctx, ALL_BLOCK_NUMBER, CONSUMED_BLOCK_NUMBER, NO_CONSUMED_BLOCK_NUMBER, CONSUMING_BLOCK_NUMBER)
	all_block_numbers, err := m.Redis.SMembers(*m.Ctx, ALL_BLOCK_NUMBER).Result()
	if err != nil {
		log.Fatal(err)
	}
	if len(all_block_numbers) == 0 {
		fmt.Println(time.Now().Format(TIME_FORMAT), "the redis block number quene is ready")
		return
	}
	// 将消费中的区块从CONSUMING_BLOCK_NUMBER中 移动到 UNCONSUMED_BLOCK_NUMBER中
	consuming_block_numbers, err := m.Redis.SMembers(*m.Ctx, CONSUMING_BLOCK_NUMBER).Result()
	m.Redis.SMove(*m.Ctx, CONSUMING_BLOCK_NUMBER, NO_CONSUMED_BLOCK_NUMBER, consuming_block_numbers)
	// 删除CONSUMING_BLOCK_NUMBER中的区块
	m.Redis.Del(*m.Ctx, CONSUMING_BLOCK_NUMBER)
	if err != nil {
		log.Fatalf("获取CONSUMING_BLOCK_NUMBER中的区块出错:%v", err)
	}

	// 对并集进行排序
	sort.Strings(all_block_numbers)
	// 取头尾两个区块
	first_block_number := all_block_numbers[0]
	last_block_number := all_block_numbers[len(all_block_numbers)-1]
	fmt.Println(time.Now().Format(TIME_FORMAT), "Redis任务队列初始化加载完毕,区块链区块头尾区块号为", first_block_number, last_block_number)
	// 创建 WaitGroup 和 channel
	var wg sync.WaitGroup
	done := make(chan struct{}) //用空结构体定义管道，是为了说明该管道不是作为容器来使用的，而是作为协程同步的一种手段
	// 按每隔 10 万个区块号划分任务，每个任务分别处理
	var batchSize = 100000
	var blocksToProcess []int
	for i, _ := strconv.Atoi(first_block_number); i <= m.parseInt(last_block_number); i++ {
		blocksToProcess = append(blocksToProcess, i)
		if len(blocksToProcess) == batchSize {
			// 开启一个协程处理批次
			wg.Add(1)
			go func() {
				m.BatchProcessCheck(blocksToProcess, &wg)
			}()
			blocksToProcess = nil // 在处理完后清空，以便下一个批次使用
		}
	}
	// 处理最后一批不足 batchSize 的区块号
	if len(blocksToProcess) > 0 {
		wg.Add(1)
		go func() {
			m.BatchProcessCheck(blocksToProcess, &wg)
		}()
	}
	// 等待所有协程任务完成
	go func() {
		wg.Wait()   // 等到所有协程任务完成
		close(done) //调用close关闭channel
	}()
	<-done // 从空的channel中读取数据，阻塞当前协程，直到所有协程任务完成管道关闭后 继续执行后续操作
	// 更新NO_CONSUMED_BLOCK_NUMBER中的区块 =  all_block_number - consumed_block_number - consuming_block_number
	m.Redis.SDiffStore(*m.Ctx, NO_CONSUMED_BLOCK_NUMBER, ALL_BLOCK_NUMBER, CONSUMED_BLOCK_NUMBER, CONSUMING_BLOCK_NUMBER)
}

// 批处理
func (m *Monitor) BatchProcessCheck(blockNumbers []int, wg *sync.WaitGroup) {
	defer wg.Done() // 通知 WaitGroup 已完成一个任务
	// 使用 Pipeline 批量添加区块号到集合
	pipe := m.Redis.Pipeline()
	for _, blockNumber := range blockNumbers {
		// 利用集合特性检查
		found, err := m.Redis.SIsMember(*m.Ctx, ALL_BLOCK_NUMBER, blockNumber).Result()
		if err != nil {
			logx.Info("检查ALL_BLOCK_NUMBER是否连续时出错:", err)
		}
		if !found {
			pipe.SAdd(*m.Ctx, ALL_BLOCK_NUMBER, blockNumber)
			fmt.Println(time.Now().Format(TIME_FORMAT), "区块号", blockNumber, "不存在,已添加至ALL_BLOCK_NUMBER")
		}
	}
	_, err := pipe.Exec(*m.Ctx)
	if err != nil {
		logx.Info("批量添加区块号到集合出错:", err)
	}

}

// 定期检查watcher是否正常工作
func (m *Monitor) CheckWatcherIsWorking() bool {
	// defer 遇到panic 忽略错误 继续执行
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	// 获取所有的区块
	all_block_numbers, err := m.Redis.SMembers(*m.Ctx, ALL_BLOCK_NUMBER).Result()
	if err != nil {
		return false
	}
	// 对并集进行排序
	sort.Strings(all_block_numbers)
	// 取头尾两个区块
	last_block_number := all_block_numbers[len(all_block_numbers)-1]
	// 获取最新区块
	latestBlock, err := m.Provider.BlockByNumber(*m.Ctx, nil)
	if err != nil {
		return false
	}
	// 如果redis中的尾区块号小于最新区块号 则说明watcher异常
	if m.parseInt(last_block_number) < int(latestBlock.Number().Uint64()) {
		fmt.Println(time.Now().Format(TIME_FORMAT), "检查到watcher异常，redis中区块链区块尾区块号为", last_block_number, "最新区块号为", latestBlock.Number().Uint64(), "正在重新启动watcher。。。🛑")
		return false
	}
	return true
}

// 每隔一段时间检查一次ALL_BLOCK_NUMBER是否连续，不连续则更新任务
func (m *Monitor) CheckAllBlockNumberIsContinue() {
	// defer 遇到panic 忽略错误 继续执行
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	// 获取所有的区块
	all_block_numbers, err := m.Redis.SMembers(*m.Ctx, ALL_BLOCK_NUMBER).Result()
	if err != nil {
		return
	}

	//  length == 0
	if len(all_block_numbers) == 0 {
		return
	}
	// 对并集进行排序
	sort.Strings(all_block_numbers)

	// 取头尾两个区块
	first_block_number := all_block_numbers[0]
	last_block_number := all_block_numbers[len(all_block_numbers)-1]

	// 遍历头尾两个区块之间的所有区块 确保没有遗漏
	missingBlockNumber := make([]int, 0)
	for i, _ := strconv.Atoi(first_block_number); i <= m.parseInt(last_block_number); i++ {
		found := false
		for _, num := range all_block_numbers {
			if i == m.parseInt(num) {
				found = true
				break
			}
		}
		if !found {
			missingBlockNumber = append(missingBlockNumber, i)
		}
	}

	// 将遗漏的区块更新到all_block_number中
	for _, blockNumber := range missingBlockNumber {
		m.Redis.SAdd(*m.Ctx, ALL_BLOCK_NUMBER, blockNumber)
	}
	// 如果程序运行一切正常 则all_block_number 应该完全连续 即missingBlockNumber长度为0
	if len(missingBlockNumber) > 0 {
		// 更新NO_CONSUMED_BLOCK_NUMBER中的区块 =  all_block_number - consumed_block_number
		m.Redis.SDiffStore(*m.Ctx, NO_CONSUMED_BLOCK_NUMBER, ALL_BLOCK_NUMBER, CONSUMED_BLOCK_NUMBER, CONSUMING_BLOCK_NUMBER)
		fmt.Println(time.Now().Format(TIME_FORMAT), "检查到区块链区块不连续，缺失区块数量：", len(missingBlockNumber), "已更新NO_CONSUMED_BLOCK_NUMBER中的区块")
	} else {
		fmt.Println(time.Now().Format(TIME_FORMAT), "redis中区块链区块完全连续,10s后继续检查程序运行状态。。。🟢")
	}
}
