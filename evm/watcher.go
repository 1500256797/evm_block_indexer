package evm

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// ===============================================================
// 3. 定义生产者 生产者实时监控区块链最新区块 然后将区块号放入redis任务队列
// ===============================================================
type Watcher struct {
	Redis    *redis.Client
	Provider *ethclient.Client
	Ctx      *context.Context
}

/**
 * @Description: NewWatcher 新建生产者
 * @param  redis *redis.Client
 * @param  provider *ethclient.Client
 * @param  ctx *context.Context
 * @return *Watcher
 */
func NewWatcher(redis *redis.Client, provider *ethclient.Client, ctx *context.Context) *Watcher {
	return &Watcher{
		Redis:    redis,
		Provider: provider,
		Ctx:      ctx,
	}
}

/**
 * @Description: UpdateProvider 更新provider
 * @param  provider *ethclient.Client
 */
func (w *Watcher) UpdateProvider(provider *ethclient.Client) {
	w.Provider = provider
}

// unSubscribe 取消订阅
func (w *Watcher) UnSubscribe() {
	// sub.Unsubscribe()
	w.Provider.Close()
}

/**
 * @Description: WatchNewBlock 监控最新区块
 * @param  nothing，函数名大些开头，说明是公开函数
 * @return error
 */
func (w *Watcher) WatchNewBlock() error {
	headers := make(chan *types.Header)
	sub, err := w.Provider.SubscribeNewHead(*w.Ctx, headers)
	// 取消订阅
	defer sub.Unsubscribe()
	if err != nil {
		return err
	}
	for {
		select {
		case err := <-sub.Err():
			return fmt.Errorf("🛑: %v", err)
		case header := <-headers:
			var block *types.Block
			var err error
			block, _ = w.Provider.BlockByHash(*w.Ctx, header.Hash())
			if block == nil {
				// fmt.Println(time.Now().Format(TIME_FORMAT), "WatchNewBlock 区块为空，正在重试...")
				block, err = w.Provider.BlockByHash(*w.Ctx, header.Hash())
				if err != nil {
					// fmt.Println(time.Now().Format(TIME_FORMAT), "WatchNewBlock 区块重试失败，正在重试...")
					continue
				}
			}
			// set block number to redis list
			err = w.Redis.SAdd(*w.Ctx, ALL_BLOCK_NUMBER, block.Number().Uint64()).Err()
			if err != nil {
				logx.Info("Failed to set block number to redis list: ", err)
			}
			// 同时将区块号放入未消费区块队列
			err = w.Redis.SAdd(*w.Ctx, NO_CONSUMED_BLOCK_NUMBER, block.Number().Uint64()).Err()
			if err != nil {
				logx.Info("Failed to set block number to redis list: ", err)
			}

			// 	发起广播 向频道发送消息
			err = w.Redis.Publish(*w.Ctx, NEW_BLOCK_CHANNEL, block.Number().Uint64()).Err()
			if err != nil {
				logx.Info("Failed to publish block number to redis channel: ", err)
			}
		default:
			continue
		}
	}
}
