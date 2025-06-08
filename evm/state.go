package evm

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	NO_CONSUMED_BLOCK_NUMBER = "no_consumed_block_number" // 所有未消费的区块
	CONSUMED_BLOCK_NUMBER    = "consumed_block_number"    // 所有已消费完成的区块
	CONSUMING_BLOCK_NUMBER   = "consuming_block_number"   // 所有正在消费的区块
	ALL_BLOCK_NUMBER         = "all_block_number"         // 完整区块链区块
	TRANSACTIONS             = "transactions"             // 交易
	NEW_BLOCK_CHANNEL        = "new_block_channel"        // 新区块通道
	TIME_FORMAT              = "2006-01-02 15:04:05"      // 定义时间格式
)

/**
 * @Description: NewRedisClient 新建redis连接 返回redis连接对象
 * @param  nothing，函数名大些开头，说明是公开函数
 * @return *redisclient.Client
 */
func NewRedisClient() (*redis.Client, error) {
	// create redis instance
	rdb := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Password:     "", // no password set
		DB:           0,  // use default DB
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	})
	// ping redis
	_, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		logx.Errorf(time.Now().Format(TIME_FORMAT), "connect to redis failed,Error: %s", err)
		return nil, err
	}
	return rdb, nil
}
