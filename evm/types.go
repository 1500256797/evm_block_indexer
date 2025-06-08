package evm

import "time"

type Transaction struct {
	TxId          string    `json:"TxId"`           // 交易id
	BlockNumber   uint64    `json:"BlockNumber"`    // 区块号
	BlockHash     string    `json:"BlockHash"`      // 区块hash
	From          string    `json:"From"`           // 发送者
	To            string    `json:"To"`             // 接收者
	Value         string    `json:"Value"`          // 交易金额
	Nonce         uint64    `json:"Nonce"`          // 交易nonce
	TranactionFee string    `json:"TransactionFee"` // 交易手续费
	GasPrice      string    `json:"GasPrice"`       // 交易gas价格
	Byte4Data     string    `json:"Byte4Data"`      // 交易数据
	Timestamp     time.Time `json:"Timestamp"`      // 交易时间
	Type          int       `json:"Type"`           // 交易类型
	ChainID       int       `json:"ChainID"`        // 链id
	BlockTxs      int       `json:"BlockTxs"`       // 区块交易数
	CreatedAt     time.Time `json:"CreatedAt"`      // 创建时间
	UpdatedAt     time.Time `json:"UpdatedAt"`      // 更新时间
}
