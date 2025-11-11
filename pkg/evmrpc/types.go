package evmrpc

import "encoding/json"

type Transaction struct {
	Hash                 string          `json:"hash"`
	Nonce                string          `json:"nonce"`
	BlockHash            string          `json:"blockHash"`
	BlockNumber          string          `json:"blockNumber"`
	TransactionIndex     string          `json:"transactionIndex"`
	From                 string          `json:"from"`
	To                   string          `json:"to"`
	Value                string          `json:"value"`
	Gas                  string          `json:"gas"`
	GasPrice             string          `json:"gasPrice"`
	Input                string          `json:"input"`
	V                    string          `json:"v,omitempty"`
	R                    string          `json:"r,omitempty"`
	S                    string          `json:"s,omitempty"`
	YParity              string          `json:"yParity,omitempty"`
	Type                 string          `json:"type,omitempty"`
	TypeHex              string          `json:"typeHex,omitempty"`
	ChainId              string          `json:"chainId,omitempty"`
	MaxFeePerGas         string          `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas string          `json:"maxPriorityFeePerGas,omitempty"`
	AccessList           json.RawMessage `json:"accessList,omitempty"`
}

type Block struct {
	Number                string        `json:"number"`
	Hash                  string        `json:"hash"`
	ParentHash            string        `json:"parentHash"`
	Timestamp             string        `json:"timestamp"`
	Miner                 string        `json:"miner"`
	Difficulty            string        `json:"difficulty"`
	TotalDifficulty       string        `json:"totalDifficulty"`
	Size                  string        `json:"size"`
	GasLimit              string        `json:"gasLimit"`
	GasUsed               string        `json:"gasUsed"`
	BaseFeePerGas         string        `json:"baseFeePerGas,omitempty"`
	BlockGasCost          string        `json:"blockGasCost,omitempty"`
	Transactions          []Transaction `json:"transactions"`
	StateRoot             string        `json:"stateRoot"`
	TransactionsRoot      string        `json:"transactionsRoot"`
	ReceiptsRoot          string        `json:"receiptsRoot"`
	ExtraData             string        `json:"extraData,omitempty"`
	BlockExtraData        string        `json:"blockExtraData,omitempty"`
	ExtDataHash           string        `json:"extDataHash,omitempty"`
	ExtDataGasUsed        string        `json:"extDataGasUsed,omitempty"`
	LogsBloom             string        `json:"logsBloom"`
	MixHash               string        `json:"mixHash"`
	Nonce                 string        `json:"nonce"`
	Sha3Uncles            string        `json:"sha3Uncles"`
	Uncles                []string      `json:"uncles"`
	BlobGasUsed           string        `json:"blobGasUsed,omitempty"`
	ExcessBlobGas         string        `json:"excessBlobGas,omitempty"`
	ParentBeaconBlockRoot string        `json:"parentBeaconBlockRoot,omitempty"`
}

type CallTrace struct {
	From         string      `json:"from"`
	Gas          string      `json:"gas"`
	GasUsed      string      `json:"gasUsed"`
	To           string      `json:"to"`
	Input        string      `json:"input"`
	Output       string      `json:"output,omitempty"`
	Error        string      `json:"error,omitempty"`
	RevertReason string      `json:"revertReason,omitempty"`
	Calls        []CallTrace `json:"calls,omitempty"`
	Value        string      `json:"value,omitempty"`
	Type         string      `json:"type"`
}

type TraceResultOptional struct {
	TxHash string     `json:"txHash"`
	Result *CallTrace `json:"result"`
}

type Receipt struct {
	BlockHash         string  `json:"blockHash"`
	BlockNumber       string  `json:"blockNumber"`
	ContractAddress   *string `json:"contractAddress"`
	CumulativeGasUsed string  `json:"cumulativeGasUsed"`
	EffectiveGasPrice string  `json:"effectiveGasPrice"`
	From              string  `json:"from"`
	GasUsed           string  `json:"gasUsed"`
	Logs              []Log   `json:"logs"`
	LogsBloom         string  `json:"logsBloom"`
	Status            string  `json:"status"`
	To                string  `json:"to"`
	TransactionHash   string  `json:"transactionHash"`
	TransactionIndex  string  `json:"transactionIndex"`
	Type              string  `json:"type"`
}

type Log struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	BlockHash        string   `json:"blockHash"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

type NormalizedBlock struct {
	Block    Block                 `json:"block"`
	Traces   []TraceResultOptional `json:"traces"`
	Receipts []Receipt             `json:"receipts"`
}

type jsonRpcRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type jsonRpcResponse struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonRpcError   `json:"error,omitempty"`
}

type jsonRpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ProgressCallback func(phase string, current, total int64, txCount int)
