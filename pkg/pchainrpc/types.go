package pchainrpc

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/vms/platformvm/txs"
)

// NormalizedBlock represents a P-chain block with parsed transactions
type NormalizedBlock struct {
	BlockID      ids.ID
	Height       uint64
	ParentID     ids.ID
	Timestamp    time.Time
	Transactions []NormalizedTx
}

// JSONBlock represents a P-chain block with JSON-serialized transactions
type JSONBlock struct {
	BlockID      ids.ID
	Height       uint64
	ParentID     ids.ID
	Timestamp    time.Time
	Transactions []JSONTx
}

// NormalizedTx represents a normalized P-chain transaction for storage
type NormalizedTx struct {
	TxID        ids.ID
	TxType      string
	BlockHeight uint64
	BlockTime   time.Time

	// Common fields across transaction types
	Inputs  []Input
	Outputs []Output

	// Validator fields (for AddValidatorTx, AddDelegatorTx, etc.)
	NodeID    *ids.NodeID
	StartTime *uint64
	EndTime   *uint64
	Weight    *uint64

	// Subnet fields
	SubnetID *ids.ID
	ChainID  *ids.ID

	// ConvertSubnetToL1Tx specific
	Address    *[]byte // 20 bytes for Ethereum address
	Validators []byte  // JSON-encoded validator list

	// CreateSubnetTx / TransferSubnetOwnershipTx
	Owner []byte // JSON-encoded owner structure

	// CreateChainTx specific
	ChainName   *string
	GenesisData []byte
	VMID        *ids.ID
	FxIDs       []ids.ID
	SubnetAuth  []byte // JSON-encoded subnet authorization

	// ImportTx / ExportTx
	SourceChain      *ids.ID
	DestinationChain *ids.ID

	// RewardValidatorTx
	RewardTxID *ids.ID

	// TransformSubnetTx specific
	AssetID                  *ids.ID
	InitialSupply            *uint64
	MaxSupply                *uint64
	MinConsumptionRate       *uint64
	MaxConsumptionRate       *uint64
	MinValidatorStake        *uint64
	MaxValidatorStake        *uint64
	MinStakeDuration         *uint32
	MaxStakeDuration         *uint32
	MinDelegationFee         *uint32
	MinDelegatorStake        *uint64
	MaxValidatorWeightFactor *uint8
	UptimeRequirement        *uint32

	// AddPermissionlessValidatorTx / AddPermissionlessDelegatorTx
	Signer                []byte // JSON-encoded signer
	StakeOuts             []byte // JSON-encoded stake outputs
	ValidatorRewardsOwner []byte // JSON-encoded rewards owner
	DelegatorRewardsOwner []byte // JSON-encoded rewards owner
	DelegationShares      *uint32

	// IncreaseL1ValidatorBalanceTx
	ValidationID *ids.ID
	Balance      *uint64

	// SetL1ValidatorWeightTx
	Message []byte // Warp message with SetL1ValidatorWeight

	// AdvanceTimeTx
	Time *uint64 // Unix time this block proposes increasing the timestamp to
}

// JSONTx represents a P-chain transaction stored as JSON
type JSONTx struct {
	TxID        ids.ID
	TxType      string
	BlockHeight uint64
	BlockTime   time.Time
	TxData      []byte // JSON-serialized tx.Unsigned
}

// Input represents a transaction input
type Input struct {
	TxID        ids.ID
	OutputIndex uint32
	AssetID     ids.ID
	Amount      uint64
	Address     []byte // Can be multiple addresses, JSON-encoded
}

// Output represents a transaction output
type Output struct {
	AssetID   ids.ID
	Amount    uint64
	Locktime  uint64
	Threshold uint32
	Addresses [][]byte // Multiple addresses
}

// ParseBlock parses a raw P-chain block into a normalized structure
func ParseBlock(blk interface{}, blockBytes []byte) (*NormalizedBlock, error) {
	// This will be implemented in fetcher.go using the platformvm block parsing
	return nil, nil
}

// TxTypeString returns a human-readable transaction type using reflection
func TxTypeString(tx *txs.Tx) string {
	if tx == nil || tx.Unsigned == nil {
		panic("tx is nil or unsigned tx is nil")
	}

	// Get the type name using reflection
	typeName := reflect.TypeOf(tx.Unsigned).Elem().Name()

	// Remove the "Tx" suffix if present
	typeName = strings.TrimSuffix(typeName, "Tx")

	return typeName
}

// ValidatorState represents the current state of a validator
type ValidatorState struct {
	ValidationID ids.ID
	NodeID       ids.NodeID
	SubnetID     ids.ID
	Weight       uint64
	Balance      uint64
	StartTime    time.Time
	EndTime      time.Time
	Uptime       float64 // Percentage 0-100
	Active       bool
}

// GetCurrentValidatorsResponse represents the response from platform.getCurrentValidators
type GetCurrentValidatorsResponse struct {
	Validators []ValidatorInfo `json:"validators"`
}

// ValidatorInfo represents validator information from RPC
type ValidatorInfo struct {
	TxID             string          `json:"txID"`
	StartTime        string          `json:"startTime"` // Changed to string because RPC returns it as string sometimes
	EndTime          string          `json:"endTime"`   // Changed to string because RPC returns it as string sometimes
	Weight           string          `json:"weight"`    // String because it can be large
	NodeID           string          `json:"nodeID"`
	ValidationID     string          `json:"validationID,omitempty"` // For L1 validators
	Balance          string          `json:"balance,omitempty"`      // For L1 validators
	Uptime           string          `json:"uptime,omitempty"`       // Percentage as string
	Connected        bool            `json:"connected"`
	Signer           json.RawMessage `json:"signer,omitempty"` // Can be string (L1) or object (Primary Network BLS)
	DelegationFee    string          `json:"delegationFee,omitempty"`
	PotentialReward  string          `json:"potentialReward,omitempty"`
	AccruedDelegatee string          `json:"accruedDelegatee,omitempty"`
}
