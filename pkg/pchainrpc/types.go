package pchainrpc

import (
	"fmt"
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

// TxTypeString returns a human-readable transaction type
func TxTypeString(tx *txs.Tx) string {
	if tx == nil || tx.Unsigned == nil {
		panic("TxTypeString called with nil transaction or nil unsigned tx")
	}

	switch tx.Unsigned.(type) {
	case *txs.ConvertSubnetToL1Tx:
		return "ConvertSubnetToL1"
	case *txs.AddValidatorTx:
		return "AddValidator"
	case *txs.AddDelegatorTx:
		return "AddDelegator"
	case *txs.BaseTx:
		return "Base"
	case *txs.CreateSubnetTx:
		return "CreateSubnet"
	case *txs.CreateChainTx:
		return "CreateChain"
	case *txs.ImportTx:
		return "Import"
	case *txs.ExportTx:
		return "Export"
	case *txs.AddSubnetValidatorTx:
		return "AddSubnetValidator"
	case *txs.RemoveSubnetValidatorTx:
		return "RemoveSubnetValidator"
	case *txs.TransformSubnetTx:
		return "TransformSubnet"
	case *txs.TransferSubnetOwnershipTx:
		return "TransferSubnetOwnership"
	case *txs.AddPermissionlessValidatorTx:
		return "AddPermissionlessValidator"
	case *txs.AddPermissionlessDelegatorTx:
		return "AddPermissionlessDelegator"
	case *txs.RewardValidatorTx:
		return "RewardValidator"
	case *txs.IncreaseL1ValidatorBalanceTx:
		return "IncreaseL1ValidatorBalance"
	case *txs.SetL1ValidatorWeightTx:
		return "SetL1ValidatorWeight"
	case *txs.AdvanceTimeTx:
		return "AdvanceTime"
	default:
		panic(fmt.Sprintf("Unknown P-chain transaction type: %T - indexer is broken and needs to be updated", tx.Unsigned))
	}
}
