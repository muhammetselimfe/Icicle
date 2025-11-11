package pchainsyncer

import (
	"clickhouse-metrics-poc/pkg/pchainrpc"
	"context"
	"encoding/json"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ava-labs/avalanchego/ids"
)

// idToBytes converts an ids.ID to a byte slice
func idToBytes(id ids.ID) []byte {
	return id[:]
}

// nodeIDToBytes converts an ids.NodeID to a byte slice
func nodeIDToBytes(id ids.NodeID) []byte {
	return id[:]
}

// InsertPChainTxs inserts P-chain transaction data into the p_chain_txs table
func InsertPChainTxs(ctx context.Context, conn clickhouse.Conn, blocks []*pchainrpc.NormalizedBlock) error {
	if len(blocks) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO p_chain_txs (
		tx_id, tx_type, block_number, block_time,
		node_id, start_time, end_time, weight,
		subnet_id, chain_id,
		address, validators,
		owner,
		chain_name, genesis_data, vm_id, fx_ids, subnet_auth,
		source_chain, destination_chain,
		reward_tx_id,
		asset_id, initial_supply, max_supply,
		min_consumption_rate, max_consumption_rate,
		min_validator_stake, max_validator_stake,
		min_stake_duration, max_stake_duration,
		min_delegation_fee, min_delegator_stake,
		max_validator_weight_factor, uptime_requirement,
		signer, stake_outs, validator_rewards_owner, delegator_rewards_owner,
		delegation_shares,
		validation_id, balance,
		message,
		time
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, block := range blocks {
		for _, tx := range block.Transactions {
			// Convert tx_id
			txID := idToBytes(tx.TxID)

			// Validator fields
			var nodeID any = nil
			if tx.NodeID != nil {
				nodeID = nodeIDToBytes(*tx.NodeID)
			}

			// Subnet/Chain IDs
			var subnetID any = nil
			if tx.SubnetID != nil {
				subnetID = idToBytes(*tx.SubnetID)
			}

			var chainID any = nil
			if tx.ChainID != nil {
				chainID = idToBytes(*tx.ChainID)
			}

			// ConvertSubnetToL1Tx fields
			var address any = nil
			if tx.Address != nil {
				address = *tx.Address
			}

			validators := string(tx.Validators)
			if validators == "" {
				validators = ""
			}

			// Owner
			owner := string(tx.Owner)
			if owner == "" {
				owner = ""
			}

			// CreateChainTx fields
			chainName := ""
			if tx.ChainName != nil {
				chainName = *tx.ChainName
			}

			genesisData := string(tx.GenesisData)
			if genesisData == "" {
				genesisData = ""
			}

			var vmID any = nil
			if tx.VMID != nil {
				vmID = idToBytes(*tx.VMID)
			}

			fxIDs := ""
			if len(tx.FxIDs) > 0 {
				fxIDsJSON, _ := json.Marshal(tx.FxIDs)
				fxIDs = string(fxIDsJSON)
			}

			subnetAuth := string(tx.SubnetAuth)
			if subnetAuth == "" {
				subnetAuth = ""
			}

			// ImportTx/ExportTx fields
			var sourceChain any = nil
			if tx.SourceChain != nil {
				sourceChain = idToBytes(*tx.SourceChain)
			}

			var destinationChain any = nil
			if tx.DestinationChain != nil {
				destinationChain = idToBytes(*tx.DestinationChain)
			}

			// RewardValidatorTx
			var rewardTxID any = nil
			if tx.RewardTxID != nil {
				rewardTxID = idToBytes(*tx.RewardTxID)
			}

			// TransformSubnetTx fields
			var assetID any = nil
			if tx.AssetID != nil {
				assetID = idToBytes(*tx.AssetID)
			}

			// Permissionless validator/delegator fields
			signer := string(tx.Signer)
			if signer == "" {
				signer = ""
			}

			stakeOuts := string(tx.StakeOuts)
			if stakeOuts == "" {
				stakeOuts = ""
			}

			validatorRewardsOwner := string(tx.ValidatorRewardsOwner)
			if validatorRewardsOwner == "" {
				validatorRewardsOwner = ""
			}

			delegatorRewardsOwner := string(tx.DelegatorRewardsOwner)
			if delegatorRewardsOwner == "" {
				delegatorRewardsOwner = ""
			}

			// IncreaseL1ValidatorBalanceTx fields
			var validationID any = nil
			if tx.ValidationID != nil {
				validationID = idToBytes(*tx.ValidationID)
			}

			// SetL1ValidatorWeightTx fields
			message := string(tx.Message)
			if message == "" {
				message = ""
			}

			// Append to batch
			err = batch.Append(
				txID,
				tx.TxType,
				tx.BlockHeight,
				tx.BlockTime,
				nodeID,
				tx.StartTime,
				tx.EndTime,
				tx.Weight,
				subnetID,
				chainID,
				address,
				validators,
				owner,
				chainName,
				genesisData,
				vmID,
				fxIDs,
				subnetAuth,
				sourceChain,
				destinationChain,
				rewardTxID,
				assetID,
				tx.InitialSupply,
				tx.MaxSupply,
				tx.MinConsumptionRate,
				tx.MaxConsumptionRate,
				tx.MinValidatorStake,
				tx.MaxValidatorStake,
				tx.MinStakeDuration,
				tx.MaxStakeDuration,
				tx.MinDelegationFee,
				tx.MinDelegatorStake,
				tx.MaxValidatorWeightFactor,
				tx.UptimeRequirement,
				signer,
				stakeOuts,
				validatorRewardsOwner,
				delegatorRewardsOwner,
				tx.DelegationShares,
				validationID,
				tx.Balance,
				message,
				tx.Time,
			)
			if err != nil {
				return fmt.Errorf("failed to append tx %s: %w", tx.TxID, err)
			}
		}
	}

	return batch.Send()
}
