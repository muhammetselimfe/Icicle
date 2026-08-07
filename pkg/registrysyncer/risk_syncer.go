package registrysyncer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"icicle/pkg/chwrapper"
)

// Function selectors (keccak256(signature)[:4]). The well-known Gnosis/OZ ones are
// hardcoded; the ValidatorManager/StakingManager ones were computed and verified
// against a live contract on C-Chain (see plan).
const (
	getThresholdSelector        = "0xe75235b8" // Gnosis Safe getThreshold()
	getOwnersSelector           = "0xa0e67e2b" // Gnosis Safe getOwners()
	getMinDelaySelector         = "0xf27a0c92" // OZ TimelockController getMinDelay()
	getChurnPeriodSelector      = "0x09c1df66" // ValidatorManager getChurnPeriodSeconds()
	getChurnTrackerSelector     = "0x4d693536" // ValidatorManager getChurnTracker() -> (uint64, uint8 maxChurnPct, ...)
	erc20Selector               = "0x785e9e86" // ERC20TokenStakingManager erc20()
	stakingTokenSelector        = "0x72f702f3" // stakingToken()
	tokenSelector               = "0xfc0c546a" // token()
	rewardCalculatorSelector    = "0x96769e89" // PoS rewardCalculator()
	weightToValueFactorSelector = "0x94aafc15" // PoS weightToValueFactor()
)

// EIP-1967 storage slots.
const (
	eip1967ImplSlot  = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
	eip1967AdminSlot = "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103"
)

const zeroAddress = "0x0000000000000000000000000000000000000000"

// riskInput is a chain that needs its ValidatorManager risk profile resolved.
type riskInput struct {
	ChainID   string
	VMAddress string
	RpcURL    string // may be empty -> only C-Chain is tried
}

// riskRow is the resolved on-chain risk profile, ready to insert into chain_risk.
type riskRow struct {
	ChainID             string
	VMAddress           string
	ManagerType         string // PoA | PoS-native | PoS-erc20 | unknown
	ManagerLocation     string // c-chain | self | unknown — which chain the manager contract has code on
	OwnerAddress        *string
	OwnerKind           string // eoa | multisig | timelock | dao | contract | unknown
	MultisigThreshold   *uint16
	MultisigOwners      *uint16
	IsProxy             bool
	ProxyImplementation *string
	ProxyAdmin          *string
	ProxyAdminOwner     *string
	UpgradeDelaySeconds *uint64
	ChurnPeriodSeconds  *uint64
	MaxChurnPercentage  *uint8
}

// SyncChainRisk reads each L1's ValidatorManager control & upgradeability state from
// on-chain contract storage and stores it in chain_risk. Best-effort: anything we
// can't determine is left null (never fabricated). Runs the contracts in a bounded
// worker pool since each chain needs several RPC round-trips.
func SyncChainRisk(ctx context.Context, conn driver.Conn) error {
	slog.Info("Starting chain risk sync")
	startTime := time.Now()

	rows, err := conn.Query(ctx, `
		SELECT sc.chain_id, s.validator_manager_address, r.rpc_url
		FROM (SELECT * FROM subnets FINAL) s
		INNER JOIN (SELECT * FROM subnet_chains FINAL) sc ON s.subnet_id = sc.subnet_id
		LEFT JOIN (SELECT * FROM l1_registry FINAL) r ON sc.chain_id = r.blockchain_id
		WHERE s.validator_manager_address != ''
	`)
	if err != nil {
		return fmt.Errorf("failed to query chains for risk sync: %w", err)
	}
	defer rows.Close()

	var inputs []riskInput
	for rows.Next() {
		var in riskInput
		var rpcURL *string
		if err := rows.Scan(&in.ChainID, &in.VMAddress, &rpcURL); err != nil {
			return fmt.Errorf("failed to scan risk row: %w", err)
		}
		if rpcURL != nil {
			in.RpcURL = *rpcURL
		}
		inputs = append(inputs, in)
	}

	if len(inputs) == 0 {
		slog.Info("No chains with a validator manager address found for risk sync")
		return nil
	}

	slog.Info("Resolving chain risk profiles", "count", len(inputs))

	httpClient := &http.Client{Timeout: 10 * time.Second}

	// Bounded concurrency: each chain is several sequential RPC calls.
	const workers = 8
	sem := make(chan struct{}, workers)
	results := make([]riskRow, len(inputs))
	var wg sync.WaitGroup
	for i, in := range inputs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, in riskInput) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = resolveChainRisk(httpClient, in)
		}(i, in)
	}
	wg.Wait()

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO chain_risk (
		chain_id, validator_manager_address,
		manager_type, manager_location, owner_address, owner_kind, multisig_threshold, multisig_owners,
		is_proxy, proxy_implementation, proxy_admin, proxy_admin_owner, upgrade_delay_seconds,
		churn_period_seconds, max_churn_percentage, last_updated
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare chain_risk batch: %w", err)
	}
	defer func() { _ = batch.Abort() }()

	now := time.Now()
	var resolved, poa, pos, onCChain, onSelf int
	for _, r := range results {
		if err := batch.Append(
			r.ChainID, r.VMAddress,
			r.ManagerType, r.ManagerLocation, r.OwnerAddress, r.OwnerKind, r.MultisigThreshold, r.MultisigOwners,
			r.IsProxy, r.ProxyImplementation, r.ProxyAdmin, r.ProxyAdminOwner, r.UpgradeDelaySeconds,
			r.ChurnPeriodSeconds, r.MaxChurnPercentage, now,
		); err != nil {
			slog.Warn("Failed to append chain_risk row", "chain_id", r.ChainID, "error", err)
			continue
		}
		if r.OwnerAddress != nil {
			resolved++
		}
		switch r.ManagerType {
		case "PoA":
			poa++
		case "PoS-native", "PoS-erc20":
			pos++
		}
		switch r.ManagerLocation {
		case "c-chain":
			onCChain++
		case "self":
			onSelf++
		}
	}

	if err := chwrapper.RetryableBatchSend(batch); err != nil {
		return fmt.Errorf("failed to send chain_risk batch: %w", err)
	}

	slog.Info("Chain risk sync complete",
		"total", len(inputs), "owner_resolved", resolved, "poa", poa, "pos", pos,
		"manager_on_cchain", onCChain, "manager_on_self", onSelf,
		"duration", time.Since(startTime))
	return nil
}

// resolveChainRisk runs the detection algorithm for one chain's ValidatorManager.
func resolveChainRisk(client *http.Client, in riskInput) riskRow {
	row := riskRow{
		ChainID:         in.ChainID,
		VMAddress:       in.VMAddress,
		ManagerType:     "unknown",
		ManagerLocation: "unknown",
		OwnerKind:       "unknown",
	}

	// The ValidatorManager may live on the L1's own chain or on C-Chain. Find the one
	// that actually has code at the address, then run all probes against it. Where the
	// code lives is itself a liveness signal: a manager on C-Chain can still add/remove
	// validators if the L1 halts; one deployed on the L1 ("self") cannot.
	rpc, location := workingRPC(client, in)
	if rpc == "" {
		return row // no contract code found anywhere; minimal row
	}
	row.ManagerLocation = location

	// Churn limits. getChurnTracker() returns (uint64 churnPeriodSeconds, uint8 maxChurnPct, ...)
	// in words 0 and 1 — read both from the single call (more reliable than the standalone
	// getChurnPeriodSeconds(), which returns 0 on some manager versions). Fall back to
	// getChurnPeriodSeconds() for the period only if the tracker call is unavailable.
	if res, err := ethCall(client, rpc, in.VMAddress, getChurnTrackerSelector); err == nil && hasResult(res) {
		if w, ok := wordAt(res, 0); ok {
			p := hexWordToUint64(w)
			row.ChurnPeriodSeconds = &p
		}
		if w, ok := wordAt(res, 1); ok {
			m := uint8(hexWordToUint64(w))
			row.MaxChurnPercentage = &m
		}
	} else if res, err := ethCall(client, rpc, in.VMAddress, getChurnPeriodSelector); err == nil && hasResult(res) {
		if w, ok := wordAt(res, 0); ok {
			p := hexWordToUint64(w)
			row.ChurnPeriodSeconds = &p
		}
	}

	// EIP-1967 proxy / upgradeability.
	if res, err := ethGetStorageAt(client, rpc, in.VMAddress, eip1967ImplSlot); err == nil {
		if impl, ok := nonZeroAddr(res); ok {
			row.IsProxy = true
			row.ProxyImplementation = &impl
		}
	}
	if row.IsProxy {
		if res, err := ethGetStorageAt(client, rpc, in.VMAddress, eip1967AdminSlot); err == nil {
			if admin, ok := nonZeroAddr(res); ok {
				row.ProxyAdmin = &admin
				// ProxyAdmin.owner() is the real upgrade controller.
				if ownerRes, err := ethCall(client, rpc, admin, ownerSelector); err == nil {
					if pao, ok := nonZeroAddr(ownerRes); ok {
						row.ProxyAdminOwner = &pao
						delay := upgradeDelay(client, rpc, pao)
						row.UpgradeDelaySeconds = &delay
					}
				}
			}
		}
	}

	// Owner, owner kind, and (from the owner) manager type.
	if ownerRes, err := ethCall(client, rpc, in.VMAddress, ownerSelector); err == nil {
		if owner, ok := nonZeroAddr(ownerRes); ok {
			row.OwnerAddress = &owner
			kind, threshold, count := classifyOwner(client, rpc, owner)
			row.OwnerKind = kind
			row.MultisigThreshold = threshold
			row.MultisigOwners = count
			row.ManagerType = classifyManagerType(client, rpc, owner, kind)
		}
	}

	return row
}

// classifyOwner determines whether the owner is an EOA, a Gnosis Safe multisig, an
// OZ timelock, or some other contract.
func classifyOwner(client *http.Client, rpc, owner string) (kind string, threshold *uint16, count *uint16) {
	code, err := ethGetCode(client, rpc, owner)
	if err != nil || !hasCode(code) {
		return "eoa", nil, nil
	}
	// Gnosis Safe: both getThreshold() and getOwners() respond.
	thrRes, e1 := ethCall(client, rpc, owner, getThresholdSelector)
	ownRes, e2 := ethCall(client, rpc, owner, getOwnersSelector)
	if e1 == nil && hasResult(thrRes) && e2 == nil && hasResult(ownRes) {
		if w, ok := wordAt(thrRes, 0); ok {
			t := uint16(hexWordToUint64(w))
			c := uint16(decodeArrayLen(ownRes))
			return "multisig", &t, &c
		}
	}
	// OZ TimelockController: getMinDelay() responds.
	if res, err := ethCall(client, rpc, owner, getMinDelaySelector); err == nil && hasResult(res) {
		return "timelock", nil, nil
	}
	return "contract", nil, nil
}

// classifyManagerType infers PoA vs PoS from the owner. An EOA/multisig/timelock owner
// means the admin governs the ValidatorManager directly (PoA). A plain contract owner is
// a StakingManager (PoS); we try to tell native from ERC20 via a staking-token getter.
func classifyManagerType(client *http.Client, rpc, owner, kind string) string {
	switch kind {
	case "eoa", "multisig", "timelock":
		return "PoA"
	case "contract":
		if _, ok := erc20StakingToken(client, rpc, owner); ok {
			return "PoS-erc20"
		}
		if isPoSStakingManager(client, rpc, owner) {
			return "PoS-native"
		}
		return "unknown"
	default:
		return "unknown"
	}
}

// erc20StakingToken returns a non-zero ERC20 token address if the contract exposes one.
func erc20StakingToken(client *http.Client, rpc, addr string) (string, bool) {
	for _, sel := range []string{erc20Selector, stakingTokenSelector, tokenSelector} {
		if res, err := ethCall(client, rpc, addr, sel); err == nil {
			if token, ok := nonZeroAddr(res); ok {
				return token, true
			}
		}
	}
	return "", false
}

// isPoSStakingManager returns true if the contract exposes PoS staking-manager getters.
func isPoSStakingManager(client *http.Client, rpc, addr string) bool {
	for _, sel := range []string{rewardCalculatorSelector, weightToValueFactorSelector} {
		if res, err := ethCall(client, rpc, addr, sel); err == nil && hasResult(res) {
			return true
		}
	}
	return false
}

// upgradeDelay returns the timelock min-delay controlling upgrades, or 0 (instant) if the
// upgrade controller is not a timelock.
func upgradeDelay(client *http.Client, rpc, addr string) uint64 {
	if res, err := ethCall(client, rpc, addr, getMinDelaySelector); err == nil && hasResult(res) {
		if w, ok := wordAt(res, 0); ok {
			return hexWordToUint64(w)
		}
	}
	return 0
}

// workingRPC returns the first RPC (the L1's own chain first, then C-Chain fallback) that
// has contract code at the ValidatorManager address, along with a location label
// ("self" for the L1's own chain, "c-chain" for C-Chain). Returns "", "" if none does.
func workingRPC(client *http.Client, in riskInput) (rpcURL, location string) {
	candidates := []struct{ url, loc string }{
		{in.RpcURL, "self"},
		{cChainRPC, "c-chain"},
	}
	seen := map[string]bool{}
	for _, c := range candidates {
		if c.url == "" || seen[c.url] {
			continue
		}
		seen[c.url] = true
		if code, err := ethGetCode(client, c.url, in.VMAddress); err == nil && hasCode(code) {
			return c.url, c.loc
		}
	}
	return "", ""
}

// --- ABI / hex helpers ---

// hasResult reports whether an eth_call returned at least one 32-byte word (i.e. didn't
// revert or hit a code-less address, both of which yield "0x").
func hasResult(res string) bool {
	return len(strings.TrimPrefix(res, "0x")) >= 64
}

// hasCode reports whether eth_getCode returned non-empty bytecode.
func hasCode(res string) bool {
	return res != "" && res != "0x"
}

// wordAt returns the i-th 32-byte word (64 hex chars) of an ABI-encoded result.
func wordAt(res string, i int) (string, bool) {
	h := strings.TrimPrefix(res, "0x")
	start := i * 64
	if len(h) < start+64 {
		return "", false
	}
	return h[start : start+64], true
}

// hexWordToUint64 parses a 32-byte hex word as an integer and returns its low 64 bits.
func hexWordToUint64(word string) uint64 {
	n := new(big.Int)
	n.SetString(word, 16)
	return n.Uint64()
}

// decodeArrayLen returns the length of an ABI-encoded dynamic array (word 0 is the offset,
// word 1 is the length).
func decodeArrayLen(res string) uint64 {
	if w, ok := wordAt(res, 1); ok {
		return hexWordToUint64(w)
	}
	return 0
}

// nonZeroAddr decodes a 32-byte word as an address and reports false for zero/invalid.
func nonZeroAddr(res string) (string, bool) {
	addr, err := decodeAddress(res)
	if err != nil || addr == zeroAddress {
		return "", false
	}
	return addr, true
}

// --- JSON-RPC helpers (raw HTTP, matching owner_syncer's dependency-free style) ---

func ethCall(client *http.Client, rpcURL, to, data string) (string, error) {
	return rpcResult(client, rpcURL, "eth_call", []interface{}{
		map[string]string{"to": to, "data": data}, "latest",
	})
}

func ethGetCode(client *http.Client, rpcURL, addr string) (string, error) {
	return rpcResult(client, rpcURL, "eth_getCode", []interface{}{addr, "latest"})
}

func ethGetStorageAt(client *http.Client, rpcURL, addr, slot string) (string, error) {
	return rpcResult(client, rpcURL, "eth_getStorageAt", []interface{}{addr, slot, "latest"})
}

// rpcResult performs a JSON-RPC POST and returns the raw "result" string. A JSON-RPC
// error (e.g. execution reverted) is returned as a Go error.
func rpcResult(client *http.Client, rpcURL, method string, params []interface{}) (string, error) {
	request := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	}
	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", err
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var rpcResp struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return "", err
	}
	if rpcResp.Error != nil {
		return "", fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}
	return rpcResp.Result, nil
}
