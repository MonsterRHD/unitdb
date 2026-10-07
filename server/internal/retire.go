/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package internal

// A contract's retirement (unitdb/retire), opened and confirmed by the
// contract's primary client, permanently takes the contract out of service:
// once opened, a generation is persisted and held by every node with the
// security state, so the contract is barred whatever node is up, and a node
// that was down is barred when it is back, from its own copy.
//
// Every node drives the same phases for every retirement it knows, by the
// generation: it confirms each phase into the converging state, and advances
// only once every node it can reach that can retire confirmed the phase
// before, so the phases order cluster-wide:
//
//  1. barred: refuse new clients, client ids, topic keys, publishes,
//     subscriptions and relays of the contract;
//  2. drained: close the contract's connections still open, on the node and
//     the proxied ones, and remove its subscriptions;
//  3. purged: delete the contract's messages, reliable replicas, topic index
//     entries, replica hints, dedup ids, session logs and revocation records,
//     and confirm the local legacy scan is empty.
//
// A phase's progress is persisted before it is confirmed, so a crash or a
// failed step resumes at that phase, repeating idempotent removals; a
// repeated request opens no second retirement, the generation is fixed for
// the contract. The retirement completes once every reachable node confirmed
// the purge with an empty scan; a node not reached, or one that cannot retire
// yet, stays in the answer as the blocker, and does its phases when it can.
// The retirement itself is never removed, so the contract never serves
// again; TTL expiry, other contracts, ordinary disconnects and rebalancing
// are untouched.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/pkg/hash"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
)

const (
	// Retirement phases a node confirms, in order.
	retireBarred  = 1
	retireDrained = 2
	retirePurged  = 3

	// retireStandalone is this node's name in the retirement state when it
	// is no part of a cluster.
	retireStandalone = "-"

	// retireScanTimeout bounds asking one node for its phase and leftovers.
	retireScanTimeout = 2 * time.Second
)

// requestRetire opens or reads a contract's retirement (see onRetire).
var requestRetire = hash.WithSalt([]byte("retire"), message.Contract)

// retireInterval is the time between a node's runs of its retirement phases.
// Set by UNITDB_RETIRE_INTERVAL, as a duration, for tests; 500ms otherwise.
var retireInterval = func() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("UNITDB_RETIRE_INTERVAL")); err == nil && d > 0 {
		return d
	}
	return 500 * time.Millisecond
}()

// Retirement is the persistent retirement of one contract: its generation,
// when it opened, the phase each node confirmed, and whether it completed.
// It merges whatever order its changes arrive in, so every node's copy
// converges.
type Retirement struct {
	// Generation is fixed for the contract (retirementGeneration): a
	// repeated request never opens a second retirement.
	Generation uint64 `json:"generation"`
	// At is the unix second the generation opened, the earliest seen.
	At int64 `json:"at,omitempty"`
	// Nodes is the highest phase each node confirmed, by its name.
	Nodes map[string]int `json:"nodes,omitempty"`
	// Done is set once every reachable node confirmed the purge with an
	// empty scan. It stays set.
	Done bool `json:"done,omitempty"`
}

// retirementGeneration derives the one generation a contract's retirement
// can have, deterministically: two requests, even to two nodes, open the same
// retirement, and a retry cannot start a second one.
func retirementGeneration(contract uint32) uint64 {
	h := hash.WithSalt([]byte("contract-retirement"), contract)
	return uint64(h)<<32 | uint64(contract)
}

// merge merges o into t and reports whether t changed. Merges commute: the
// fixed generation, the earliest opening time, the highest phase per node,
// and done once set.
func (t *Retirement) merge(o *Retirement) bool {
	changed := false
	if o.Generation != 0 && t.Generation == 0 {
		t.Generation, changed = o.Generation, true
	}
	if o.At != 0 && (t.At == 0 || o.At < t.At) {
		t.At, changed = o.At, true
	}
	if o.Done && !t.Done {
		t.Done, changed = true, true
	}
	for node, phase := range o.Nodes {
		if phase > t.Nodes[node] {
			if t.Nodes == nil {
				t.Nodes = make(map[string]int)
			}
			t.Nodes[node], changed = phase, true
		}
	}
	return changed
}

func (t *Retirement) copy() *Retirement {
	c := &Retirement{Generation: t.Generation, At: t.At, Done: t.Done}
	if len(t.Nodes) > 0 {
		c.Nodes = make(map[string]int, len(t.Nodes))
		for k, v := range t.Nodes {
			c.Nodes[k] = v
		}
	}
	return c
}

// mergeRetiredLocked merges retirement changes, and returns the ones that
// changed as they are now. The caller holds r.mu, or is alone with r.
func (r *revocations) mergeRetiredLocked(changes map[uint32]*Retirement) map[uint32]*Retirement {
	changed := make(map[uint32]*Retirement)
	for contract, o := range changes {
		if o == nil {
			continue
		}
		t := r.retired[contract]
		if t == nil {
			t = &Retirement{}
		}
		if t.merge(o) {
			r.retired[contract] = t
			changed[contract] = t.copy()
		}
	}
	return changed
}

// isRetired reports whether the contract was retired. Nil receiver safe.
func (r *revocations) isRetired(contract uint32) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.retired[contract] != nil
}

// retirement returns a copy of the contract's retirement.
func (r *revocations) retirement(contract uint32) (Retirement, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t := r.retired[contract]
	if t == nil {
		return Retirement{}, false
	}
	return *t.copy(), true
}

// allRetired returns a copy of every retirement.
func (r *revocations) allRetired() map[uint32]*Retirement {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m := make(map[uint32]*Retirement, len(r.retired))
	for k, v := range r.retired {
		m[k] = v.copy()
	}
	return m
}

// retireName is this node's name in retirement state.
func retireName() string {
	if c := Globals.Cluster; c != nil {
		return c.thisNodeName
	}
	return retireStandalone
}

// retireMembers are every node a retirement's confirmation is from: the
// whole configured cluster, this node included, or just this node.
func retireMembers() []string {
	if c := Globals.Cluster; c != nil {
		return append([]string(nil), c.allNodes...)
	}
	return []string{retireStandalone}
}

// isConnected reports whether the node is currently connected, without
// redialing.
func (n *ClusterNode) isConnected() bool {
	n.lock.Lock()
	defer n.lock.Unlock()
	return n.connected && (n.conn == nil || !n.conn.isClosed())
}

// localPhase returns this node's persisted phase for a contract, 0 before it
// confirmed one.
func (r *revocations) localPhase(contract uint32) store.RetireProgress {
	r.retireMu.Lock()
	defer r.retireMu.Unlock()
	return r.progress[contract]
}

// rememberRetiredBlocksLocked records sessions whose logs this node purged
// with a retirement, so a late hint of one is dropped. The caller holds
// r.retireMu.
func (r *revocations) rememberRetiredBlocksLocked(blocks []uint32) {
	for _, b := range blocks {
		r.retireBlocks[b] = true
	}
}

// isRetiredBlock reports whether a session log change's session was purged
// with a retirement.
func isRetiredBlock(block uint32) bool {
	r := securityState.Load()
	if r == nil {
		return false
	}
	r.retireMu.Lock()
	defer r.retireMu.Unlock()
	return r.retireBlocks[block]
}

// reachableAckedLocked reports whether every reachable node that can retire
// confirmed at least phase, this node's persisted progress included. A
// reachable node without the capability is skipped for the phase gates: the
// capable nodes still retire among themselves, while allReachableCapable
// keeps the retirement from completing until the old node can retire what it
// holds. An unreachable node does not block a phase, it confirms when it is
// back. The caller holds r.retireMu.
func (r *revocations) reachableAckedLocked(t Retirement, phase, localPhase int) bool {
	self := retireName()
	if localPhase < phase {
		return false
	}
	c := Globals.Cluster
	for _, name := range retireMembers() {
		if name == self || c == nil {
			continue
		}
		n := c.nodes[name]
		if n == nil || !n.isConnected() || !n.supports(capRetire) {
			continue
		}
		if t.Nodes[name] < phase {
			return false
		}
	}
	return true
}

// allReachableCapable reports that every node currently reachable can
// retire; a reachable older node blocks completion, and is named in the
// status, until it is upgraded.
func (r *revocations) allReachableCapable() bool {
	c := Globals.Cluster
	if c == nil {
		return true
	}
	for _, name := range c.allNodes {
		if name == c.thisNodeName {
			continue
		}
		n := c.nodes[name]
		if n != nil && n.isConnected() && !n.supports(capRetire) {
			return false
		}
	}
	return true
}

// ackConfirmed merges this node's confirmation of a phase for the contract.
func (r *revocations) ackConfirmed(contract uint32, t Retirement, phase int) {
	upd := Retirement{Generation: t.Generation, At: t.At, Nodes: map[string]int{retireName(): phase}}
	if _, err := r.applyRetired(map[uint32]*Retirement{contract: &upd}, ""); err != nil {
		log.ErrLogger.Error().Err(err).Uint32("contract", contract).Int("phase", phase).Str("context", "retire.ack").Msg("unable to confirm a retirement phase")
	}
}

// drainContract closes the contract's connections on this node, both its own
// clients and the ones proxied for clients of other nodes, and removes the
// contract's subscriptions held here. It runs once every reachable node bars
// the contract.
func (r *revocations) drainContract(contract uint32) {
	// Close the contract's connections without waiting here: a connection's
	// request handler can be blocked on the state this drain runs under, so
	// waiting for its read loop to exit would deadlock. The close is
	// idempotent, and the wildcard purge below removes its subscriptions.
	if Globals.connCache != nil {
		for _, cn := range Globals.connCache.all() {
			if cn.clientID == nil || cn.clientID.Contract() != contract {
				continue
			}
			if cn.clnode != nil {
				// Its write loop's exit removes the subscriptions held for
				// the proxied client.
				cn.stopRPC()
				continue
			}
			go cn.close()
		}
	}
	// Every subscription of the contract, including ones no connection is
	// open for; the purge removes them again, in case one was moved here.
	if n, err := store.Subscription.Retire(contract); err != nil {
		log.ErrLogger.Error().Err(err).Uint32("contract", contract).Str("context", "retire.drain").Msg("unable to remove the contract's subscriptions")
	} else if n > 0 {
		log.ErrLogger.Info().Uint32("contract", contract).Int("subscriptions", n).Str("context", "retire.drain").Msg("removed a retired contract's subscriptions")
	}
	// Hints kept only in memory for the contract: never hand them off.
	if c := Globals.Cluster; c != nil {
		c.dropRetiredPending(contract)
	}
}

// purgeContract deletes everything the node holds of the contract but the
// retirement itself, removes its revocation records, and returns the
// resources left and the sessions whose logs it removed. blocks names
// sessions already purged in an earlier run.
func (r *revocations) purgeContract(contract uint32, previous []uint32) (store.RetireLeftovers, []uint32, error) {
	known := make(map[uint32]bool, len(previous))
	for _, b := range previous {
		known[b] = true
	}
	for _, b := range store.ContractSessionBlocks(contract) {
		known[b] = true
	}
	left, err := store.RetireContract(contract, known)
	if err != nil {
		return left, previous, err
	}
	blocks := make([]uint32, 0, len(known))
	for b := range known {
		blocks = append(blocks, b)
	}
	r.rememberRetiredBlocksLocked(blocks)
	// The contract's revocation records: the retirement supersedes them.
	r.mu.Lock()
	delete(r.contracts, contract)
	err = r.saveLocked()
	r.mu.Unlock()
	return left, blocks, err
}

// markLocalPhase records the phase this node confirmed in the snapshot the
// phase gate reads, initializing its node map.
func markLocalPhase(t *Retirement, phase int) {
	if t.Nodes == nil {
		t.Nodes = make(map[string]int)
	}
	t.Nodes[retireName()] = phase
}

// advance runs the next local phase of one retirement, if its gate is open.
// It is idempotent: a phase already persisted is confirmed again, and a purge
// repeated, so a crash or a failed step resumes here.
func (r *revocations) advance(contract uint32) {
	if !hasCapability(capRetire) {
		return
	}
	r.retireMu.Lock()
	defer r.retireMu.Unlock()
	t, ok := r.retirement(contract)
	if !ok || t.Generation == 0 {
		return
	}
	prog := r.progress[contract]
	startedAt := prog.Phase

	// Phase 1: the contract is barred from the state, which is persisted
	// before the request is answered; record that this node enforces it.
	if prog.Phase < retireBarred {
		p := store.RetireProgress{Contract: contract, Generation: t.Generation, Phase: retireBarred}
		if err := store.SaveRetireProgress(p); err != nil {
			log.ErrLogger.Error().Err(err).Uint32("contract", contract).Str("context", "retire").Msg("unable to persist the barred phase")
			return
		}
		r.progress[contract] = p
		r.ackConfirmed(contract, t, retireBarred)
		markLocalPhase(&t, retireBarred)
		prog = p
	}

	// Phase 2: once every reachable node bars it, close connections and
	// remove subscriptions.
	if prog.Phase == retireBarred && r.reachableAckedLocked(t, retireBarred, prog.Phase) {
		r.drainContract(contract)
		p := store.RetireProgress{Contract: contract, Generation: t.Generation, Phase: retireDrained}
		if err := store.SaveRetireProgress(p); err != nil {
			log.ErrLogger.Error().Err(err).Uint32("contract", contract).Str("context", "retire").Msg("unable to persist the drained phase")
			return
		}
		r.progress[contract] = p
		r.ackConfirmed(contract, t, retireDrained)
		markLocalPhase(&t, retireDrained)
		prog = p
	}

	// Phase 3: once every reachable node drained, purge, verify and confirm.
	if prog.Phase == retireDrained && r.reachableAckedLocked(t, retireDrained, prog.Phase) {
		r.finishPurge(contract, &t, &prog)
	}

	// Resuming phase 3 after a crash, before this node confirmed it: redo
	// the idempotent purge and scan. The transition above does not re-run it
	// in the same tick.
	if startedAt == retirePurged && prog.Phase == retirePurged && t.Nodes[retireName()] < retirePurged {
		r.finishPurge(contract, &t, &prog)
	}

	// Completion: every reachable node capable of retiring confirmed an
	// empty purge, and no reachable node lacks the capability.
	if prog.Phase >= retirePurged {
		current, ok := r.retirement(contract)
		if ok && !current.Done && r.allReachableCapable() && r.reachableAckedLocked(current, retirePurged, prog.Phase) {
			upd := Retirement{Generation: current.Generation, At: current.At, Done: true}
			if _, err := r.applyRetired(map[uint32]*Retirement{contract: &upd}, ""); err != nil {
				log.ErrLogger.Error().Err(err).Uint32("contract", contract).Str("context", "retire.done").Msg("unable to complete the retirement")
			}
		}
	}
}

// finishPurge purges the contract, persists phase 3 with the sessions purged,
// and confirms the phase once the scan is empty. The caller holds r.retireMu.
func (r *revocations) finishPurge(contract uint32, t *Retirement, prog *store.RetireProgress) {
	left, blocks, err := r.purgeContract(contract, prog.Blocks)
	if err != nil {
		log.ErrLogger.Error().Err(err).Uint32("contract", contract).Str("context", "retire").Msg("unable to purge the retired contract: will retry")
		return
	}
	if !left.Empty() {
		// A resource reappeared: the purge repeats on the next run, and the
		// phase is not confirmed until a scan is empty.
		log.ErrLogger.Warn().Uint32("contract", contract).Interface("left", left.Map()).Str("context", "retire").Msg("a retired contract still holds resources: purging again")
		return
	}
	p := store.RetireProgress{Contract: contract, Generation: t.Generation, Phase: retirePurged, Blocks: blocks}
	if err := store.SaveRetireProgress(p); err != nil {
		log.ErrLogger.Error().Err(err).Uint32("contract", contract).Str("context", "retire").Msg("unable to persist the purged phase")
		return
	}
	r.progress[contract] = p
	*prog = p
	r.ackConfirmed(contract, *t, retirePurged)
}

// runRetirements drives every known retirement until the service stops.
func (s *_Service) runRetirements(ctx context.Context) {
	if !hasCapability(capRetire) {
		return
	}
	ticker := time.NewTicker(retireInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r := securityState.Load()
			if r == nil {
				continue
			}
			for contract := range r.allRetired() {
				r.advance(contract)
			}
		}
	}
}

// RetireScanReq asks a node for its phase of a contract's retirement and the
// resources it still holds, without changing either.
type RetireScanReq struct {
	Node       string
	Contract   uint32
	Generation uint64
}

// RetireScanResp is the node's answer.
type RetireScanResp struct {
	Phase      int
	Generation uint64
	Done       bool
	Left       map[string]int
}

// RetireScan answers a node's status request for a retirement. Called by
// another node.
func (c *Cluster) RetireScan(req *RetireScanReq, resp *RetireScanResp) error {
	if err := refuse(capRetire); err != nil {
		return err
	}
	if c.nodes[req.Node] == nil {
		return errUnknownNode
	}
	r := securityState.Load()
	if r == nil {
		return errors.New("cluster: the security state is not loaded yet")
	}
	t, ok := r.retirement(req.Contract)
	if !ok {
		return nil
	}
	prog := r.localPhase(req.Contract)
	resp.Phase, resp.Generation, resp.Done = prog.Phase, t.Generation, t.Done
	blocks := map[uint32]bool{}
	for _, b := range prog.Blocks {
		blocks[b] = true
	}
	resp.Left = store.ScanRetire(req.Contract, append([]string(nil), c.allNodes...), blocks).Map()
	return nil
}

// retireStateName names a phase for an answer.
func retireStateName(phase int, done bool) string {
	if done {
		return types.RetireDone
	}
	switch phase {
	case retireBarred:
		return types.RetireBarred
	case retireDrained:
		return types.RetireDraining
	case retirePurged:
		return types.RetirePurging
	default:
		return types.RetireBarred
	}
}

// retireStatus builds the diagnostic answer for a contract's retirement:
// this node's state and leftovers, and each other node's phase, reachability
// and leftovers, asked directly.
func (r *revocations) retireStatus(contract uint32) *types.RetireResponse {
	t, ok := r.retirement(contract)
	if !ok {
		return nil
	}
	prog := r.localPhase(contract)
	resp := &types.RetireResponse{
		Status:     200,
		Contract:   contract,
		Generation: t.Generation,
		State:      retireStateName(prog.Phase, t.Done),
		Done:       t.Done,
	}
	blocks := map[uint32]bool{}
	for _, b := range prog.Blocks {
		blocks[b] = true
	}
	selfLeft := store.ScanRetire(contract, store.HintNodes(), blocks).Map()
	if len(selfLeft) > 0 {
		resp.Remaining = selfLeft
	}
	self := retireName()
	for _, name := range retireMembersSorted() {
		ns := types.RetireNodeStatus{Node: name, State: "blocked"}
		if name == self {
			ns.Reachable, ns.Phase = true, prog.Phase
			ns.State = retireStateName(prog.Phase, t.Done)
			ns.Remaining = selfLeft
			resp.Nodes = append(resp.Nodes, ns)
			continue
		}
		c := Globals.Cluster
		n := c.nodes[name]
		ack := t.Nodes[name]
		if n == nil || !n.isConnected() {
			ns.Reachable, ns.Phase = false, ack
			ns.State = "unreachable"
			resp.Nodes = append(resp.Nodes, ns)
			continue
		}
		ns.Reachable = true
		if !n.supports(capRetire) {
			ns.State = "unsupported"
			resp.Nodes = append(resp.Nodes, ns)
			continue
		}
		var scan RetireScanResp
		if err := n.callTimeout("Cluster.RetireScan", &RetireScanReq{Node: self, Contract: contract, Generation: t.Generation}, &scan, retireScanTimeout); err != nil {
			n.lacks(err, capRetire)
			ns.Reachable, ns.Phase = false, ack
			ns.State = "unreachable"
		} else {
			ns.Phase = scan.Phase
			ns.State = retireStateName(scan.Phase, scan.Done)
			ns.Remaining = scan.Left
		}
		resp.Nodes = append(resp.Nodes, ns)
	}
	return resp
}

// retireMembersSorted is retireMembers sorted, for a stable answer.
func retireMembersSorted() []string {
	members := retireMembers()
	sort.Strings(members)
	return members
}

// onRetire handles a unitdb/retire request, from the contract's primary
// client: with confirm it opens the contract's one retirement, persistently;
// without it it reads the status. Either way it answers with the generation
// and the blockers.
func (c *_Conn) onRetire(payload []byte) (interface{}, bool) {
	if !hasCapability(capRetire) {
		return types.ErrNotFound, false
	}
	if !c.clientID.IsPrimary() {
		return types.ErrForbidden, false
	}
	var req types.RetireRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return types.ErrBadRequest, false
	}
	r := c.service.revocations
	contract := c.clientID.Contract()
	t, exists := r.retirement(contract)
	if req.Confirm && !exists {
		t = Retirement{Generation: retirementGeneration(contract), At: time.Now().Unix()}
		if _, err := r.applyRetired(map[uint32]*Retirement{contract: &t}, ""); err != nil {
			log.ErrLogger.Error().Err(err).Uint32("contract", contract).Str("context", "conn.onRetire").Msg("unable to persist the retirement")
			return types.ErrServerError, false
		}
	} else if !exists {
		return types.ErrNotFound, false
	}
	// The phases run on the retirement driver, not on the caller's
	// goroutine: draining closes this contract's connections, including the
	// caller's, and the answer is sent first. The driver's next tick
	// persists the local bar and advances.
	return r.retireStatus(contract), true
}

// startRetirements resumes every known retirement's phases on boot.
// Enforcement itself needs no resume: the retirements are in the security
// state loaded before the node serves.
func (s *_Service) startRetirements() {
	if !hasCapability(capRetire) {
		return
	}
	s.retireWG.Add(1)
	go func() {
		defer s.retireWG.Done()
		s.runRetirements(s.context)
	}()
}
