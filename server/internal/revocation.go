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

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/pkg/hash"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
)

// A contract's primary client revokes the contract's client ids and topic
// keys with unitdb/revoke: one by its uuid, or everything the contract
// issued before now (its not-before time). An id sealed again from a v1 one
// (server/cmd/mintid -from) has no uuid: it is revoked by a not-before time,
// as it has an issue time.
//
// The security state, what was revoked in each contract, is held by every
// node, as wildcard subscriptions are: a change is sent to every other node,
// and a node that reconnects exchanges the whole state with the node it
// reconnects to. States merge whatever their order, so they converge. Each
// node keeps it in its store (store.Security), and checks it when it opens a
// client id (CONNECT, unitdb/service) or checks a topic key, its own
// clients' and the ones other nodes forward to it.
//
// An older node, without capRevocations, is sent none of it, and refuses
// nothing: an id is refused only where its client connects to a node that
// has the state, and a key where the client's node or the topic's owner has
// it. See docs/rolling-deploys.md.

// requestRevoke revokes client ids and topic keys (see onRevoke).
var requestRevoke = hash.WithSalt([]byte("revoke"), message.Contract)

// revocationsTimeout bounds a call that sends the security state.
const revocationsTimeout = 2 * time.Second

// ContractState is what the cluster revoked in one contract.
type ContractState struct {
	// NotBefore refuses the client ids and topic keys issued before it, in
	// unix seconds.
	NotBefore int64 `json:"not_before,omitempty"`
	// Revoked refuses the client ids and topic keys with these uuids, until
	// a unix time, 0 for ever.
	Revoked map[uint64]int64 `json:"revoked,omitempty"`
}

// expired reports whether a revocation until until is over at now.
func expired(until, now int64) bool {
	return until != 0 && until <= now
}

// merge merges o into s, and reports whether s changed. The later not-before
// time is kept, and for each uuid the later end, 0 (for ever) the latest:
// merges commute, so states converge whatever order they arrive in. A
// revocation that is over at now is dropped.
func (s *ContractState) merge(o *ContractState, now int64) bool {
	changed := false
	if o.NotBefore > s.NotBefore {
		s.NotBefore, changed = o.NotBefore, true
	}
	for uuid, until := range o.Revoked {
		if uuid == 0 || expired(until, now) {
			continue
		}
		if cur, ok := s.Revoked[uuid]; ok && (cur == 0 || (until != 0 && until <= cur)) {
			continue
		}
		if s.Revoked == nil {
			s.Revoked = make(map[uint64]int64)
		}
		s.Revoked[uuid], changed = until, true
	}
	return changed
}

// prune drops the revocations that are over at now.
func (s *ContractState) prune(now int64) {
	for uuid, until := range s.Revoked {
		if expired(until, now) {
			delete(s.Revoked, uuid)
		}
	}
}

func (s *ContractState) empty() bool {
	return s.NotBefore == 0 && len(s.Revoked) == 0
}

func (s *ContractState) copy() *ContractState {
	c := &ContractState{NotBefore: s.NotBefore}
	if len(s.Revoked) > 0 {
		c.Revoked = make(map[uint64]int64, len(s.Revoked))
		for k, v := range s.Revoked {
			c.Revoked[k] = v
		}
	}
	return c
}

// securityState is this node's security state, for the cluster's calls.
var securityState atomic.Pointer[revocations]

// revocations is this node's copy of the security state.
type revocations struct {
	mu        sync.RWMutex
	contracts map[uint32]*ContractState
	// retired is the contracts' retirements, one per contract for ever, by
	// their generation. A retired contract's ContractState is not kept: the
	// retirement refuses every id and key, whatever their issue time.
	retired map[uint32]*Retirement
	// ids are the store's records of the state, deleted once a newer one
	// is written.
	ids [][]byte

	// Local progress through each retirement, and the sessions purged with
	// one, for fencing a late session-log hint. retireMu serializes the
	// phase transitions; the two maps are read with it too.
	retireMu     sync.Mutex
	progress     map[uint32]store.RetireProgress
	retireBlocks map[uint32]bool
}

// securityRecord is a record of the security state in the store.
type securityRecord struct {
	ID        []byte                    `json:"id"`
	Contracts map[uint32]*ContractState `json:"contracts"`
	// Retired is the contracts' retirements, held for ever.
	Retired map[uint32]*Retirement `json:"retired,omitempty"`
}

// loadRevocations reads the security state from the store, and makes it
// this node's. Several records, left by a crash between writing a record and
// deleting the one before, are merged and written again as one.
//
// The records a v0.6.0 node stored under a fixed id are merged too, the
// state written where it is kept now, and then they are deleted: a crash in
// between leaves them to be merged again, which changes nothing.
func loadRevocations() (*revocations, error) {
	r := &revocations{contracts: make(map[uint32]*ContractState), retired: make(map[uint32]*Retirement), progress: make(map[uint32]store.RetireProgress), retireBlocks: make(map[uint32]bool)}
	raw, err := store.Security.All()
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "revocations").Msg("unable to read the security state")
	}
	legacyIDs, legacy, err := store.Security.Legacy()
	if err != nil {
		return nil, fmt.Errorf("revocations: reading the security state of an older version: %w", err)
	}
	now := time.Now().Unix()
	all := append(raw, legacy...)
	// Retirements first, so that the revocation records of a retired
	// contract are dropped as the records are merged.
	for i, b := range all {
		var rec securityRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "revocations").Msg("unreadable record of the security state: skipped")
			continue
		}
		if i < len(raw) && len(rec.ID) > 0 {
			r.ids = append(r.ids, rec.ID)
		}
		r.mergeRetiredLocked(rec.Retired)
	}
	for i, b := range all {
		var rec securityRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			continue
		}
		if i < len(raw) && len(rec.ID) > 0 && !hasID(r.ids, rec.ID) {
			r.ids = append(r.ids, rec.ID)
		}
		r.mergeLocked(rec.Contracts, now)
	}
	// This node's progress through retirements it knew. The retirements
	// themselves are in the state above, so a node enforces them from boot,
	// before it serves, and resumes the phases it had not confirmed.
	progress, err := store.RetireProgressAll()
	if err != nil {
		return nil, fmt.Errorf("revocations: reading retirement progress: %w", err)
	}
	for _, p := range progress {
		r.progress[p.Contract] = p
		for _, b := range p.Blocks {
			r.retireBlocks[b] = true
		}
	}
	if len(r.ids) > 1 || len(legacyIDs) > 0 {
		r.mu.Lock()
		err := r.saveLocked()
		r.mu.Unlock()
		if err != nil {
			if len(legacyIDs) > 0 {
				return nil, fmt.Errorf("revocations: moving the security state of an older version: %w", err)
			}
			log.ErrLogger.Error().Err(err).Str("context", "revocations").Msg("unable to save the security state")
		}
	}
	for _, id := range legacyIDs {
		if err := store.Security.DeleteLegacy(id); err != nil {
			return nil, fmt.Errorf("revocations: deleting the security state of an older version: %w", err)
		}
	}
	if len(legacyIDs) > 0 {
		log.ErrLogger.Info().Str("context", "revocations").Int("records", len(legacyIDs)).Msg("moved the security state of an older version to a $sys topic")
	}
	securityState.Store(r)
	return r, nil
}

// hasID reports whether ids hold id.
func hasID(ids [][]byte, id []byte) bool {
	for _, x := range ids {
		if bytes.Equal(x, id) {
			return true
		}
	}
	return false
}

// refuses returns why the client id or topic key of contract with uuid (0
// for none), issued at issuedAt, is refused, or "" if it isn't.
func (r *revocations) refuses(contract uint32, uuid uint64, issuedAt uint32) string {
	if r == nil {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := r.contracts[contract]
	if s == nil {
		return ""
	}
	if s.NotBefore != 0 && int64(issuedAt) < s.NotBefore {
		return "issued before the contract's not-before time"
	}
	if until, ok := s.Revoked[uuid]; ok && uuid != 0 && !expired(until, time.Now().Unix()) {
		return "revoked"
	}
	return ""
}

// apply merges changes into the state and saves it, and sends what changed
// to every other node but from, if set: the node changes came from. It
// returns what changed, as it is now.
func (r *revocations) apply(changes map[uint32]*ContractState, from string) (map[uint32]*ContractState, error) {
	changed, _, err := r.applyAll(changes, nil, from)
	return changed, err
}

// applyAll merges revocation and retirement changes into the state, saves it
// once, and sends what changed to every other node but from, if set. It
// returns each part that changed, as it is now. Retirements are merged first:
// a retired contract's revocation records are dropped whatever the change
// carries.
func (r *revocations) applyAll(changes map[uint32]*ContractState, retired map[uint32]*Retirement, from string) (map[uint32]*ContractState, map[uint32]*Retirement, error) {
	r.mu.Lock()
	changedR := r.mergeRetiredLocked(retired)
	changedC := r.mergeLocked(changes, time.Now().Unix())
	var err error
	if len(changedC) > 0 || len(changedR) > 0 {
		err = r.saveLocked()
	}
	r.mu.Unlock()
	if err == nil && (len(changedC) > 0 || len(changedR) > 0) {
		if c := Globals.Cluster; c != nil {
			go c.sendRevocations(changedC, changedR, nil, from)
		}
	}
	return changedC, changedR, err
}

// applyRetired merges retirement changes, saves and sends them.
func (r *revocations) applyRetired(retired map[uint32]*Retirement, from string) (map[uint32]*Retirement, error) {
	_, changed, err := r.applyAll(nil, retired, from)
	return changed, err
}

// mergeLocked merges changes into the state, and returns the contracts that
// changed, as they are now. The caller holds r.mu, or is the only one with r.
func (r *revocations) mergeLocked(changes map[uint32]*ContractState, now int64) map[uint32]*ContractState {
	changed := make(map[uint32]*ContractState)
	for contract, o := range changes {
		if o == nil {
			continue
		}
		if r.retired[contract] != nil {
			// A retired contract keeps no revocation records: the
			// retirement refuses every id and key, and the purge removed
			// them on every node. Never take them from another node.
			delete(r.contracts, contract)
			continue
		}
		s := r.contracts[contract]
		if s == nil {
			s = &ContractState{}
		}
		s.prune(now)
		if s.merge(o, now) {
			changed[contract] = s.copy()
		}
		if s.empty() {
			delete(r.contracts, contract)
		} else {
			r.contracts[contract] = s
		}
	}
	return changed
}

// saveLocked writes the whole state as a new record, then deletes the ones
// before. The caller holds r.mu.
func (r *revocations) saveLocked() error {
	id, err := store.Security.NewID()
	if err != nil {
		return err
	}
	b, err := json.Marshal(securityRecord{ID: id, Contracts: r.contracts, Retired: r.retired})
	if err != nil {
		return err
	}
	if err := store.Security.Put(id, b); err != nil {
		return err
	}
	for _, old := range r.ids {
		if err := store.Security.Delete(old); err != nil {
			// Merged again when read.
			log.ErrLogger.Debug().Err(err).Str("context", "revocations").Msg("unable to delete a record of the security state")
		}
	}
	r.ids = [][]byte{id}
	return nil
}

// all returns a copy of the whole state.
func (r *revocations) all() map[uint32]*ContractState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m := make(map[uint32]*ContractState, len(r.contracts))
	for k, v := range r.contracts {
		m[k] = v.copy()
	}
	return m
}

// RevocationsReq carries security state from one node to another: what
// changed, or with Full the sender's whole state, which the receiver answers
// with its own. Retired carries contracts' retirements, to nodes that support
// them: an older node is sent none.
type RevocationsReq struct {
	Node      string
	Contracts map[uint32]*ContractState
	Retired   map[uint32]*Retirement
	Full      bool
}

// RevocationsResp answers a RevocationsReq: with Full, the receiver's whole
// state.
type RevocationsResp struct {
	Contracts map[uint32]*ContractState
	Retired   map[uint32]*Retirement
}

// sendRevocations sends changes to every other node but skip, or to the
// node to only. With to, it sends the whole state, and merges the node's
// whole state, its answer. Retirements are sent only to nodes that support
// them, as older nodes know nothing of them.
func (c *Cluster) sendRevocations(changes map[uint32]*ContractState, retired map[uint32]*Retirement, to *ClusterNode, skip string) {
	if c == nil || !hasCapability(capRevocations) {
		return
	}
	var nodes []*ClusterNode
	if to != nil {
		nodes = append(nodes, to)
	} else {
		for name, n := range c.nodes {
			if name != skip {
				nodes = append(nodes, n)
			}
		}
	}
	for _, n := range nodes {
		if !n.supports(capRevocations) {
			continue
		}
		req := &RevocationsReq{Node: c.thisNodeName, Contracts: changes, Full: to != nil}
		if hasCapability(capRetire) && n.supports(capRetire) {
			req.Retired = retired
		}
		var resp RevocationsResp
		if err := n.callTimeout("Cluster.Revocations", req, &resp, revocationsTimeout); err != nil {
			if !n.lacks(err, capRevocations) {
				// It exchanges the whole state when it reconnects.
				log.ErrLogger.Debug().Err(err).Str("node", n.name).Msg("unable to send the security state")
			}
			continue
		}
		if r := securityState.Load(); r != nil && (len(resp.Contracts) > 0 || len(resp.Retired) > 0) {
			if _, _, err := r.applyAll(resp.Contracts, resp.Retired, n.name); err != nil {
				log.ErrLogger.Error().Err(err).Str("context", "revocations").Msg("unable to save the security state")
			}
		}
	}
}

// pushRevocations exchanges the whole security state with a node that
// (re)connected.
func (c *Cluster) pushRevocations(n *ClusterNode) {
	if r := securityState.Load(); r != nil {
		c.sendRevocations(r.all(), r.allRetired(), n, "")
	}
}

// errUnknownNode refuses a call from a node not in the cluster's config.
var errUnknownNode = errors.New("cluster: call from an unknown node")

// Revocations merges another node's security state into this one's, and
// sends what changed to the other nodes, in case the sender could not. With
// req.Full it answers with this node's whole state.
func (c *Cluster) Revocations(req *RevocationsReq, resp *RevocationsResp) error {
	if err := refuse(capRevocations); err != nil {
		return err
	}
	n := c.nodes[req.Node]
	if n == nil {
		return errUnknownNode
	}
	// The sender has the capability, whatever it told before.
	n.hasNow(capRevocations)
	if req.Retired != nil {
		n.hasNow(capRetire)
	}
	r := securityState.Load()
	if r == nil {
		return errors.New("cluster: the security state is not loaded yet")
	}
	var retired map[uint32]*Retirement
	if hasCapability(capRetire) {
		retired = req.Retired
	}
	if _, _, err := r.applyAll(req.Contracts, retired, req.Node); err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "revocations").Msg("unable to save the security state")
	}
	if req.Full {
		resp.Contracts = r.all()
		if hasCapability(capRetire) {
			resp.Retired = r.allRetired()
		}
	}
	return nil
}

// onRevoke handles a unitdb/revoke request, from a primary client of the
// contract: a trusted service's id (server/cmd/mintid -service) is one.
func (c *_Conn) onRevoke(payload []byte) (interface{}, bool) {
	if !hasCapability(capRevocations) {
		// As an older node, which knows no such request.
		return types.ErrNotFound, false
	}
	if !c.clientID.IsPrimary() {
		return types.ErrForbidden, false
	}
	if c.service.revocations.isRetired(c.clientID.Contract()) {
		// A retired contract keeps no revocation records: its retirement
		// refuses every id and key.
		return types.ErrContractRetired, false
	}
	var req types.RevokeRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return types.ErrBadRequest, false
	}
	now := time.Now().Unix()
	s := &ContractState{}
	switch {
	case req.Uuid != "":
		uuid, err := strconv.ParseUint(req.Uuid, 10, 64)
		if err != nil || uuid == 0 || req.Until < 0 || expired(req.Until, now) {
			return types.ErrBadRequest, false
		}
		s.Revoked = map[uint64]int64{uuid: req.Until}
	case req.Until != 0 || !req.All:
		// Nothing to revoke, or an end without a uuid.
		return types.ErrBadRequest, false
	}
	if req.All {
		s.NotBefore = now
	}
	if _, err := c.service.revocations.apply(map[uint32]*ContractState{c.clientID.Contract(): s}, ""); err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "conn.onRevoke").Msg("unable to save the security state")
		return types.ErrServerError, false
	}
	return &types.RevokeResponse{Status: 200}, true
}
