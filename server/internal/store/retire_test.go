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

package store_test

// Retiring a contract removes, of that contract alone, its messages and
// reliable replicas, its topic index entries, its subscriptions, the hints
// and dedup ids that refer to it, and its sessions' rows and logs; and
// repeats nothing the second time it runs. What another contract, and the
// node's own records, hold stays.

import (
	"fmt"
	"testing"

	"github.com/unit-io/unitdb/server/internal/store"
)

const (
	retireA uint32 = 0x5ea1ed01
	retireB uint32 = 0x5ea1ed02
)

// putRetirementData writes one of each resource a retirement removes for
// contract, and the same for another contract.
func putRetirementData(t *testing.T, c uint32, marker, topic, node string, block uint32, rowKey, logKey uint64) {
	t.Helper()
	m := []byte(marker)
	subID, err := store.Subscription.NewID()
	if err != nil {
		t.Fatal(err)
	}
	hintID, err := store.Hint.NewID()
	if err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"message":      store.Message.Put(c, topic, m, "1h"),
		"replica":      store.Message.PutReplica(c, topic, m, 0),
		"subscription": store.Subscription.Put(c, subID, topic, m),
		"hint":         store.PutRetireHintForTest(node, hintID, marker+"-seen", c, topic),
		"seen":         store.Seen.Put(marker+"-seen", 0),
		"session":      store.Session.Put(rowKey, store.SessionRowForTest(block, rowKey, c)),
	} {
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	store.Log.Apply(store.LogOp{Key: logKey, Raw: m})
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestRetireContractPurge(t *testing.T) {
	for _, seal := range []bool{false, true} {
		t.Run(fmt.Sprintf("seal=%t", seal), func(t *testing.T) {
			newStore(t, 0, ring(0), seal)
			store.SetHintNodes(func() []string { return []string{"node-a"} })
			const node = "node-a"
			putRetirementData(t, retireA, fmt.Sprintf("alpha-%t", seal), "groups.retire.alpha", node, 0x4101, 1<<63|0x4101, 0x4101)
			putRetirementData(t, retireB, fmt.Sprintf("beta-%t", seal), "groups.retire.beta", node, 0x4202, 1<<63|0x4202, 0x4202)

			left := store.ScanRetireForTest(retireA, []string{node})
			if left.Empty() {
				t.Fatal("control: the scan finds nothing of the contract before its retirement")
			}
			if left.SessionRows != 1 || left.SessionLogs != 1 {
				t.Errorf("control: %d session rows, %d logs, want 1 each", left.SessionRows, left.SessionLogs)
			}
			if left.Hints != 1 || left.Subscriptions == 0 || left.Messages == 0 {
				t.Errorf("control: leftovers before purge: %+v", left)
			}

			purged, err := store.RetireForTest(retireA, []string{node})
			if err != nil {
				t.Fatal(err)
			}
			if !purged.Empty() {
				t.Fatalf("resources left after the purge: %+v", purged)
			}
			if left := store.ScanRetireForTest(retireA, []string{node}); !left.Empty() {
				t.Fatalf("the scan after the purge still finds: %+v", left)
			}
			// Idempotent: a resumed retirement removes nothing and finds nothing.
			again, err := store.RetireForTest(retireA, []string{node})
			if err != nil || !again.Empty() {
				t.Fatalf("the second purge: %+v %v", again, err)
			}

			// The other contract is untouched.
			leftB := store.ScanRetireForTest(retireB, []string{node})
			if leftB.Messages == 0 || leftB.Replicas == 0 || leftB.Subscriptions == 0 ||
				leftB.Hints == 0 || leftB.SessionRows != 1 || leftB.SessionLogs != 1 {
				t.Errorf("another contract was changed: %+v", leftB)
			}

			// Progress persists and reads back.
			if err := store.SaveRetireProgress(store.RetireProgress{Contract: retireA, Generation: 123, Phase: 3, Blocks: []uint32{0x4101}}); err != nil {
				t.Fatalf("save progress: %v", err)
			}
			progress, err := store.RetireProgressAll()
			if err != nil {
				t.Fatal(err)
			}
			var found bool
			for _, p := range progress {
				if p.Contract == retireA && p.Generation == 123 && p.Phase == 3 && len(p.Blocks) == 1 && p.Blocks[0] == 0x4101 {
					found = true
				}
			}
			if !found {
				t.Errorf("progress did not read back: %+v", progress)
			}
		})
	}
}

// TestRetireContractSurvivesRestart checks that the purge is durable: after
// the store reopens, nothing of the retired contract is indexed or read.
func TestRetireContractSurvivesRestart(t *testing.T) {
	dir := newStore(t, 0, ring(0), false)
	store.SetHintNodes(func() []string { return []string{"node-a"} })
	const node = "node-a"
	putRetirementData(t, retireA, "alpha-restart", "groups.retire.restart", node, 0x4303, 1<<63|0x4303, 0x4303)
	if _, err := store.RetireForTest(retireA, []string{node}); err != nil {
		t.Fatal(err)
	}
	closeStore(t)
	openStore(t, dir)
	store.SetHintNodes(func() []string { return []string{"node-a"} })
	if left := store.ScanRetireForTest(retireA, []string{node}); !left.Empty() {
		t.Fatalf("the retired contract's resources survived a restart: %+v", left)
	}
}
