package internal

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/utp"
)

func mintPrimaryText(contract uint32) string {
	id, err := uid.MintClientID(contract, false)
	if err != nil {
		panic(err)
	}
	text, err := Globals.Service.keys.SealClientID(id, 0)
	if err != nil {
		panic(err)
	}
	return text
}

func mintSecondaryText(contract uint32) string {
	id, err := uid.NewClientID(1)
	if err != nil {
		panic(err)
	}
	id.SetContract(contract)
	id.SetPermissions(0)
	text, err := Globals.Service.keys.SealClientID(id, 0)
	if err != nil {
		panic(err)
	}
	return text
}

// retireAnswer sends unitdb/retire on c and decodes the answer.
func retireAnswer(t *testing.T, c *testClient, confirm bool) types.RetireResponse {
	t.Helper()
	answer := c.special("retire", types.RetireRequest{Confirm: confirm})
	var resp types.RetireResponse
	if err := json.Unmarshal(answer, &resp); err != nil {
		t.Fatalf("retire answer %q: %v", answer, err)
	}
	return resp
}

// TestRetireFlow drives a standalone retirement through its phases: the
// primary opens one generation, the driver completes an empty purge, the
// contract is barred on every path, the primary keeps only the retire entry,
// a repeated request is the same generation, and another contract serves.
func TestRetireFlow(t *testing.T) {
	contract := uint32(0x6e5d0e11)
	other := uint32(0x6e5d0e12)
	topic := "groups.retire.unit.alpha"

	// Before: the contract serves.
	admin := connectedClient(t, mintPrimaryText(contract), true)
	if k := admin.keygen(topic, "rw"); k == "" {
		t.Fatal("control: no key")
	}

	// Open the retirement.
	first := retireAnswer(t, admin, true)
	if first.Status != 200 || first.Generation == 0 || first.Done {
		t.Fatalf("first answer: %+v", first)
	}
	gen := first.Generation
	admin.conn.Close()

	// The driver runs through the phases on its own ticks; the admin
	// connection is closed in the drain, so reconnect to poll.
	deadline := time.Now().Add(10 * time.Second)
	var done types.RetireResponse
	for {
		c := connectedClient(t, mintPrimaryText(contract), true)
		done = retireAnswer(t, c, false)
		c.conn.Close()
		if done.Done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retirement did not complete: %+v", done)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if done.Generation != gen {
		t.Errorf("generation changed: %d then %d", gen, done.Generation)
	}
	if len(done.Nodes) != 1 || done.Nodes[0].Phase != 3 || len(done.Nodes[0].Remaining) != 0 {
		t.Errorf("completion: %+v", done)
	}

	// A repeated open is the same retirement, and key generation is
	// refused on the retired primary's connection.
	c := connectedClient(t, mintPrimaryText(contract), true)
	if again := retireAnswer(t, c, true); again.Generation != gen || !again.Done {
		t.Errorf("repeated retire: %+v", again)
	}
	// Key generation is refused with the retirement error rather than
	// answered.
	c.send(&utp.Publish{MessageID: 6, Messages: []*utp.PublishMessage{{Topic: "unitdb/keygen", Payload: []byte(`[{"topic":"` + topic + `","type":"rw"}]`)}}})
	if st := c.serverError(6); st.Status != types.ErrContractRetired.Status {
		t.Errorf("keygen on a retired contract: status %d", st.Status)
	}

	// Any other publish is refused too.
	c.send(&utp.Publish{MessageID: 7, Messages: []*utp.PublishMessage{{Topic: "unitdb/service", Payload: []byte("{}")}}})
	if st := c.serverError(7); st.Status != types.ErrContractRetired.Status {
		t.Errorf("a non-retire request on a retired admin connection: status %d", st.Status)
	}
	c.conn.Close()

	// A secondary id no longer connects.
	sec := dialTCP(t)
	if ack := sec.rawConnect(&utp.Connect{ClientID: mintSecondaryText(contract)}); ack.ReturnCode == 0 {
		t.Error("a secondary id of a retired contract connects")
	}

	// Another contract is untouched.
	otherAdmin := connectedClient(t, mintPrimaryText(other), true)
	if otherAdmin.keygen("groups.retire.unit.other", "rw") == "" {
		t.Error("another contract cannot issue keys")
	}
}

// TestRetirementMerge checks that retirement updates merge in any order to
// the same end state: fixed generation, earliest opening, highest phase per
// node, and done once set.
func TestRetirementMerge(t *testing.T) {
	g := retirementGeneration(42)
	updates := []Retirement{
		{Generation: g, At: 100, Nodes: map[string]int{"a": 1}},
		{Generation: g, At: 90, Nodes: map[string]int{"b": 1}},
		{Generation: g, Nodes: map[string]int{"a": 3, "b": 2}},
		{Generation: g, At: 110, Nodes: map[string]int{"c": 1}, Done: true},
		{Generation: g, Nodes: map[string]int{"b": 3}},
	}
	got := &Retirement{}
	for i := range updates {
		got.merge(&updates[i])
	}
	want := &Retirement{Generation: g, At: 90, Nodes: map[string]int{"a": 3, "b": 3, "c": 1}, Done: true}
	if got.Generation != want.Generation || got.At != want.At || got.Done != want.Done || len(got.Nodes) != 3 {
		t.Fatalf("merged %+v, want %+v", got, want)
	}
	for n, p := range want.Nodes {
		if got.Nodes[n] != p {
			t.Errorf("node %s phase %d, want %d", n, got.Nodes[n], p)
		}
	}
	// Merging again changes nothing.
	for i := range updates {
		got2 := *got
		if got2.merge(&updates[i]) {
			t.Fatalf("merging an update again changed %+v", got2)
		}
	}
}
