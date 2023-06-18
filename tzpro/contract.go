// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tzpro

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"blockwatch.cc/tzgo/micheline"
	"blockwatch.cc/tzgo/tezos"
)

type Contract struct {
	RowId         uint64               `json:"row_id,omitempty"`
	AccountId     uint64               `json:"account_id,omitempty"`
	Address       tezos.Address        `json:"address"`
	CreatorId     uint64               `json:"creator_id,omitempty"`
	Creator       tezos.Address        `json:"creator"`
	BakerId       uint64               `json:"baker_id,omitempty"  tzpro:"-"`
	Baker         tezos.Address        `json:"baker"               tzpro:"-"`
	FirstSeen     int64                `json:"first_seen"`
	LastSeen      int64                `json:"last_seen"`
	FirstSeenTime time.Time            `json:"first_seen_time"`
	LastSeenTime  time.Time            `json:"last_seen_time"`
	StorageSize   int64                `json:"storage_size"`
	StoragePaid   int64                `json:"storage_paid"`
	TotalFeesUsed float64              `json:"total_fees_used"     tzpro:"-"`
	Script        *micheline.Script    `json:"script,omitempty"    tzpro:"hex"`
	Storage       *micheline.Prim      `json:"storage,omitempty"   tzpro:"hex"`
	InterfaceHash string               `json:"iface_hash"`
	CodeHash      string               `json:"code_hash"`
	StorageHash   string               `json:"storage_hash"`
	Features      StringList           `json:"features"`
	Interfaces    StringList           `json:"interfaces"`
	CallStats     map[string]int       `json:"call_stats"          tzpro:"-"`
	NCallsIn      int                  `json:"n_calls_in"          tzpro:"-"`
	NCallsOut     int                  `json:"n_calls_out"         tzpro:"-"`
	NCallsFailed  int                  `json:"n_calls_failed"      tzpro:"-"`
	Bigmaps       map[string]int64     `json:"bigmaps,omitempty"   tzpro:"-"`
	Metadata      map[string]*Metadata `json:"metadata,omitempty"  tzpro:"-"`
}

func ParseU64(s string) (u uint64) {
	buf, _ := hex.DecodeString(s)
	u = binary.BigEndian.Uint64(buf[:8])
	return
}

func (c *Contract) Meta() *Metadata {
	m, ok := c.Metadata[c.Address.String()]
	if !ok {
		m = NewMetadata(c.Address)
		if c.Metadata == nil {
			c.Metadata = make(map[string]*Metadata)
		}
		c.Metadata[c.Address.String()] = m
	}
	return m
}

type ContractList struct {
	Rows    []*Contract
	columns []string
}

func (l ContractList) Len() int {
	return len(l.Rows)
}

func (l ContractList) Cursor() uint64 {
	if len(l.Rows) == 0 {
		return 0
	}
	return l.Rows[len(l.Rows)-1].RowId
}

func (l *ContractList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, null) {
		return nil
	}
	if data[0] != '[' {
		return fmt.Errorf("ContractList: expected JSON array")
	}
	return DecodeSlice(data, l.columns, &l.Rows)
}

type ContractMeta struct {
	Address string    `json:"contract"`
	Time    time.Time `json:"time"`
	Height  int64     `json:"height"`
	Block   string    `json:"block"`
}

type ContractParameters struct {
	ContractValue                 // contract
	Entrypoint    string          `json:"entrypoint"`           // contract
	L2Address     *tezos.Address  `json:"l2_address,omitempty"` // rollup
	Method        string          `json:"method,omitempty"`     // rollup
	Arguments     json.RawMessage `json:"arguments,omitempty"`  // rollup
}

type ContractScript struct {
	Script          *micheline.Script         `json:"script,omitempty"`
	StorageType     micheline.Typedef         `json:"storage_type"`
	Entrypoints     micheline.Entrypoints     `json:"entrypoints"`
	Views           micheline.Views           `json:"views,omitempty"`
	BigmapNames     map[string]int64          `json:"bigmaps,omitempty"`
	BigmapTypes     map[string]micheline.Type `json:"bigmap_types,omitempty"`
	BigmapTypesById map[int64]micheline.Type  `json:"-"`
}

func (s ContractScript) Types() (param, store micheline.Type, eps micheline.Entrypoints, bigmaps map[int64]micheline.Type) {
	param = s.Script.ParamType()
	store = s.Script.StorageType()
	eps, _ = s.Script.Entrypoints(true)
	bigmaps = s.BigmapTypesById
	return
}

type ContractValue struct {
	Value any             `json:"value,omitempty"`
	Prim  *micheline.Prim `json:"prim,omitempty"`
}

func (v ContractValue) IsPrim() bool {
	if v.Value == nil {
		return false
	}
	if m, ok := v.Value.(map[string]any); !ok {
		return false
	} else {
		_, ok := m["prim"]
		return ok
	}
}

func (v ContractValue) AsPrim() (micheline.Prim, bool) {
	if v.Prim.IsValid() {
		return *v.Prim, true
	}

	if v.IsPrim() {
		buf, _ := json.Marshal(v.Value)
		p := micheline.Prim{}
		err := p.UnmarshalJSON(buf)
		return p, err == nil
	}

	return micheline.InvalidPrim, false
}

func (v ContractValue) Has(path string) bool {
	return hasPath(v.Value, path)
}

func (v ContractValue) GetString(path string) (string, bool) {
	return getPathString(v.Value, path)
}

func (v ContractValue) GetInt64(path string) (int64, bool) {
	return getPathInt64(v.Value, path)
}

func (v ContractValue) GetBig(path string) (*big.Int, bool) {
	return getPathBig(v.Value, path)
}

func (v ContractValue) GetZ(path string) (tezos.Z, bool) {
	return getPathZ(v.Value, path)
}

func (v ContractValue) GetTime(path string) (time.Time, bool) {
	return getPathTime(v.Value, path)
}

func (v ContractValue) GetAddress(path string) (tezos.Address, bool) {
	return getPathAddress(v.Value, path)
}

func (v ContractValue) GetValue(path string) (interface{}, bool) {
	return getPathValue(v.Value, path)
}

func (v ContractValue) Walk(path string, fn ValueWalkerFunc) error {
	val := v.Value
	if len(path) > 0 {
		var ok bool
		val, ok = getPathValue(val, path)
		if !ok {
			return nil
		}
	}
	return walkValueMap(path, val, fn)
}

func (v ContractValue) Unmarshal(val interface{}) error {
	buf, _ := json.Marshal(v.Value)
	return json.Unmarshal(buf, val)
}

type ContractParams = Params[Contract]

func NewContractParams() ContractParams {
	return ContractParams{
		Query: make(map[string][]string),
	}
}

type ContractQuery struct {
	tableQuery
}

func (c *Client) NewContractQuery() ContractQuery {
	return ContractQuery{c.newTableQuery("contract", &Contract{})}
}

func (q ContractQuery) Run(ctx context.Context) (*ContractList, error) {
	result := &ContractList{
		columns: q.Columns,
	}
	if err := q.client.QueryTable(ctx, &q.tableQuery, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) GetContract(ctx context.Context, addr tezos.Address, params ContractParams) (*Contract, error) {
	cc := &Contract{}
	u := params.WithPath(fmt.Sprintf("/explorer/contract/%s", addr)).Url()
	if err := c.get(ctx, u, nil, cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func (c *Client) GetContractScript(ctx context.Context, addr tezos.Address, params ContractParams) (*ContractScript, error) {
	cc := &ContractScript{}
	u := params.WithPath(fmt.Sprintf("/explorer/contract/%s/script", addr)).Url()
	if err := c.get(ctx, u, nil, cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func (c *Client) GetContractStorage(ctx context.Context, addr tezos.Address, params ContractParams) (*ContractValue, error) {
	cc := &ContractValue{}
	u := params.WithPath(fmt.Sprintf("/explorer/contract/%s/storage", addr)).Url()
	if err := c.get(ctx, u, nil, cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func (c *Client) ListContractCalls(ctx context.Context, addr tezos.Address, params ContractParams) ([]*Op, error) {
	calls := make([]*Op, 0)
	u := params.WithPath(fmt.Sprintf("/explorer/contract/%s/calls", addr)).Url()
	if err := c.get(ctx, u, nil, &calls); err != nil {
		return nil, err
	}
	return calls, nil
}

func (c *Client) loadCachedContractScript(ctx context.Context, addr tezos.Address) (*ContractScript, error) {
	if c.cache != nil {
		if script, ok := c.cache.Get(addr.String()); ok {
			return script.(*ContractScript), nil
		}
	}
	c.log.Tracef("Loading contract %s", addr)
	script, err := c.GetContractScript(ctx, addr, NewContractParams().WithPrim())
	if err != nil {
		return nil, err
	}
	// strip code
	script.Script.Code.Code = micheline.Prim{}
	script.Script.Code.View = micheline.Prim{}
	// fill bigmap type info
	script.BigmapNames = script.Script.Bigmaps()
	script.BigmapTypes = script.Script.BigmapTypes()
	script.BigmapTypesById = make(map[int64]micheline.Type)
	for n, v := range script.BigmapTypes {
		id := script.BigmapNames[n]
		script.BigmapTypesById[id] = v
	}
	if c.cache != nil {
		c.cache.Add(addr.String(), script)
	}
	return script, nil
}

func (c *Client) AddCachedScript(addr tezos.Address, script *micheline.Script) {
	if !addr.IsValid() || script == nil || c.cache == nil {
		return
	}
	eps, _ := script.Entrypoints(true)
	views, _ := script.Views(true, false)
	s := &ContractScript{
		Script:          script,
		StorageType:     script.StorageType().Typedef(""),
		Entrypoints:     eps,
		Views:           views,
		BigmapNames:     script.Bigmaps(),
		BigmapTypes:     script.BigmapTypes(),
		BigmapTypesById: make(map[int64]micheline.Type),
	}
	for n, v := range s.BigmapTypes {
		id := s.BigmapNames[n]
		s.BigmapTypesById[id] = v
	}
	c.cache.Add(addr.String(), s)
}
