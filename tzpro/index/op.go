// Copyright (c) 2020-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"blockwatch.cc/tzpro-go/internal/client"
	"blockwatch.cc/tzpro-go/internal/util"
)

type OpAPI interface {
	Get(context.Context, OpHash, Query) (OpList, error)
	ResolveTypes(context.Context, ...*Op) error
	NewQuery() *OpQuery
}

func NewOpAPI(c *client.Client) OpAPI {
	return &opClient{client: c}
}

type opClient struct {
	client *client.Client
}

type Costs struct {
	Fee            float64 // the total fee paid
	Burn           float64 // total amount burned (not included in fee)
	GasUsed        int64   // gas used
	StorageUsed    int64   // new storage bytes allocated
	StorageBurn    float64 // burned for allocating new storage (not included in fee)
	AllocationBurn float64 // burned for allocating a new account (not included in fee)
}

func (x Costs) Add(y Costs) Costs {
	x.Fee += y.Fee
	x.Burn += y.Burn
	x.GasUsed += y.GasUsed
	x.StorageUsed += y.StorageUsed
	x.StorageBurn += y.StorageBurn
	x.AllocationBurn += y.AllocationBurn
	return x
}

func (x Costs) Sum() float64 {
	return x.Fee + x.Burn
}

type Op struct {
	Id           uint64          `json:"id"`
	Type         OpType          `json:"type"`
	Hash         OpHash          `json:"hash"`
	Height       int64           `json:"height"`
	Cycle        int64           `json:"cycle"`
	Timestamp    time.Time       `json:"time"`
	OpN          int             `json:"op_n"`
	OpP          int             `json:"op_p"`
	Status       OpStatus        `json:"status"`
	IsSuccess    bool            `json:"is_success"`
	IsContract   bool            `json:"is_contract"`
	IsInternal   bool            `json:"is_internal"`
	IsEvent      bool            `json:"is_event"`
	IsRollup     bool            `json:"is_rollup"`
	Counter      int64           `json:"counter"`
	GasLimit     int64           `json:"gas_limit"`
	GasUsed      int64           `json:"gas_used"`
	StorageLimit int64           `json:"storage_limit"`
	StoragePaid  int64           `json:"storage_paid"`
	Volume       float64         `json:"volume"`
	Fee          float64         `json:"fee"`
	Reward       float64         `json:"reward"`
	Deposit      float64         `json:"deposit"`
	Burned       float64         `json:"burned"`
	SenderId     uint64          `json:"sender_id"`
	ReceiverId   uint64          `json:"receiver_id"`
	CreatorId    uint64          `json:"creator_id"`
	BakerId      uint64          `json:"baker_id"`
	Data         json.RawMessage `json:"data,omitempty"`
	Parameters   json.RawMessage `json:"parameters,omitempty"`
	BigmapDiff   json.RawMessage `json:"big_map_diff,omitempty"` // transaction, origination
	StorageHash  util.HexBytes   `json:"storage_hash,omitempty"`
	CodeHash     util.HexBytes   `json:"code_hash,omitempty"`
	Errors       json.RawMessage `json:"errors,omitempty"`
	Sender       Address         `json:"sender"`
	Receiver     Address         `json:"receiver"`
	Creator      Address         `json:"creator"` // origination
	Baker        Address         `json:"baker"`   // delegation, origination
	Block        BlockHash       `json:"block"`
	Entrypoint   string          `json:"entrypoint,omitempty"`

	// explorer or ZMQ APIs only
	PrevBaker     Address             `json:"previous_baker"            tzpro:"-"` // delegation
	Source        Address             `json:"source"                    tzpro:"-"` // internal operations
	Offender      Address             `json:"offender"                  tzpro:"-"` // double_x
	Accuser       Address             `json:"accuser"                   tzpro:"-"` // double_x
	Loser         Address             `json:"loser"                     tzpro:"-"` // smart rollup refutation game
	Winner        Address             `json:"winner"                    tzpro:"-"` // smart rollup refutation game
	Staker        Address             `json:"staker"                    tzpro:"-"` // smart rollup refutation game
	Storage       json.RawMessage     `json:"storage,omitempty"         tzpro:"-"` // transaction, origination
	Script        *Script             `json:"script,omitempty"          tzpro:"-"` // origination
	Power         int                 `json:"power,omitempty"           tzpro:"-"` // endorsement
	Limit         *float64            `json:"limit,omitempty"           tzpro:"-"` // set deposits limit
	Confirmations int64               `json:"confirmations"             tzpro:"-"`
	NOps          int                 `json:"n_ops,omitempty"           tzpro:"-"`
	Batch         []*Op               `json:"batch,omitempty"           tzpro:"-"`
	Internal      []*Op               `json:"internal,omitempty"        tzpro:"-"`
	Metadata      map[string]Metadata `json:"metadata,omitempty"        tzpro:"-"`
	Events        []Event             `json:"events,omitempty"          tzpro:"-"`
	TicketUpdates []TicketUpdate      `json:"ticket_updates,omitempty"  tzpro:"-"`

	param   Type           // optional, may be decoded from script
	store   Type           // optional, may be decoded from script
	eps     Entrypoints    // optional, may be decoded from script
	bigmaps map[int64]Type // optional, may be decoded from script
}

func (o *Op) BlockId() BlockId {
	return BlockId{
		Height: o.Height,
		Hash:   o.Block.Clone(),
		Time:   o.Timestamp,
	}
}

func (o *Op) Content() []*Op {
	list := []*Op{o}
	if len(o.Batch) == 0 && len(o.Internal) == 0 {
		return list
	}
	if len(o.Batch) > 0 {
		list = list[:0]
		for _, v := range o.Batch {
			list = append(list, v)
			if len(v.Internal) > 0 {
				list = append(list, v.Internal...)
			}
		}
	}
	if len(o.Internal) > 0 {
		list = append(list, o.Internal...)
	}
	return list
}

func (o *Op) Addresses() *AddressSet {
	set := NewAddressSet()
	for _, op := range o.Content() {
		for _, v := range []Address{
			op.Sender,
			op.Receiver,
			op.Creator,
			op.Baker,
			op.PrevBaker,
			op.Source,
			op.Offender,
			op.Accuser,
		} {
			if v.IsValid() {
				set.AddUnique(v)
			}
		}
	}
	return set
}

func (o *Op) Cursor() uint64 {
	op := o
	if l := len(op.Batch); l > 0 {
		op = op.Batch[l-1]
	}
	if l := len(op.Internal); l > 0 {
		op = op.Internal[l-1]
	}
	return op.Id
}

func (o *Op) WithScript(s *ContractScript) *Op {
	if s != nil {
		o.param, o.store, o.eps, o.bigmaps = s.Types()
	} else {
		o.param, o.store, o.eps, o.bigmaps = Type{}, Type{}, nil, nil
	}
	return o
}

func (o *Op) WithTypes(param, store Type, eps Entrypoints, b map[int64]Type) *Op {
	o.param = param
	o.store = store
	o.eps = eps
	o.bigmaps = b
	return o
}

func (o Op) Costs() Costs {
	storageBurn := float64(o.StoragePaid) * 0.000250
	return Costs{
		Fee:            o.Fee,
		Burn:           o.Burned,
		GasUsed:        o.GasUsed,
		StorageUsed:    o.StoragePaid,
		StorageBurn:    storageBurn,
		AllocationBurn: o.Burned - storageBurn,
	}
}

func (o Op) HasParameters() bool {
	return len(o.Parameters) > 0
}

func (o Op) HasStorage() bool {
	return len(o.Storage) > 0
}

func (o Op) HasBigmapUpdates() bool {
	return len(o.BigmapDiff) > 0
}

func (o Op) DecodeParams(noFail bool, onError int) (*ContractParameters, error) {
	if o.Parameters == nil {
		return nil, ErrNoParams
	}
	switch o.Parameters[0] {
	case '"':
		buf, err := hex.DecodeString(string(o.Parameters[1 : len(o.Parameters)-1]))
		if err != nil && !noFail {
			return nil, err
		}
		params := &Parameters{}
		err = params.UnmarshalBinary(buf)
		if err != nil && !noFail {
			return nil, err
		}
		if o.param.IsValid() {
			ep, prim, _ := params.MapEntrypoint(o.param)
			cp := &ContractParameters{
				Entrypoint: ep.Name,
			}
			cp.Prim = &prim
			typ := ep.Type()
			typ.Prim.Anno = nil // strip entrypoint name annot
			val := NewValue(typ, prim)
			val.Render = onError
			cp.ContractValue.Value, err = val.Map()
			if err != nil && !noFail {
				return nil, fmt.Errorf("op %s (%d) decoding params %s: %v", o.Hash, o.Id, string(o.Parameters), err)
			}
			return cp, nil
		} else {
			return nil, ErrNoType
		}
	case '{':
		cp := &ContractParameters{}
		err := json.Unmarshal(o.Parameters, cp)
		return cp, err
	}
	return nil, ErrNoParams
}

func (o Op) DecodeStoragePrim(noFail bool) (prim Prim, err error) {
	if o.Storage == nil {
		err = ErrNoStorage
		return
	}
	switch o.Storage[0] {
	case '"':
		var buf []byte
		buf, err = hex.DecodeString(string(o.Storage[1 : len(o.Storage)-1]))
		if err != nil && !noFail {
			return
		}
		err = prim.UnmarshalBinary(buf)
		if err != nil && !noFail {
			return
		}
		err = nil
		return
	default:
		cv := &ContractValue{}
		err := json.Unmarshal(o.Storage, cv)
		return *cv.Prim, err
	}
}

func (o Op) DecodeStorage(noFail bool, onError int) (*ContractValue, error) {
	if o.Storage == nil {
		return nil, ErrNoStorage
	}
	switch o.Storage[0] {
	case '"':
		buf, err := hex.DecodeString(string(o.Storage[1 : len(o.Storage)-1]))
		if err != nil && !noFail {
			return nil, err
		}
		var prim Prim
		err = prim.UnmarshalBinary(buf)
		if err != nil && !noFail {
			return nil, err
		}
		cv := &ContractValue{
			Prim: &prim,
		}
		if o.store.IsValid() {
			val := NewValue(o.store, prim)
			val.Render = onError
			cv.Value, err = val.Map()
			if err != nil && !noFail {
				return nil, fmt.Errorf("op %s (%d) decoding storage %s: %v", o.Hash, o.Id, string(o.Storage), err)
			}
		} else {
			return nil, ErrNoType
		}
		return cv, nil
	default:
		cv := &ContractValue{}
		err := json.Unmarshal(o.Storage, cv)
		return cv, err
	}
}

func (o Op) DecodeBigmapEvents(noFail bool) (BigmapEvents, error) {
	if o.BigmapDiff == nil {
		return nil, ErrNoBigmapDiff
	}
	switch o.BigmapDiff[0] {
	case '"':
		// hex encoded low-level events
		buf, err := hex.DecodeString(string(o.BigmapDiff[1 : len(o.BigmapDiff)-1]))
		if err != nil && !noFail {
			return nil, err
		}
		events := make(BigmapEvents, 0)
		err = events.UnmarshalBinary(buf)
		if err != nil && !noFail {
			return nil, err
		}
		return events, nil
	default:
		// json encoded high-level updates
		updates, err := o.DecodeBigmapUpdates(true, noFail, 0)
		if err != nil {
			return nil, err
		}
		return updates.Events(), nil
	}
}

func (o Op) DecodeBigmapUpdates(withPrim, noFail bool, onError int) (BigmapUpdateList, error) {
	if o.BigmapDiff == nil {
		return nil, ErrNoBigmapDiff
	}
	switch o.BigmapDiff[0] {
	case '"':
		events, err := o.DecodeBigmapEvents(noFail)
		if err != nil {
			return nil, err
		}
		updates := make(BigmapUpdateList, 0, len(events))
		if withPrim {
			// decode prim only
			for _, v := range events {
				upd := &BigmapUpdate{
					Action:   v.Action,
					BigmapId: v.Id,
				}
				switch v.Action {
				case DiffActionAlloc, DiffActionCopy:
					kt, vt := v.KeyType.Clone(), v.ValueType.Clone()
					upd.KeyTypePrim = &kt
					upd.ValueTypePrim = &vt
				case DiffActionUpdate:
					key, val := v.Key.Clone(), v.Value.Clone()
					upd.KeyPrim, upd.ValuePrim = &key, &val
				case DiffActionRemove:
					key := v.Key.Clone()
					upd.KeyPrim = &key
				}
				updates = append(updates, upd)
			}
		} else {
			// full key/value unpack, requires script type
			for _, v := range events {
				var ktyp, vtyp Type
				if typ, ok := o.bigmaps[v.Id]; ok {
					ktyp, vtyp = typ.Left(), typ.Right()
				} else {
					ktyp = v.Key.BuildType()
				}
				upd := &BigmapUpdate{
					Action:   v.Action,
					BigmapId: v.Id,
				}
				switch v.Action {
				case DiffActionAlloc, DiffActionCopy:
					// alloc/copy only
					upd.KeyType = Type{Prim: v.KeyType}.TypedefPtr("@key")
					upd.ValueType = Type{Prim: v.ValueType}.TypedefPtr("@value")
					upd.SourceId = v.SourceId
					upd.DestId = v.DestId
				default:
					// update/remove only
					if !v.Key.IsEmptyBigmap() {
						keybuf, _ := v.GetKey(ktyp).MarshalJSON()
						mk := MultiKey{}
						_ = mk.UnmarshalJSON(keybuf)
						upd.Key = mk
						upd.Hash = v.KeyHash
					}
					upd.Meta = &BigmapMeta{
						Contract:     o.Receiver,
						BigmapId:     v.Id,
						UpdateTime:   o.Timestamp,
						UpdateHeight: o.Height,
					}
					if v.Action == DiffActionUpdate {
						// unpack value if type is known
						if vtyp.IsValid() {
							val := NewValue(vtyp, v.Value)
							val.Render = onError
							upd.Value, err = val.Map()
							if err != nil && !noFail {
								return nil, fmt.Errorf("op %s (%d) decoding bigmap %d/%s: %v", o.Hash, o.Id, v.Id, v.KeyHash, err)
							}
						}
					}
				}
				updates = append(updates, upd)
			}
		}
		return updates, nil
	default:
		// json encoded high-level updates
		var bmu BigmapUpdateList
		err := json.Unmarshal(o.BigmapDiff, &bmu)
		return bmu, err
	}
}

func (og OpList) Costs() Costs {
	var c Costs
	for _, v := range og {
		c = c.Add(v.Costs())
	}
	return c
}

type OpList []*Op

func (l OpList) Len() int {
	return len(l)
}

func (l OpList) Cursor() uint64 {
	if len(l) == 0 {
		return 0
	}
	return l[len(l)-1].Id
}

func (c opClient) ResolveTypes(ctx context.Context, ops ...*Op) error {
	for _, op := range ops {
		if !op.IsContract || !op.Receiver.IsContract() {
			continue
		}
		// load contract type info (required for decoding storage/param data)
		script, err := c.loadScript(ctx, op.Receiver)
		if err != nil {
			return err
		}
		op.WithScript(script)
	}
	return nil
}

type OpQuery = client.TableQuery[*Op]

func (c opClient) NewQuery() *OpQuery {
	return client.NewTableQuery[*Op](c.client, "op")
}

func (c opClient) Get(ctx context.Context, hash OpHash, params Query) (OpList, error) {
	o := make(OpList, 0)
	u := params.WithPath(fmt.Sprintf("/explorer/op/%s", hash)).Url()
	if err := c.client.Get(ctx, u, nil, &o); err != nil {
		return nil, err
	}
	return o, nil
}
