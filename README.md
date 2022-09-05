## TzPro-Go – Official Go SDK for the TzPro API

The official Blockwatch Go client library for TzPro. This SDK is free to use under a permissive license and works with the most recent version of the TzPro API v012-2022-02-22. API documentation can be found [here](https://docs.tzpro.io/api).

We will maintain this SDK on a regular basis to keep track of changes to the Tezos network and add new API features as they are released. Open-source support is provided through issues in this Github repository. If you are looking for commercial support, please contact us at licensing@blockwatch.cc.

This SDK is based on [TzGo](https://github.com/blockwatch-cc/tzgo), our open-source Go library for Tezos.

### TzPro-Go Versioning

As long as TzPro-Go is in beta status we will use major version 0.x. Once interfaces are stable we switch to 1.x. The minor version number expresses compatibility with a Tezos protocol release, e.g. v0.12.x supports the Ithaca API, v0.11.x supports Hangzhou.

Supported API and Tezos versions

- **v0.12**: API release v012-2022-02-22, Tezos Ithaca

Older API versions are no longer supported. Please use an older version tag.


### Installation

```sh
go get -u blockwatch.cc/tzpro-go
```

Then import, using

```go
import (
	"blockwatch.cc/tzpro-go"
)
```

### Initializing the TzPro SDK Client

All functions are exported through a `Client` object. For convenience we have defined two default clients `tzpro.DefaultClient` for mainnet and `tzpro.IpfsClient`for our IPFS gateway. You may construct custom clients for different API URLs like so:

```go
c, err := tzpro.NewClient("https://api.tzpro.io", nil)
```

The default configuration should work just fine, but if you need special timeouts, proxy or TLS settings you may use a custom `http.Client` as second argument.

```go
import (
	"crypto/tls"
	"log"
	"net"
	"net/http"

	"blockwatch.cc/tzpro-go"
)


func main() {
	hc := &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 180 * time.Second,
			}).Dial,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			}
		}
	}

	c, err := tzpro.NewClient("https://my-private-index.local:8000", hc)
	if err != nil {
		log.Fatalln(err)
	}
}
```

### Reading a single Tezos Account

```go
import (
  "context"
  "blockwatch.cc/tzpro-go"
  "blockwatch.cc/tzgo/tezos"
)

// use default Mainnet client
client := tzpro.DefaultClient
ctx := context.Background()
addr := tezos.MustParseAddress("tz3RDC3Jdn4j15J7bBHZd29EUee9gVB1CxD9")

// get account data and embed metadata if available
params := tzpro.NewAccountParams().WithMeta()
a, err := client.GetAccount(ctx, addr, params)
```

### Reading Smart Contract Storage

```go
import (
  "context"
  "blockwatch.cc/tzgo"
  "blockwatch.cc/tzpro-go"
)

// use default Mainnet client
client := tzpro.DefaultClient
ctx := context.Background()
addr := tezos.MustParseAddress("KT1Puc9St8wdNoGtLiD2WXaHbWU7styaxYhD")

// read storage from API
params := tzpro.NewContractParams()
raw, err := client.GetContractStorage(ctx, addr, params)

// access individual properties with correct type
tokenAddress, ok := raw.GetAddress("tokenAddress")
tokenPool, ok := raw.GetBig("tokenPool")
xtzPool, ok := raw.GetBig("xtzPool")
```

### Listing Account Transactions

```go
import (
  "context"
  "blockwatch.cc/tzpro-go"
  "blockwatch.cc/tzgo/tezos"
)

// use default Mainnet client
client := tzpro.DefaultClient
ctx := context.Background()
addr := tezos.MustParseAddress("tz1irJKkXS2DBWkU1NnmFQx1c1L7pbGg4yhk")

// list operations sent and received by this account
params := tzpro.NewOpParams().WithLimit(100).WithOrder(tzpro.OrderDesc)
ops, err := client.GetAccountOps(ctx, addr, params)
```

### Cursoring through results

The SDK has a convenient way for fetching results longer than the default maximum of 500 entries. We use the more efficient cursor method here, but offset would work similar. An empty result means there is no more data available right now. As the chain grows you can obtain fresh data by using the most recent row_id like shown below

```go
import (
  "context"
  "blockwatch.cc/tzpro-go"
  "blockwatch.cc/tzgo/tezos"
)

client := tzpro.DefaultClient
ctx := context.Background()
addr := tezos.MustParseAddress("tz1irJKkXS2DBWkU1NnmFQx1c1L7pbGg4yhk")
params := tzpro.NewOpParams()

for {
	// fetch next batch from the explorer API
	ops, err := client.GetAccountOps(ctx, addr, params)
	// handle error if necessary

	// stop when result is empty
	if len(ops) == 0 {
		break
	}

	// handle ops here

	// prepare for next iteration
	params = params.WithCursor(ops[len(ops)-1].RowId)
}
```

### Decoding smart contract data into Go types

```go
import (
  "context"
  "blockwatch.cc/tzgo"
  "blockwatch.cc/tzpro-go"
)

// use default Mainnet client
client := tzpro.DefaultClient
ctx := context.Background()
addr := tezos.MustParseAddress("KT1Puc9St8wdNoGtLiD2WXaHbWU7styaxYhD")

// read storage from API
raw, err := client.GetContractStorage(ctx, addr, tzpro.NewContractParams())

// decode into Go struct
type DexterStorage struct {
	Accounts                int64         `json:"accounts"`
	SelfIsUpdatingTokenPool bool          `json:"selfIsUpdatingTokenPool"`
	FreezeBaker             bool          `json:"freezeBaker"`
	LqtTotal                *big.Int      `json:"lqtTotal"`
	Manager                 tezos.Address `json:"manager"`
	TokenAddress            tezos.Address `json:"tokenAddress"`
	TokenPool               *big.Int      `json:"tokenPool"`
	XtzPool                 *big.Int      `json:"xtzPool"`
}

dexterPool := &DexterStorage{}
err := raw.Unmarshal(dexterPool)
```

### Listing bigmap key/value pairs with server-side data unfolding

```go
import (
  "context"
  "blockwatch.cc/tzpro-go"
)

type HicNFT struct {
	TokenId   int               `json:"token_id,string"`
	TokenInfo map[string]string `json:"token_info"`
}

client := tzpro.DefaultClient
ctx := context.Background()
params := tzpro.NewContractParams().
	WithUnpack().
	WithLimit(500)

for {
	// fetch next batch from the explorer API
	nfts, err := client.GetBigmapValues(ctx, 514, params)
	if err != nil {
		return err
	}

	// stop when result is empty
	if len(nfts) == 0 {
		break
	}
	for _, v := range nfts {
		var nft HicNFT
		if err := v.Unmarshal(&nft); err != nil {
			return err
		}
		// handle the value
	}
}
```

### Building complex Table Queries

The [TzPro Table API](https://tzpro.com/docs/api#table-endpoints) is the fastest way to ingest and process on-chain data in bulk. The SDK defines typed query objects for most tables and allows you to add filter conditions and other configuration to these queries.

```go
import (
  "context"
  "blockwatch.cc/tzpro-go"
)

client := tzpro.DefaultClient
ctx := context.Background()

// create a new query object
q := client.NewBigmapValueQuery()

// add filters and configure the query to list all active keys
q.WithFilter(tzpro.FilterModeEqual, "bigmap_id", 514).
    WithColumns("row_id", "key_hash", "key", "value").
    WithLimit(1000).
    WithOrder(tzpro.OrderDesc)

// execute the query
list, err := q.Run(ctx)

// walk rows
for _, row := range list.Rows {
	// process data here
}
```

### Listing many Bigmap keys with client-side data decoding

Extending the example above, we now use TzGo's Micheline features to decode annotated bigmap data into native Go structs. For efficiency reasons the API only sends binary (hex-encoded) content for smart contract storage. The SDK lets you decodes this into native Micheline primitives or native Go structs for further processing as shown in the example below.

```go
import (
  "context"
  "blockwatch.cc/tzpro-go"
)

type HicNFT struct {
	TokenId   int               `json:"token_id,string"`
	TokenInfo map[string]string `json:"token_info"`
}

client := tzpro.DefaultClient
ctx := context.Background()

// fetch bigmap info with prim (required for key/value types)
params := tzpro.NewContractParams().WithPrim()
info, err := client.GetBigmap(ctx, 514, params))
keyType := info.MakeKeyType()
valType := info.MakeValueType()

// create a new query object
q := client.NewBigmapValueQuery()

// add filters and configure the query to list all active keys
q.WithFilter(tzpro.FilterModeEqual, "bigmap_id", 514).
    WithColumns("row_id", "key_hash", "key", "value").
    WithLimit(1000).
    WithOrder(tzpro.OrderDesc)

// execute the query
list, err := q.Run(ctx)

// walk rows
for _, row := range list.Rows {
    // unpack to Micheline primitives and tag types (we ignore errors here)
    key, _ := row.DecodeKey(keyType)
    val, _ := row.DecodeValue(valType)

    // unpack into Go type
    var nft HicNFT
    err = val.Unmarshal(&nft)

    // or access individual named values
    i, ok := val.GetInt64("token_id")
}
```

### Gracefully handle rate-limits

To avoid excessive overload of our API we limit the rate at which we process your requests. This means your program may from time to time run into a rate limit. To let you gracefully handle retries by waiting until a rate limit resets, we expose the deadline and a done channel much like Go's network context does. Here's how you may use this feature:

```go
var acc *tzpro.Account
for {
	var err error
	acc, err = tzpro.GetAccount(ctx, tezos.MustParseAddress("tz1irJKkXS2DBWkU1NnmFQx1c1L7pbGg4yhk"))
	if err != nil {
		if e, ok := tzpro.IsRateLimited(err); ok {
			fmt.Printf("Rate limited, waiting for %s\n", e.Deadline())
			select {
			case <-ctx.Done():
				// wait until external context is canceled
				err = ctx.Err()
			case <-e.Done():
				// wait until rate limit reset and retry
				continue
			}
		}
	}
	break
}

// handle error and/or result here

```

## License

The MIT License (MIT) Copyright (c) 2021-2022 Blockwatch Data Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished
to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
