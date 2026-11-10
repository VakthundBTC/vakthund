# block-sync

`block-sync` is the main initiator and processing center of the node:

- requests new headers and blocks;
- receives new headers and blocks;
- works with [chain](../chain/README.md) as the CRUD for headers and blocks;
- decides which blocks to download;
- decides which chain is active;
- decides whether the tip must be rolled back.

It selects peers using [net](../net/README.md) as the CRUD for peers.

### Modes

| Mode     | Flow                                             |
|----------|--------------------------------------------------|
| IBD      | [mode-ibd.md](../../flows/mode-ibd.md)           |
| Standard | [mode-standard.md](../../flows/mode-standard.md) |
| Reorg    | [mode-reorg.md](../../flows/mode-reorg.md)       |

During IBD and reorg, all other processes are paused, except maintaining the connections with peers. Messages in the
queue that are not part of the running IBD or reorg are skipped.

TBD (how we launch, how we switch from IBD to Standard)

### State

It only uses in-memory storage for non-persistent peers: mode, candidate headers, requests in flight, new block
batches (until included into chain), etc.

### Communication

The module accepts input messages from queue filled by `net`. It also has timers for
[request timeouts](#request-timeout). The module may call directly both `net` and `chain` modules:

| Module  | Calls                                                                                                               |
|---------|---------------------------------------------------------------------------------------------------------------------|
| `net`   | `get_peers`, `get_peer`, `send`, `disconnect`                                                                       |
| `chain` | `validate_headers`, `store_headers`, `header`, `header_at`, `tip`, `store_block`, `connect_block`, `disconnect_tip` |

To obtain information from peers about the blockchain state, the module sends the following messages:

| Command      | Content                                              | Used in                                                            |
|--------------|------------------------------------------------------|--------------------------------------------------------------------|
| `getheaders` | protocol version, block locator, stop hash           | header download, last common ancestor search, inventory processing |
| `getdata`    | inventory items of type `MSG_WITNESS_BLOCK` (BIP144) | block download                                                     |

### Request timeout

Every `getheaders` and `getdata` that `block-sync` sends is a request with a deadline of `request_timeout_secs` seconds
after sending. A request is answered when a message that matches it by peer and content arrives (`headers` whose first
header's parent is a block of the locator, or an empty `headers`, for `getheaders`; the `block` with the requested hash,
or a `notfound` naming it, for `getdata`). A request that reaches its deadline unanswered has failed. Each flow states
what a failed request means for it: header download in [IBD](../../flows/mode-ibd.md#downloading-headers), block
download in [peer selection](#peer-selection), the LCA search in
[standard mode](../../flows/mode-standard.md#processing-headers).

--- 

## Interface

`block-sync` exposes no calls to other modules. Its only input from other modules is its queue, which they reach
through a push-only interface: they can push a message, but cannot read the queue.

---

## Configuration

```toml
[block-sync]
request_timeout_secs = 60 # deadline of a getheaders or getdata request
```

---

## Special mechanics

### Peer selection

| Request                     | Peers eligible                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Header download (IBD)       | one random connected peer: the sync peer                                                                                                                                                                                                                                                                                                                                                                                                              |
| Block download              | first the peer the blocks were learned from: the sync peer in IBD, the peer that announced the header in standard mode and in a reorg. If that peer does not answer within the request timeout or answers `notfound`, random connected peers whose features include `NODE_NETWORK` and `NODE_WITNESS`, one after another, until a peer returns the block. When every eligible peer has failed for the block: [unavailable block](#unavailable-block). |
| Last common ancestor search | the peer that sent the header                                                                                                                                                                                                                                                                                                                                                                                                                         |

### Block download

Every block the node downloads goes through two `chain` calls, in IBD, standard mode and reorg alike:

1. Before requesting a block, `block-sync` calls `chain.header(hash)`. If `has_block` is set, the block is not
   requested: it is already stored.
2. On receipt, `block-sync` calls `chain.store_block(block)`. If the block is rejected:
    - `rejected {mutated}`: the block is requested from another eligible peer ([peer selection](#peer-selection)).
    - `rejected {consensus}`: `block-sync` reports a consensus failure and the node stops.
3. In height order, once the block `tip+1` is stored, `block-sync` calls `chain.connect_block(hash)`. `rejected`:
   `block-sync` reports a consensus failure and the node stops.

Blocks may be received and stored in any order; they are connected in height order.

### Unavailable block

A block is unavailable when every eligible peer has failed for it: request timeout or `notfound`. `block-sync` then:

- drops the request for it and the requests that depend on it: the blocks after it in the same download;
- keeps everything stored so far: the headers beyond the tip and the blocks already stored with `chain.store_block`;
- continues in standard mode on the current tip. In IBD this is the switch to standard mode. In a reorg the reorg is
  abandoned; nothing has been disconnected.

The blocks are requested again when a later announcement leads to them
([append new header](../../flows/mode-standard.md#append-new-header)); the blocks already stored are not downloaded
again (`has_block`).
