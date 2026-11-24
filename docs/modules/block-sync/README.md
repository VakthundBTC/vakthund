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

During IBD all other processes are paused, except maintaining the connections with peers; messages in the queue that are
not part of the IBD are skipped. During reorg mode the [candidates](#candidates) keep receiving announcements and
downloading; only the decisions made against the tip wait until the reorg ends
([mode-reorg.md](../../flows/mode-reorg.md)).

At start-up `block-sync` enters IBD when the tip is the genesis block or its chain work is below `minimum_chain_work`,
and standard mode otherwise. IBD switches to standard mode once its blocks are connected and the tip's chain work has
reached `minimum_chain_work` ([mode-ibd.md](../../flows/mode-ibd.md)).

### State

It only uses in-memory storage for non-persistent peers: mode, candidate headers, requests in flight, new block
batches (until included into chain), etc.

### Communication

The module reads two queues: its queue, which `net` fills with routed messages and peer events, and its answer queue,
into which `chain` pushes the answers to its requests. It also has timers for [request timeouts](#request-timeout). It
calls `net` directly and sends requests to `chain` ([interface](../chain/README.md#interface)):

| Module  | Calls                                                                                                                          |
|---------|--------------------------------------------------------------------------------------------------------------------------------|
| `net`   | `get_peers`, `get_peer`, `send`, `disconnect`                                                                                  |
| `chain` | `validate_headers`, `store_headers`, `header`, `header_at`, `locator`, `tip`, `store_block`, `connect_block`, `disconnect_tip` |

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
download in [failed block request](#failed-block-request), the LCA search in
[standard mode](../../flows/mode-standard.md#processing-headers).

### Internal structure

`block-sync` is a shell around state machines. A machine holds a flow's logic and does no I/O: it takes an input with
a read-only context (the tip, `minimum_chain_work`, the connected peers) and returns effects: messages to send to peers,
requests to `chain`, blocks to fetch, a mode change. The shell carries the effects out: it sends, keeps every request
with its deadline in one registry, and routes each answer, failure and message to the machine that owns it.

| Machine                      | Exists                     | States                                                                                     |
|------------------------------|----------------------------|--------------------------------------------------------------------------------------------|
| IBD                          | in IBD                     | waiting for a peer, headers, validating, storing headers, blocks                           |
| Peer, one per connected peer | always; idle in IBD        | idle, validating, searching, finding the LCA, waiting for the tip, downloading, ready      |
| Reorg                        | in reorg mode              | disconnecting, connecting                                                                  |
| Block fetcher                | always                     | per block: checking `has_block`, requested from a peer, storing                            |

- Mode changes are effects: a finished IBD enters standard mode; a ready peer machine starts reorg mode; a finished
  reorg enters standard mode and tells every peer machine that the tip changed.
- The block fetcher keeps one request per block for every machine that needs it, and tells them when a block is
  stored or its request failed.
- An unsolicited message goes to the sender's peer machine in standard and reorg mode, and is dropped in IBD.

Machines never call each other. A machine that has something to tell others emits an internal event; the shell queues
it and delivers it, in order, before it reads the next event from `net`, `chain` or a deadline. So after every event
each machine's state agrees with the current tip and mode. Delivery is eager: a machine waiting for the tip continues
as soon as it changes, not on its peer's next announcement.

| Internal event       | Emitted by    | Delivered to                                     |
|----------------------|---------------|--------------------------------------------------|
| tip changed          | shell         | every peer machine                               |
| block stored         | block fetcher | the machines that asked for the block            |
| block request failed | block fetcher | the machines that asked for the block            |

The states, events and transitions of every machine: [machines.md](machines.md).

### Candidates

In standard mode a candidate is a chain announced by a peer that the node may extend its chain with or switch to. It
starts as the [LCA search](../../flows/mode-standard.md#processing-headers) with that peer, and continues as the block
download of `LCA + 1 ... H`: an [appended chain](../../flows/mode-standard.md#append-new-header) when the LCA is the
tip, an [alternative chain](../../flows/mode-reorg.md) when it is below the tip and H has more chain work than the tip.
A newer announcement from the same peer replaces its candidate.

Several candidates may download at the same time, usually the same chain announced by several peers; a block already
requested for one candidate is not requested again for another. The first candidate whose blocks are all stored is
connected through reorg mode, with k = 0 for an appended chain. The other candidates keep their headers and blocks:
when the reorg ends, each one finds its LCA and compares its chain work against the new tip again. One whose headers
are now all on the active chain, or whose chain has at most the new tip's chain work, is dropped; the others continue,
and their stored blocks are not downloaded again (`has_block`).

The decisions made against the tip are the LCA, the chain-work comparison and the start of a reorg. A candidate that
reaches one of them during reorg mode waits for the reorg to end; everything else — validating its headers, the LCA
search's `getheaders` loop, fetching and storing its blocks — goes on.

--- 

## Interface

`block-sync` exposes no calls to other modules. Its only inputs from other modules are its queue and its answer queue,
which they reach through a push-only interface: they can push an item, but cannot read the queue.

---

## Configuration

```toml
[block-sync]
request_timeout_secs = 60 # deadline of a getheaders or getdata request
minimum_chain_work = "0x…" # IBD while the tip has less chain work; 32-byte hex
```

The default `minimum_chain_work` is Bitcoin Core's `nMinimumChainWork` for the network (v32.1): main
`0x145ec036acc5ba740052af1a0`, testnet4 `0xe346a558455ade8eca9`, signet `0x1090e9dc1520`, regtest 0.

---

## Special mechanics

### Peer selection

| Request                              | Peer                                                                                                       |
|--------------------------------------|------------------------------------------------------------------------------------------------------------|
| Header download (IBD)                | one random connected peer: the sync peer                                                                   |
| Block download (IBD)                 | the sync peer                                                                                              |
| Block download (standard mode/reorg) | the peer whose [candidate](#candidates) needs the block, unless the block is already requested for another |
| Last common ancestor search          | the peer that sent the header                                                                              |

### Block download

Every block the node downloads goes through two `chain` calls, in IBD, standard mode and reorg alike:

1. Before requesting a block, `block-sync` calls `chain.header(hash)`. If `has_block` is set, the block is not
   requested: it is already stored.
2. On receipt, `block-sync` calls `chain.store_block(block)`. If the block is rejected:
    - `rejected {mutated}`: the request has failed ([failed block request](#failed-block-request)).
    - `rejected {consensus}`: `block-sync` reports a consensus failure and the node stops.
3. In height order, once the block `tip+1` is stored, `block-sync` calls `chain.connect_block(hash)`. `rejected`:
   `block-sync` reports a consensus failure and the node stops.

Blocks may be received and stored in any order; they are connected in height order.

### Failed block request

A block request fails on its [request timeout](#request-timeout), on a `notfound` naming the block, on
`rejected {mutated}`, or when the peer disconnects. `block-sync` asks no other peer for the block:

- In IBD the sync peer has failed. `block-sync` selects another connected peer as the sync peer and starts IBD again
  from the [header download](../../flows/mode-ibd.md#downloading-headers). Its locator starts with the tip, so the peer
  answers from the first block not connected yet. When every connected peer has failed in turn, `block-sync` waits
  `request_timeout_secs` seconds and starts again with the first.
- In standard mode, and in the block download of a reorg, the candidate of the peer that failed is dropped. Another
  candidate that needs the block requests it from its own peer. If none does, the block is requested again when a
  later announcement leads to it ([append new header](../../flows/mode-standard.md#append-new-header)).

Everything stored so far stays: the headers beyond the tip and the blocks stored with `chain.store_block`. Stored blocks
are not downloaded again (`has_block`). In a reorg nothing has been disconnected yet.
