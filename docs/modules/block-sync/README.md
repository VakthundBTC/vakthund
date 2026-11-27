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

`block-sync` is a loop around state machines: the IBD machine, one peer machine per connected peer, and the reorg
machine ([state machines](state-machines.md)). It starts in IBD when the tip is the genesis block or below
`minimum_chain_work`, and in standard mode otherwise; IBD ends in standard mode, and a reorg runs inside standard mode
([modes](state-machines.md#modes)).

### State

It only uses in-memory storage: the mode, the machines' states (candidate headers, requests in flight, the IBD
machine's peer failures) and the [invalid blocks](state-machines.md#invalid-blocks).

### Communication

The module takes peer messages from its [queue](state-machines.md#queue), filled by `net`, and has two ticks: one for
[request timeouts](#request-timeout), one to learn the connected peers with `net.get_peers`
([peers](state-machines.md#peers)). The module may call directly both `net` and `chain` modules:

| Module  | Calls                                                                                                               |
|---------|---------------------------------------------------------------------------------------------------------------------|
| `net`   | `get_peers`, `send`                                                                                                 |
| `chain` | `validate_headers`, `store_headers`, `header`, `header_at`, `tip`, `store_block`, `connect_block`, `disconnect_tip` |

To obtain information from peers about the blockchain state, the module sends the following messages:

| Command      | Content                                              | Used in                                                            |
|--------------|------------------------------------------------------|--------------------------------------------------------------------|
| `getheaders` | protocol version, block locator, stop hash           | header download, last common ancestor search, inventory processing |
| `getdata`    | inventory items of type `MSG_WITNESS_BLOCK` (BIP144) | block download                                                     |

### Request timeout

Every `getheaders` and `getdata` that `block-sync` sends is a request with a deadline of `request_timeout_secs` seconds
after sending. A request is answered when a message that matches it by peer and content arrives
([routing](state-machines.md#routing)). A request that reaches its deadline unanswered, or a `getdata` answered with
`notfound`, has failed. Deadlines are checked on a tick every second, so a request fails up to a second after its
deadline. A failed request is sent again until its peer has failed `sync_peer_attempts` times in a row
([failed requests](state-machines.md#failed-requests)).

--- 

## Interface

`block-sync` exposes no calls to other modules. Its only input from other modules is its queue, which they reach
through a push-only interface: they can push a message, but cannot read the queue.

---

## Configuration

```toml
[block-sync]
request_timeout_secs = 60  # deadline of a getheaders or getdata request
sync_peer_attempts = 3     # consecutive failed requests after which a peer has failed
# minimum_chain_work = "0x..." # below it the node is in IBD; optional, the defaults per network are below
```

| Network    | Default `minimum_chain_work` (Bitcoin Core v32.1) |
|------------|---------------------------------------------------|
| `main`     | `0x145ec036acc5ba740052af1a0`                     |
| `testnet4` | `0xe346a558455ade8eca9`                           |
| `signet`   | `0x1090e9dc1520`                                  |
| `regtest`  | `0x0`                                             |
