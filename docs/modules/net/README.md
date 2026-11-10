# net

`net` is the module responsible for handling peers, and the CRUD for peers. It maintains the `peer-list` table: all
known peers and the features stored for each of them. It knows nothing about messages except `ping`/`pong`, the commands
of the connection flow, and which module must process each message.

## State

| State                                                       | Where             |
|-------------------------------------------------------------|-------------------|
| Known peers: address, features                              | table `peer-list` |
| Per known peer while the process runs: id, connection state | memory            |

For the `peer-list` table details see ([storage.md](../../architecture/storage.md#peer-list)). In memory, per known
peer:

| Field   | Description                                                                                                    |
|---------|----------------------------------------------------------------------------------------------------------------|
| `id`    | Peer identifier: the first 64 bits of the SHA-256 of the peer's `peer-list` key, shown as 16 hex digits.       |
| `state` | `known` (not connected), `connecting`, `connected`.                                                            |

The `id` depends only on the address, so a peer keeps it across restarts, and a removed peer that is added again gets
the same `id` back. Two addresses with the same `id` are an error.

---

## Peers

Known peers come from the configuration (`peers`) and from [`add_peer`](#add_peeraddress); both are stored in
`peer-list`. Known peers are trusted: `net` requires no services and no minimum protocol version from them. On start-up
`net` connects to every peer in `peer-list` and every configured peer; with `clear_peer_list = true` it first empties
`peer-list`, so only the configured peers remain.

After start-up, `net` connects to a peer only on [`connect`](#connectid). From then on `net` keeps the peer connected:
when the connection ends on its own (the [connection flow](#connection-flow) fails, the [pong](#pings) is missing, the
peer sends an [invalid message](#message-exchange) or closes the connection), the peer's state becomes `known` and `net`
starts the connection flow again after `reconnect_delay_secs`. This goes on until [`disconnect`](#disconnectid).
Adding a peer does not connect it; a removed configured peer comes back at the next
start-up. `net` does not accept inbound connections.

---

## Connection flow

```
Connect TCP -> Exchange features (version, verack) -> Peer connected -> sendheaders
```

1. `net` sets the peer's state to `connecting` and opens a TCP connection to the peer's address.
2. Features exchange: `net` sends `version` and the peer answers with its own `version`; each side acknowledges the
   other's `version` with `verack`. Our `version` advertises no services, `relay = false` (no transactions) and
   `start_height = 0` (`net` does not call `chain`). The peer's features recorded in `peer-list` are its services,
   protocol version and user agent.
3. After both `verack`s, `net` sets the peer's state to `connected`. Messages of other modules are sent only to
   `connected` peers, so they never reach a peer before the handshake is complete.
4. `net` sends `sendheaders`.

If the flow, the TCP connection included, does not complete within 60 seconds, or the TCP connection fails, `net`
disconnects, the peer's state becomes `known`, and `net` [reconnects](#peers) after `reconnect_delay_secs`.

The last step of the flow: `net` sends `sendheaders` (BIP130, empty payload). From then on the peer announces a new
block with a `headers` message carrying the block's header, instead of an `inv` carrying only its hash. A peer still
announces with `inv` when it has more than 8 blocks to announce or does not yet know which blocks the node has.

---

## Pings

Every 30 seconds `net` sends a `ping` with a random 64-bit nonce to each connected peer (BIP31). `net` responds to every
`ping` with a `pong` carrying the same nonce. Only a `pong` whose nonce matches the last `ping` counts as an answer. If no
matching `pong` arrives for 20 minutes (Bitcoin Core's `TIMEOUT_INTERVAL`), `net` disconnects the peer, its state
becomes `known`, and `net` [reconnects](#peers) after `reconnect_delay_secs`.

---

## Message exchange

Each message type (command) is bound to exactly one module. A message of that type is only sent by that module and only
delivered to that module. For example, `getblocktxn` can only be sent or received by `block-sync`. In v0.1 `block-sync`
is the only module besides `net` that exchanges messages with peers ([routing table](#routing-table)).

`net` reads only the message header of the v1 transport: network magic, command, payload length, checksum. It decodes
payloads only for `ping`, `pong` and the [connection flow](#connection-flow). Every other payload is
passed to its owner as raw bytes.

A message is invalid when the magic is wrong, the checksum does not match, or the payload length exceeds 4 000 000
bytes (Core's `MAX_PROTOCOL_MESSAGE_LENGTH`). A payload of `net`'s own commands that cannot be decoded is invalid as
well. On an invalid message `net` disconnects the peer, its state becomes `known`, and `net` [reconnects](#peers)
after `reconnect_delay_secs`.

### Requesting a peer

1. `block-sync` decides on the recipient peer.
2. `block-sync` forms a raw P2P message: command and encoded payload.
3. `block-sync` initiates the sending request to the peer (`send`).
4. `net` prepares the message (message header: magic, command, length, checksum) and sends it.

### Processing a request

1. `net` receives a message. It can also be a response to one of our previous requests.
2. `net` routes the message to the module that owns its command ([routing table](#routing-table)): it pushes
   `{peer, command, payload}` to the `block-sync` queue. If the queue is full, it is a node failure and the node stops.
3. `block-sync` initiates a request to respond to the peer, if a response is needed.

### Routing table

| Command                                                        | Owner        |
|----------------------------------------------------------------|--------------|
| `ping`, `pong`                                                 | `net`        |
| `version`, `verack`, `sendheaders`                             | `net`        |
| `headers`, `block`, `inv`, `notfound`, `getheaders`, `getdata` | `block-sync` |
| *Every other command*                                          | none in v0.1 |

---

## Interface

Direct calls. In v0.1 the only caller is `block-sync`. Every call except `get_peers` and `get_peer` fails for an unknown
peer id.

### get_peers(state?)

Returns known peers: `peer-list` rows with their in-memory fields

### get_peer(id)

Return the peer from `peer-list` with its in-memory fields, or not found

### add_peer(address)

Adds the peer to `peer-list` as `known`; does not connect to it. For an address that is already known, returns that
peer.

### remove_peer(id)

Removes the peer from `peer-list`. Fails for a peer that is `connecting` or `connected`, or that `net` is waiting to
reconnect to: it must be disconnected first.
Messages the peer sent before the removal may still be in the queue; if the peer is added again, they carry its `id` as
well.

### connect(id)

Starts the connection flow with the peer if it is `known`; does nothing if it is `connecting` or `connected`, or if
`net` is waiting to reconnect to it.

### disconnect(id)

Stops the connection with the peer and the reconnects to it; its state becomes `known` and it stays in `peer-list`.
Does nothing for a `known` peer that `net` is not reconnecting to.

### send(id, command, payload)

Sends the message to the peer if it is `connected`; fails otherwise. The answer, if any, is routed to the queue.

---

## Configuration

```toml
[net]
peers = []                 # peers to connect to: ["ip:port", "ip:port"]; host names are not resolved
clear_peer_list = false    # empty peer-list at start-up, so only the configured peers remain
reconnect_delay_secs = 30  # delay before reconnecting to a peer whose connection ended
```
