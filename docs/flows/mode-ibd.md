# Bode: Initial Block Download

During IBD all other processes are paused, except maintaining the connections with peers.
Messages in the `block-sync` queue that are not part of the IBD are skipped. A message is part of the IBD when it
answers a request `block-sync` sent for the IBD.

The node enters IBD at start-up when the tip is the genesis block or the tip's chain work is below
`minimum_chain_work` ([configuration](../modules/block-sync/README.md#configuration)); otherwise it starts in standard
mode. So a node restarted during IBD continues IBD from its tip.

IBD ends when every block of the downloaded headers is connected. If the tip's chain work is then still below
`minimum_chain_work`, the sync peer was behind: it has failed, and IBD starts again with another
peer ([failed block request](../modules/block-sync/README.md#failed-block-request)). Otherwise `block-sync` switches to
standard mode.

## Downloading headers

1. `block-sync` selects one connected peer, the sync peer, using `net` as the CRUD for peers.
2. `block-sync` requests `getheaders` from it, in batches of 2000 headers. The first request carries a locator of the
   active chain; each next request carries the last header
   received ([block locator](../modules/block-sync/README.md#block-locator)).
3. `block-sync` continues until it has downloaded all headers this peer knows: an answer with fewer than 2000 headers.
4. `block-sync` validates the whole header chain by calling `chain.validate_headers`, and stores it with
   `chain.store_headers`.

If a `getheaders` to the sync peer fails ([request timeout](../modules/block-sync/README.md#request-timeout)),
`block-sync` selects another random connected peer as the sync peer and repeats the request with the same locator; the
download resumes from the last header received. When every connected peer has failed in turn, `block-sync` waits
`request_timeout_secs` seconds and starts again with the first.

```mermaid
sequenceDiagram
    autonumber
    participant BS as block-sync
    participant N as net
    participant P as sync peer
    participant C as chain
    BS ->> N: net.peers(status=connected)
    N -->> BS: connected peers
    Note over BS: select one peer
    loop until an answer has fewer than 2000 headers
        BS ->> N: net.send(peer, "getheaders", getheaders)
        N -) P: getheaders
        P -) N: headers
        N -) BS: queue: {peer, "headers", headers}
        Note over BS: if no answer within the request timeout —<br/> select another connected peer as the sync peer,<br/>repeat the request with the same locator
    end
    BS ->> C: chain.validate_headers(headers[..])
    C -->> BS: ok
    BS ->> C: chain.store_headers(headers[..])
    C -->> BS: ok
```

At this point the node has the ~full header history, validated on the header level only. The downloaded headers form the
candidate header chain.

---

## Downloading blocks

Blocks are downloaded from the beginning ([block download](../modules/block-sync/README.md#block-download)).

1. The current tip block is at height `tip`: all headers, and all blocks up to and including the tip block, are
   validated and stored.
2. `block-sync` knows the candidate headers `tip+1 ...`. Blocks which are already stored are not requested. For the
   others it sends `getdata` (`MSG_WITNESS_BLOCK`) to the sync peer. Requests are made in batches (TBD).
3. When a block is received, `block-sync` calls `chain.store_block` with it. If the block is rejected:
    - `rejected {mutated}`: the request has failed (step 5).
    - `rejected {consensus}`: `block-sync` reports a consensus failure and the node stops.
4. When the block `tip+1` is stored, `block-sync` calls `chain.connect_block` with its hash. If the block is valid,
   `chain` moves the tip to it and `block-sync` continues from step 1. Otherwise the block is invalid by consensus while
   its header is valid: `block-sync` reports a consensus failure and the node stops.
5. If a block request fails (request timeout, `notfound`, a mutated block, or the sync peer disconnects), the sync peer
   has failed ([failed block request](../modules/block-sync/README.md#failed-block-request)): `block-sync` selects
   another connected peer as the sync peer and starts IBD again from [downloading headers](#downloading-headers). The
   locator starts with the tip, so the new sync peer answers from the first block not connected yet; the headers after
   it are downloaded, validated and stored again.

```mermaid
sequenceDiagram
    autonumber
    participant BS as block-sync
    participant N as net
    participant P as sync peer
    participant C as chain
    BS ->> C: chain.tip()
    C -->> BS: tip
    Note over BS: knows candidate headers tip+1..tip+n<br/>skips the blocks with has_block
    BS ->> N: net.send(sync peer, "getdata", getdata[tip+1..tip+n])
    N -) P: getdata
    P -) N: block[tip+1..tip+n]
    N -) BS: queue: {peer, "block", block[tip+1..tip+n]}
    Note over BS: if no answer within the request timeout, or notfound —<br/>another sync peer, IBD starts again from the headers
    loop for each received block
        BS ->> C: chain.store_block(block)
        alt stored
            C -->> BS: stored
        else mutated
            C -->> BS: rejected {mutated, reason}
            Note over BS: another sync peer, IBD starts again from the headers
        else invalid
            C -->> BS: rejected {consensus, reason}
            Note over BS: consensus failure, the node stops
        end
    end
    loop for k in 1..n, in height order
        BS ->> C: chain.connect_block(hash of tip+k)
        alt Block valid and follows the tip
            Note over C: updates tip
            C -->> BS: new tip
        else rejected
            C -->> BS: rejected {reason}
            Note over BS: consensus failure, the node stops
        end
    end
```
