# Vakthund

Vakthund is an alternative Bitcoin full node implementation written in Rust.

## Documentation is the source of truth

The documentation in [`docs/`](../docs/README.md) describes the general architecture of the node: modules, their
boundaries and interfaces, storage, and the flows (communication, IBD, standard mode, reorg). Start from
[`docs/README.md`](../docs/README.md) and follow its reading order.

Before implementing anything, the agent MUST:

1. Read the parts of `docs/` relevant to the change (at minimum `docs/architecture/overview.md` and the README of every
   module the change touches).
2. Check that the planned change complies with the documented architecture: module boundaries, which module owns which
   state and stores, allowed interactions (direct calls vs. queues and their directions), mode behaviour, and
   the scope of the version currently being implemented.
3. If the change is **not** compliant with the docs (or the docs are silent, ambiguous or contradictory on the point),
   stop and tell the operator before writing code: describe the conflict, quote or link the relevant section, and
   propose options (adjust the change, or update the docs). Do not silently diverge from the docs, and do not edit the
   docs to match an implementation without the operator's approval.
