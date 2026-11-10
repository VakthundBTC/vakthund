.PHONY: fmt fmt-check

# ---- Formatting ----
# Every target in this section is a formatting command. After finishing work that
# changes code, run all of them in the order they appear; all of them must pass.

# Format all Rust code in the workspace.
fmt:
	cargo fmt --all

# Check formatting without modifying files (fails if anything is unformatted).
fmt-check:
	cargo fmt --all -- --check

# ---- End of formatting ----
