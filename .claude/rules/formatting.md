# Rule: code must be formatted before work is finished

The [`Makefile`](../../Makefile) in the repository root contains a section of targets marked as formatting commands
(between the `# ---- Formatting ----` and `# ---- End of formatting ----` comments). Read the `Makefile` to find out
which commands are currently in that section; do not assume a fixed list.

After finishing work that changes code, the agent MUST run every formatting command, in the order they appear in the
`Makefile`, and make sure all of them pass. The work is not finished, and no commit may be proposed, until they do. If
a command fails for a reason the agent cannot fix, the agent MUST report the failure and its output to the operator
instead of claiming the work is done.
