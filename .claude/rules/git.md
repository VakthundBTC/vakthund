# Rule: git operations require the operator's approval

The agent MUST NOT commit, push, or reset code (including `git commit`, `git push`, `git reset`, `git rebase`,
`git checkout -- <file>`/`git restore`, `git stash`, force operations, or anything else that rewrites history or discards
changes) without direct approval from the operator. Approval applies to the specific action it was given for; it does
not carry over to later actions.

The agent MAY, without asking:

- add files it created itself to the staging area (`git add <file>`);
- run any read-only command (e.g. `git status`, `git diff`, `git log`, `git show`, `git blame`, `cargo check`,
  `cargo test`, `cargo clippy`).

When the work is finished, the agent MAY propose a commit to the operator, including the exact commit message it
suggests. The commit is made only after the operator approves it.
