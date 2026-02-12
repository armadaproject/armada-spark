Create a git commit for the current staged/unstaged changes.

Rules:
1. Run `git status` and `git diff` to understand what changed
2. Stage relevant files (prefer specific files over `git add -A`)
3. Write a SHORT commit message (max 50 chars for subject line, imperative mood)
4. Use `--signoff` to sign off using the committer's git config (do NOT hardcode any name/email)
5. Do NOT add Co-Authored-By or any other trailers
6. If there are no changes, say so and stop

Commit format:
```
git commit --signoff -m "short imperative message"
```

The `--signoff` flag automatically uses the name and email from `git config user.name` and `git config user.email`, so each collaborator's own identity is used.

Examples of good messages:
- "add user and permission models"
- "implement executor allocation logic"
- "fix gang scheduling annotation prefix"
- "add table-driven tests for config parsing"

Do NOT:
- Use long descriptive messages
- Add Co-Authored-By trailers
- Use past tense ("added", "fixed")
- Prefix with type tags ("feat:", "fix:") unless asked
- Hardcode any author name or email
