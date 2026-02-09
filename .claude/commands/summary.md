Generate a concise implementation summary for a PR description.

Steps:
1. Run `git diff master...HEAD --stat` and `git log master..HEAD --oneline` to understand all changes on this branch
2. Read the changed files to understand what was implemented and why
3. Write a summary suitable for a GitHub PR description

Summary format rules:
- Start with a one-line "What" statement explaining the change
- Follow with a "Why" section (2-3 sentences max) explaining the motivation
- List the key changes as plain bullet points (no nested bullets)
- If there are new tests, mention what they cover in one line
- End with a "How to verify" section with concrete steps if applicable
- Keep the total summary under 30 lines
- Use plain text with minimal markdown (no tables, no headers larger than ##, no code blocks unless showing a command)
- Do not repeat file paths or class names unnecessarily
- Focus on behavior changes, not implementation details
- Write in present tense, active voice

Do NOT:
- Use heavy markdown formatting (no bold, no tables, no badges)
- List every single file changed
- Include generic boilerplate like "This PR adds..."
- Add emojis
- Over-explain things that are obvious from the diff

Output the summary directly so the user can copy-paste it into the PR description.
