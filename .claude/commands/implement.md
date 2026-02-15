Implement a GitHub issue end-to-end: fetch the issue, plan, code with approval, commit incrementally, and generate a PR summary.

The argument is an issue number (e.g. `/implement 42`) or a full GitHub URL (e.g. `/implement https://github.com/armadaproject/armada-spark/issues/42`). If no argument is given, ask the user.

## Phase 1: Fetch issue

1. Parse the argument:
   - If it is a full URL like `https://github.com/{owner}/{repo}/issues/{number}`, extract `{owner}/{repo}` and `{number}`.
   - If it is just a number, use the current repo (run `gh repo view --json nameWithOwner -q .nameWithOwner` to get it).
2. Fetch issue details:
   ```
   gh issue view <number> --repo <owner/repo> --json title,body,labels,assignees,comments
   ```
3. Display a summary to the programmer:
   - Issue number and title
   - Labels (if any)
   - Body (truncated to ~40 lines if longer)
   - Number of comments and any noteworthy discussion points
4. Ask the programmer to confirm this is the right issue before proceeding. Use AskUserQuestion with options: "Proceed", "Show full issue body", "Cancel".

## Phase 2: Plan

1. Based on the issue description, explore the codebase to understand the relevant code paths. Use subagents to search for files, classes, and patterns referenced in or implied by the issue.
2. Create an implementation plan with numbered steps. Each step should be a logical, committable unit of work. The plan must include:
   - A 1-2 sentence summary of the issue
   - Numbered steps, where each step has a title, a description of what to change, and the affected file paths
3. Save the plan to `plans/implement-<issue-number>.md` using this format:
   ```
   ## Issue #<number>: <title>

   <1-2 sentence summary of what the issue asks for>

   ## Steps

   1. <step title>
      - <what to change and where>
      - Files: <path/to/file.scala>

   2. <step title>
      - <what to change and where>
      - Files: <path/to/file.scala, path/to/other.scala>
   ```
4. Show the full plan to the programmer for approval.
5. Ask the programmer using AskUserQuestion with options: "Approve plan", "Modify plan", "Cancel".
6. If the programmer wants modifications, iterate on the plan until approved.

## Phase 3: Execute step by step

For each step in the approved plan:

1. Announce the step: "Step <n>/<total>: <step title>"
2. Make the code changes for that step
3. If the step involves logic changes, run tests (`mvn test`) and show the result
4. Show the programmer a brief summary of what changed (key files and the nature of the change)
5. Ask the programmer using AskUserQuestion with options: "Commit this step", "Revise changes", "Skip this step", "Stop here".
6. If the programmer picks "Commit this step", run the /commit skill to commit the changes
7. If the programmer picks "Revise changes", iterate until they are satisfied
8. If the programmer picks "Skip this step", move to the next step without committing
9. If the programmer picks "Stop here", skip all remaining steps and jump to Phase 4

After each commit, briefly confirm the commit was made and move to the next step.

## Phase 4: Summary

1. After all steps are complete (or the programmer stops early):
   - Run the /summary skill to generate a PR description based on all commits made during this session
   - Show the summary to the programmer
2. Do NOT create a PR or push. Just present the summary for the programmer to use when they are ready.
3. If no commits were made (e.g. the programmer cancelled early), skip the summary and say so.

## Rules

- Never make code changes without programmer approval
- Always show what changed after each step before committing
- The programmer can modify, skip, reorder, or stop steps at any point
- Save the plan file before starting execution so the programmer has a reference
- Each commit should be a logical unit (one step = one commit, unless the step is trivial)
- If the issue is from a different repo (not the current one), fetch it via the full URL but make changes in the current repo
- No emojis in any output
- Do not create a PR or push to remote — only local commits and a summary
- If a step requires running tests, run them and show results before committing
- Use subagents for codebase exploration and code changes; keep the main flow focused on coordination and programmer interaction

$ARGUMENTS