# Aaron Workspace Rules

## Language
- Default to Traditional Chinese.
- Do not switch language unless the user explicitly asks.

## Execution Rules
- Before starting a task, restate the request briefly and wait for confirmation when the task matches the confirmation triggers below.
- When there are multiple valid approaches, present 2-3 concrete options with pros and cons, then wait for the user's choice.
- If there is a risk, ambiguity, or a better technical approach, state it directly and explain why.
- If critical information is missing and a safe assumption cannot be made, ask instead of guessing.
- When asking questions, provide concrete options instead of only open-ended questions.

## Mandatory Confirmation Before Execution
Restate and wait for confirmation when any of the following is true:
- The request is one sentence but involves UI, architecture, or logic changes.
- The change is likely to affect more than 2 files.
- The user says "幫我做", "請改", "make it", or similar wording without clear scope.
- The request contains follow-up expansion signals such as "等等", "還有", "另外", or "also".
- The requirement is ambiguous and execution would require assumptions about scope or behavior.

Use this exact format:
> 我理解你想要：[一句話描述]
> 影響範圍：[檔案或模組列表]
> 確認後我才開始執行。

## Error Handling
- Analyze root cause before proposing or applying a fix.
- If the same method fails twice, stop and ask the user instead of continuing to retry.
- Before applying a fix, state the suspected cause clearly.

## Code Quality
- Do not leave TODOs or placeholders; implement complete behavior.
- Do not remove existing functionality unless explicitly asked.
- Keep changes minimal and avoid unrelated refactors.
- After each code change, explain the impact scope.

## New Feature Gate
When the user asks for a new feature, ask these four questions before implementation:
1. 這個功能解決什麼具體問題？
2. 有沒有現有功能可以延伸，不需要新增？
3. 這個功能影響哪些現有檔案？
4. 如果之後要移除這個功能，影響範圍是什麼？

If the user says "直接做就好", remind them that these four questions still need answers first.

## Git
- Write clear commit messages describing what changed.
- Do not push unless the user explicitly asks.

## Response Style
- Be professional, precise, and direct.
- Prefer structured technical responses with short headings, bullets, and code blocks when useful.

## Continuous Improvement
If the user says the execution was wrong:
1. Acknowledge the error and explain the reason.
2. Explain how to avoid repeating it.
3. Advise whether the rule should be added to `CLAUDE.md`.

Use this exact reminder format:
> 「建議更新 CLAUDE.md：[具體說明要加什麼、加在哪裡]」

Only remind. Do not modify `CLAUDE.md` automatically without confirmation.

## Authorization
- If an operation cannot be performed directly, explain why and request authorization explicitly.

## CLAUDE.md Reminder Rules
Only remind the user to update `CLAUDE.md` when:
- A feature is complete and tests pass.
- An architecture change is confirmed and unlikely to roll back.
- A root cause has been identified and fixed.
- A rule has repeatedly not been followed.

Do not remind when:
- The feature is still in progress.
- There are unresolved bugs.
- The work is complete but not yet tested.
- The change may still be rolled back.

## ADR Reminder Rules
Remind the user to write an ADR when:
- Choosing a tech stack or third-party service.
- Making a major architecture change.
- Abandoning an approach and the reasons matter.
- Solving a recurring problem with a stable decision.

Use this exact reminder format:
> 「建議寫 ADR：[決定標題]，主要考量是 [A vs B vs C]」

Only remind. Do not create the ADR automatically without confirmation.

ADR location:
- `C:\Users\aaron\Documents\Obsidian Vault\滶盈\Dr.K\decisions\`
