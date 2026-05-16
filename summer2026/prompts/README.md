# prompts/

Editable prompt inputs for `tool/iterate.py`.

- `system_iterate.md`: default system prompt template. `{context}` is replaced
  at runtime.
- `system_iterate_ablate_*.md`: ablation variants.
- `context/`: domain context files, usually `context/eudoxia_bauplan.md`.
- `../prompts.py`: builds the user prompt from scheduler code and simulator
  feedback.

Every run snapshots the rendered prompts and template files under
`experiments/<experiment>/runs/<N>/iters/<M>/`, so older runs remain
reproducible after prompt edits.

Select variants without Python changes:

```powershell
uv run python tool/iterate.py --scheduler <scheduler.py> --trace <trace.csv> `
    --system-prompt prompts/system_iterate.md `
    --context prompts/context/eudoxia_bauplan.md
```
