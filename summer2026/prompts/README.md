# prompts/

Editable prompt templates for `tool/iterate.py`.

- `system_iterate.md`  -  system prompt template. `{context}` is replaced at runtime with the contents of `context/eudoxia_bauplan.md` (or whichever context file iterate.py is pointed at via `--context`).
- `context/`  -  eudoxia domain text that fills the `{context}` slot. Edit these to enrich what the LLM knows about the simulator.
- The user-turn feedback (per-iter perf table + scheduler code + objective) is still built in Python because it depends on simulation data. See `prompts.py:get_iteration_feedback_prompt`.

## Conventions

- Edit `.md` files directly to swap prompts  -  no Python changes needed for the system prompt.
- Every run of `tool/iterate.py` saves the fully-rendered prompts to `iterations/<run_id>/prompt_system.txt` and `prompt_user.txt`, plus copies of the template files used. So old runs stay reproducible even after you edit a template.
- Add new variants as new files (`system_iterate_est.md`, `system_iterate_minimal.md`, ...) and select via `--system-prompt`.
