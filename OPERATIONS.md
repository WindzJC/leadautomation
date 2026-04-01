# Operations

## Daily Operating Checklist

### 1. Keep Google Frozen
- Do not re-enable Google-backed runs unless the exact `key + cx` probe returns `200`.
- The current Google CSE block is tracked in GitHub Issue `#1`.

### 2. Run Only a Small `agent_hunt` Batch
- Use the scout lane, not the volume lane.

```bash
python run_lead_finder_loop.py \
  --validation-profile agent_hunt \
  --goal-final 20 \
  --scouted-output scouted_leads.csv \
  --master-output leads_full.csv
```

### 3. Check These 4 Files After Each Run
- `run_XXX_new_leads.csv`
- `scouted_leads.csv`
- `author_email_source.csv`
- `run_XXX_harvest_stats.json`

### 4. Decide Fast
- If `run_XXX_new_leads.csv` has new rows:
  - keep the pipeline in scout mode
  - review `scouted_leads.csv`
  - keep accumulating
- If `run_XXX_new_leads.csv` is empty again:
  - stop rerunning
  - inspect `run_XXX_harvest_stats.json`
  - treat source quality as the blocker

### 5. Watch the Source Mix
- If harvest is still dominated by weak directories, do not expect volume.
- Fix sources first.

### 6. Do Not Switch to `email_only` for Volume Yet
- `email_only` is the right high-volume mode only after the source layer improves.
- Do not use it as a volume engine on the current intake mix.

### 7. Weekly Rule
- If you get repeated empty `run_XXX_new_leads.csv` files over multiple runs:
  - pause the pipeline
  - work on source replacement
  - resume only after source quality improves

## Simple Operating Rule
- Need leads now: manual strict batches
- Need scout trickle: `agent_hunt`
- Need real volume later: upgrade sources, then use `email_only`
