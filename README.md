# Hybrid Author Prospect Finder (Directory Discovery -> Crawl -> Validate)

## Goal
Automate finding 20-40 qualified indie/self-pub author prospects, with optional `fully_verified` and `email_only` modes for stricter outbound-ready or lighter public-email collection workflows.

## Method (Hybrid)
1. Stage 1: Harvest deterministic author-directory candidates first (collect ~80).
2. Stage 2: Validate in two passes:
   - cheap prefilter: robots + sitemap + small page plan
   - expensive verify: title/recency/listing/contact extraction on the planned pages only
3. Stage 3: Dedupe and keep final 20-40.

## Validation Rules Enforced
- Location shown as visible text or directory profile text.
- Indie/self-pub proof shown (`Independently published`, `KDP`, `IngramSpark`, `Draft2Digital`, `B&N Press`, etc.) or trusted directory membership proof.
- Amazon/B&N listing enrichment is attempted from book pages, but `ListingStatus=missing|unverified` is allowed by default unless `--listing-strict` is enabled.
- Skip famous/enterprise signals (`Big-5` imprint claims, NYT bestseller claims, very large social following).
- Recency proof is staged now: `RecencyStatus=verified|missing`, with `missing` allowed by default.
- Weak or missing titles are staged now: `BookTitleStatus=ok|missing_or_weak`, with top title candidates preserved for manual follow-up.
- If indie proof is missing, skip and replace.
- `--validation-profile fully_verified` hard-gates final rows to:
  - visible author email text or clear text obfuscation on the author's own site
  - U.S. location proof
  - onsite/listing indie proof (directory-only proof is not enough)
  - verified Amazon/B&N listing with strict purchase signals
  - verified recency
  - strong title methods only
- `--validation-profile verified_no_us` keeps the same strict email/indie/listing/recency/title gates as `fully_verified`, but does not require U.S. location.
- `--validation-profile astra_outbound` is the one-command Astra preset on top of `fully_verified`:
  - forces U.S. location proof
  - forces strict listing proof
  - keeps strict merge policy
  - raises batch defaults toward the 20-40 outbound target without changing discovery sources
- `--validation-profile strict_interactive` keeps the same strict acceptance rules as `fully_verified`, but defaults validator runtime budgets to:
  - `--max-total-runtime 120`
  - `--max-seconds-per-domain 12`
  - `--max-fetches-per-domain 10`
- `--validation-profile strict_full` keeps the same strict acceptance rules as `fully_verified`, but defaults validator runtime budgets to:
  - `--max-total-runtime 900`
  - `--max-seconds-per-domain 25`
  - `--max-fetches-per-domain 16`
- `--validation-profile email_only` keeps validator-side junk filtering, requires:
  - plausible human author name
  - visible public email text or clear text obfuscation
  - `SourceURL`
  - basic dedupe
  but does not require listing proof, recency proof, indie proof, or U.S. location.
- `--validation-profile agent_hunt` keeps the staged validator behavior, reuses the Phase 3 replacement loop, and now scores exportable scout leads for cold outreach in three internal tiers:
  - `Tier 1` = safest `KEEP`
  - `Tier 2` = usable with caution `RECHECK`
  - `Tier 3` = weak but still potentially usable `RECHECK`
  - clearly bad leads still route to `REPLACE`
  This change does not loosen `strict_full` rules or strict CSV schemas.

## Output Columns
Validated/final CSV columns:
`AuthorName,BookTitle,BookTitleMethod,BookTitleScore,BookTitleConfidence,BookTitleStatus,BookTitleRejectReason,BookTitleTopCandidates,AuthorEmail,EmailQuality,AuthorNameSourceURL,BookTitleSourceURL,AuthorEmailSourceURL,AuthorEmailProofSnippet,AuthorWebsite,ContactPageURL,SubscribeURL,PressKitURL,MediaURL,ContactURL,ContactURLMethod,Location,LocationProofURL,LocationProofSnippet,LocationMethod,SourceURL,SourceTitle,SourceSnippet,IndieProofURL,IndieProofSnippet,IndieProofStrength,ListingURL,ListingStatus,ListingFailReason,ListingEnrichedFromURL,ListingEnrichmentMethod,RecencyProofURL,RecencyProofSnippet,RecencyStatus,RecencyFailReason`

`contact_queue.csv` keeps contactable leads for outreach ranking and includes the same proof/enrichment context needed for manual review, including `AuthorEmail`, `ContactURL`, and `ContactURLMethod`.
Under strict merge policy it also retains staged contactable rows that are not promoted to the master yet.

## Why This Approach
- Keeps automation useful while reducing deliverability/compliance risk from scraped-email outreach.
- Produces higher-quality prospects with proof-backed records, allowing manual or opt-in contact workflows.

## Scripts
- `prospect_harvest.py`
- `prospect_validate.py`
- `prospect_dedupe.py`
- `run_lead_finder.py` (one-command runner)
- `run_lead_finder_loop.py` (repeated small-batch automation)
- `enrich_queue.py` (re-validate staged queue rows and emit strict-promotable rows)
- `export_contacts.py` (ranked daily outreach exports from `contact_queue.csv`)
- `verify_emails.py` (email verification)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run Tests
```bash
.venv/bin/python -m pip install pytest
.venv/bin/python -m pytest -q
```

## Run Flow (Simplest)
```bash
python run_lead_finder.py
```

This produces:
- `candidates.csv`
- `validated.csv`
- `final_prospects.csv`
- `contact_queue.csv`
- `near_miss_location.csv` for strict rows that would otherwise qualify except for missing/ambiguous U.S. author location proof

Fully verified variant:
```bash
python run_lead_finder_loop.py \
  --validation-profile fully_verified \
  --goal-final 20 \
  --master-output leads_full.csv \
  --verified-output fully_verified_leads.csv
```

This keeps the rich audit CSVs internally and writes a 3-column no-header export:
`AuthorName,AuthorEmail,SourceURL`

Strict profiles also write a location-recovery artifact:
- per-run validator artifact: `runs/run_XXX_near_miss_location.csv`
- aggregated loop artifact: `near_miss_location.csv`

Each near-miss row includes:
`AuthorName,BookTitle,AuthorEmail,SourceURL,RejectReason,BestLocationSnippet,CandidateRecoveryURLs`

Strict verified variant without the U.S. location gate:
```bash
python run_lead_finder_loop.py \
  --validation-profile verified_no_us \
  --listing-strict \
  --goal-final 20 \
  --max-runs 5 \
  --max-stale-runs 5 \
  --master-output leads_full.csv \
  --verified-output fully_verified_leads.csv
```

Strict interactive preset:
```bash
python run_lead_finder_loop.py \
  --validation-profile strict_interactive \
  --goal-final 5 \
  --max-runs 3 \
  --max-stale-runs 3 \
  --verified-output fully_verified_leads.csv
```

Strict full preset:
```bash
python run_lead_finder_loop.py \
  --validation-profile strict_full \
  --goal-final 20 \
  --verified-output fully_verified_leads.csv
```

Agent-hunt scout preset:
```bash
python run_lead_finder_loop.py \
  --validation-profile agent_hunt \
  --goal-final 20 \
  --scouted-output scouted_leads.csv \
  --master-output leads_full.csv
```

Lightweight public-email collection preset:
```bash
python run_lead_finder_loop.py \
  --validation-profile email_only \
  --goal-final 100 \
  --minimal-output author_email_source.csv \
  --master-output leads_full.csv
```

This keeps the rich internal audit rows in `leads_full.csv` and writes a 3-column export focused on:
`AuthorName,AuthorEmail,SourceURL`

For `agent_hunt`, this keeps the staged validator behavior and writes a separate 3-column no-header scout export:
`AuthorName,AuthorEmail,SourceURL`

For `agent_hunt`, the scout export now follows a cold-outreach standard:
- exported scout leads only need a real public email, a plausible author/email match, and reasonable indie-author relevance
- imperfect proof is allowed in `agent_hunt` as long as the lead is still usable for outreach
- `strict_full` remains the gold-standard strict verifier and is unchanged

Per-run `agent_hunt` artifacts now also include same-schema tier splits in `runs/`:
- `run_XXX_tier_1_safest.csv`
- `run_XXX_tier_2_usable_with_caution.csv`
- `run_XXX_tier_3_weak_but_usable.csv`

`fully_verified_leads.csv` remains strict-only and is not loosened by `agent_hunt`.

## Current Operating Note
- `agent_hunt` is now considered tuned enough for the current source mix.
- `epic_directory` / `epic:author-directory` is a known low-ceiling source under the real `--listing-strict` off path.
- The pipeline now suppresses obvious `epic_directory` junk earlier and throttles its per-batch share, but further yield gains are more likely to come from improving source mix than from more internal routing tweaks.
- Prefer future work on stronger sources, weaker-source share reduction, and source ranking by observed yield before revisiting pipeline internals.

Astra outbound preset:
```bash
python run_lead_finder_loop.py \
  --validation-profile astra_outbound \
  --verified-output fully_verified_leads.csv \
  --master-output leads_full.csv \
  --contact-queue-output contact_queue.csv
```

When `astra_outbound` is selected, the loop automatically applies:
- `--goal-final 20` when no final goal was provided
- `--target 80` and `--min-candidates >= 80`
- `--batch-min 20 --batch-max 40`
- `--max-runs 50 --max-stale-runs 50`
- `--require-location-proof --us-only --listing-strict`
- `--merge-policy strict`

Daily outreach export:
```bash
python run_lead_finder_loop.py --merge-policy balanced --master-output leads_full.csv --contact-queue-output contact_queue.csv
python enrich_queue.py --input contact_queue.csv
python export_contacts.py --input contact_queue.csv --limit 100 --out contact_100.csv --out-email email_only.csv
```

This writes:
- `contact_100.csv` (no header): `AuthorName,BookTitle,ContactURL,SourceURL`
- `email_only.csv` (no header): `AuthorName,BookTitle,AuthorEmail,EmailSourceURL`

Email-only variant:
```bash
python run_lead_finder.py --require-email
```

## Local Dashboard
The main editable local dashboard lives under:

```text
ops_dashboard/
  app.py
  data_access.py
  static/
    index.html
    styles.css
    app.js
run_ops_dashboard.py
```

This dashboard is the local operator UI you can actually redesign with normal frontend files. The backend stays thin and reads the existing pipeline outputs from disk. No pipeline schemas or validation rules change.

Launch it with:
```bash
python run_ops_dashboard.py
```

Then open:
```text
http://127.0.0.1:8000
```

Editable UI files:
- `ops_dashboard/static/index.html`
- `ops_dashboard/static/styles.css`
- `ops_dashboard/static/app.js`

If you want the older Streamlit dashboard, those files still live under:

```text
dashboard/
  app.py
  data_loader.py
  utils.py
  views/
    overview.py
    verified_leads.py
    scouted_leads.py
    contact_queue.py
    near_misses.py
    reject_analysis.py
    candidate_detail.py
```

The Streamlit dashboard is still a thin local operator UI over the existing pipeline outputs. It reads CSV/JSON/JSONL artifacts from disk and does not change pipeline schemas or validation behavior.

Launch it with:
```bash
streamlit run dashboard/app.py
```

v1 pages:
- Run Pipeline
- Overview
- Verified Leads
- Scouted Leads
- Contact Queue
- Near Misses
- Reject Analysis
- Candidate Detail / Evidence

The `Run Pipeline` page is a thin browser launcher for the existing CLI loop. It:
- starts `run_lead_finder_loop.py` in the background
- writes outputs into a new top-level folder such as `browser_run_YYYYMMDD_HHMMSS/`
- tails the run log in the UI
- does not change pipeline validation rules or output schemas

Typical local flow:
1. `streamlit run dashboard/app.py`
2. Open the `Run Pipeline` page
3. Start a run
4. Switch to `Overview`, `Verified Leads`, `Scouted Leads`, or `Contact Queue` to review the new folder once it appears

The sidebar lets you pick a run folder and run tag. Supported inputs include:
- `fully_verified_leads.csv`
- `scouted_leads.csv`
- `contact_queue.csv`
- `near_miss_location.csv`
- `author_email_source.csv`
- `runs/run_*_stats.json`
- `runs/run_*_validate_stats.json`
- `runs/*_listing_debug.jsonl`
- `runs/*_location_debug.jsonl`

## Run Flow (Manual)
```bash
# 1) Harvest up to ~80 candidate pages
python prospect_harvest.py --target 140 --min-candidates 80 --per-query 30 --max-per-domain 0 --goodreads-pages 3 --output candidates.csv

# 2) Validate candidates against the rule set
python prospect_validate.py --input candidates.csv --output validated.csv

# 3) Dedupe and keep final 20-40
python prospect_dedupe.py --input validated.csv --output final_prospects.csv --min-final 20 --max-final 40
```

## Auto Repeat (Small Batches Until Goal)
```bash
python run_lead_finder_loop.py --goal-total 100 --batch-min 10 --batch-max 20 --max-runs 20 --master-output all_prospects.csv
```

This repeats the full pipeline in quality-focused batches and keeps accumulating unique leads into `all_prospects.csv`.
The loop now rotates query themes per batch automatically to reduce duplicate-heavy runs.

For stricter quality (drop `Location=Unknown`):
```bash
python run_lead_finder_loop.py --goal-total 100 --batch-min 10 --batch-max 20 --max-runs 20 --require-location --master-output all_prospects.csv
```

For email-only accumulation:
```bash
python run_lead_finder_loop.py --goal-total 100 --batch-min 10 --batch-max 20 --max-runs 20 --require-email --master-output all_prospects.csv
```

For contact-path mode (prefer contact/subscribe routes even without email):
```bash
python run_lead_finder_loop.py --goal-total 100 --batch-min 10 --batch-max 20 --max-runs 20 --require-contact-path --master-output all_prospects.csv
```

Recommended high-quality command:
```bash
python run_lead_finder_loop.py \
  --goal-total 100 \
  --batch-min 10 --batch-max 20 \
  --max-runs 30 --max-stale-runs 5 \
  --target 160 --max-candidates 120 \
  --max-goodreads-candidates 5 \
  --goodreads-outbound-per-url 2 \
  --search-timeout 8 --goodreads-timeout 10 --harvest-http-retries 1 \
  --delay 0.3 --timeout 12 \
  --require-email --email-gate strict \
  --master-output all_prospects.csv \
  --minimal-output author_email_source.csv
```

Notes:
- The current directory-first discovery path is the default no-key mode. Google CSE is health-checked once at run start and only used when it is actually available.
- When `--require-email` is enabled, each batch is now filtered through `verify_emails.py` before merge.
- `--email-gate balanced` keeps `deliverable` + `risky` emails with MX records.
- `--email-gate strict` keeps only `deliverable` emails with MX records.
- `--require-email` is stricter and will reduce volume; contactable leads are the default mode.
- If runs are slow due search/network timeouts, lower harvest timeouts:
  - `--search-timeout 8 --goodreads-timeout 10 --harvest-http-retries 1`
- If candidates are dominated by noisy seed sources, cap seed intake aggressively:
  - `--max-goodreads-candidates 0-5`
- Validation has bounded sitemap/nav support-page fetches:
  - `--max-pages-for-title 4`
  - `--max-pages-for-contact 6`
  - `--max-total-fetches-per-domain-per-run 14`
- Strict location recovery stays bounded and same-domain only:
  - `--location-recovery-mode same_domain|off`
  - `--location-recovery-pages 6`
  - recovery revisits likely `/about`, `/about-author`, `/bio`, `/author`, `/contact`, `/media`, and `/press` pages
  - recovery respects robots, cache reuse, and per-domain validator budgets
- Validation also supports hard runtime budgets now:
  - `--max-fetches-per-domain`
  - `--max-seconds-per-domain`
  - `--max-timeouts-per-domain`
  - `--max-total-runtime`
  - `--max-concurrency` (reserved Stage B knob; forwarded and recorded in stats)
- Validator resumability/caching:
  - per-run domain cache JSONL is auto-written next to `run_XXX_validate_stats.json`
  - cache stores robots handling, discovered sitemap URLs, and stable fetch failures
  - rerunning the same run tag reuses that cache and skips repeating robots/sitemap work when possible
  - per-run location debug JSONL is auto-written next to `run_XXX_validate_stats.json`
  - strict profiles also emit `run_XXX_near_miss_location.csv` for rows that fail only on U.S. author location after bounded recovery
- Title resolution is score-based now:
  - combines directory snippet hints, homepage/books pages, book detail pages, recency snippets, and verified listing pages
  - treats clean retailer product titles as a `listing_title_oracle` when an allowed listing page is available
  - reads structured data from `Book`, `CreativeWork`, `Product`, and `ItemList` objects
  - harvests title candidates from book-card image context such as `figcaption`, `aria-label`, and title attributes
  - weak titles no longer hard-reject the lead; they are staged with `BookTitleStatus=missing_or_weak`
  - writes `BookTitleMethod`, `BookTitleSourceURL`, `BookTitleScore`, `BookTitleConfidence`, `BookTitleRejectReason`, and `BookTitleTopCandidates` for tuning/debugging
- Recency is staged now:
  - writes `RecencyStatus` and `RecencyFailReason`
  - missing recency no longer hard-rejects otherwise contactable directory-backed leads
- `--require-contact-path` keeps only rows with non-homepage contact or subscribe/newsletter paths.
- `--contact-path-strict` tightens contact-path filtering to stronger hints only.
- `--merge-policy strict|balanced|open` controls what is allowed into `leads_full.csv`.
  - `strict` (default): only rows with visible exportable email/source evidence and `RecencyStatus=verified` merge to master.
  - `balanced`: rows with visible exportable email/source evidence merge; recency may still be staged.
  - `open`: merge all validated rows.
- `--validation-profile fully_verified` is stricter than merge policy:
  - requires visible on-page author email text/obfuscation
  - requires U.S. location proof
  - requires onsite/listing indie proof (`IndieProofStrength=onsite|both`)
  - requires `ListingStatus=verified`
  - requires `RecencyStatus=verified`
  - writes a 3-column final export to `fully_verified_leads.csv` by default (change with `--verified-output`)
- `run_lead_finder_loop.py` now auto-writes a source-linked export:
  - `AuthorName,AuthorEmail,SourceURL`
  - default file: `author_email_source.csv` (change with `--minimal-output`)
- `run_lead_finder_loop.py` now also writes:
  - `contact_queue.csv` for staged/manual-review leads and master leads without public email
  - `ContactURL` on validated/queue rows using priority:
    - `AuthorEmailSourceURL`
    - `ContactPageURL`
    - `SubscribeURL`
    - `PressKitURL` / `MediaURL`
    - `AuthorWebsite`
  - `runs/run_XXX_stats.json` with per-query candidate counts, top domains, validator reject reasons, listing reject reason counters, `BookTitleMethod` counts, `ListingStatus` counts, `IndieProofStrength` counts, email counts, stage timings, and per-domain validator performance stats
  - `runs/run_XXX_listing_debug.jsonl` when validator stats are enabled, capturing strict listing failures with the best candidate URL, page kind, evidence snippet, and per-candidate listing attempts
- `enrich_queue.py` can re-run staged queue rows with a higher validation budget and writes `promoted.csv` for rows that now satisfy strict master policy.
  - queue enrichment is best-first now: listing URLs and discovered `/books` / contact paths are processed before weaker staged rows
- `export_contacts.py` ranks the queue best-first for daily outreach:
  - `+100` visible author email
  - `+60` contact page
  - `+40` subscribe or press/media page
  - `+20` known location
  - `+20` strong book-title confidence
  - `+10` verified recency
  - `-50` robots/fetch failure flags when present
  - export dedupe suppresses repeated author names, emails, contact URLs, and listing keys within one export
- If you prefer a fixed query list, pass `--queries-file` (this disables auto-rotation).
- You can use Google Custom Search API (more stable than scraping) with:
  - `--google-api-key YOUR_KEY --google-cx YOUR_ENGINE_ID`
  - or env vars: `GOOGLE_API_KEY` and `GOOGLE_CSE_CX`
- You can also use Brave Search API as the primary web-search source:
  - `--brave-api-key YOUR_KEY`
  - or env var: `BRAVE_SEARCH_API_KEY`
- When `BRAVE_SEARCH_API_KEY` is set, the harvester uses Brave Search before Google CSE and before HTML/RSS fallbacks.
- Validation now checks robots.txt by default (RFC 9309). Override only if needed with `--ignore-robots`.
- `--robots-retry-seconds` controls temporary disallow retry window for unreachable/5xx robots.
- Bing fallback in harvest uses public Bing HTML/RSS result pages, not retired Bing Search APIs.

## Verify Emails (Before Outreach)
```bash
python verify_emails.py --input all_prospects.csv --output all_prospects_verified.csv
```

Default checks now include:
- email normalization
- hard role-address blocking (`abuse@`, `postmaster@`, `noreply@`, etc.)
- soft role-address retention as risky (`contact@`, `hello@`, `info@`, etc.)
- common typo fixes (`gamil.com` -> `gmail.com`)
- DNS/MX checks

Optional SMTP probe (slower, may be blocked by some providers):
```bash
python verify_emails.py --input all_prospects.csv --output all_prospects_verified.csv --smtp-probe
```

Allow role addresses only if you want manual review later:
```bash
python verify_emails.py --input all_prospects.csv --output all_prospects_verified.csv --allow-role
```

Export deliverable+risky only:
```bash
awk -F, 'NR==1 || $0 ~ /,deliverable,|,risky,/' all_prospects_verified.csv > all_prospects_sendable.csv
```

## Notes
- The validator extracts public emails only from the author's own site when they appear as visible text, clear text obfuscation, or `mailto:` links.
- In `--validation-profile fully_verified`, `mailto:` is only accepted when the email is also visible in page text/obfuscation; href-only mailto links are rejected.
- Retail/category/search junk remains blocked in both harvest and validation. Amazon is only accepted on `/dp/` or `/gp/product/`; Target search/category pages are rejected.
- Heuristics are configurable in each script and should be tuned for your domain niche.

## References
- Google Custom Search JSON API overview:
  - https://developers.google.com/custom-search/v1/overview
- Google Custom Search pricing/quota:
  - https://developers.google.com/custom-search/v1/overview#pricing
- Google deprecation/transition note (Custom Search JSON API):
  - https://developers.google.com/custom-search/v1/overview
- Email normalization/validation library used:
  - https://github.com/JoshData/python-email-validator
- HTTP retry strategy reference:
  - https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.Retry
- Role mailbox guidance:
  - https://www.rfc-editor.org/info/rfc2142
- Robots Exclusion Protocol:
  - https://www.rfc-editor.org/info/rfc9309
- Bing Search API retirement announcement:
  - https://learn.microsoft.com/en-au/lifecycle/announcements/bing-search-api-retirement

Detailed notes are in `SOURCES.md`.
