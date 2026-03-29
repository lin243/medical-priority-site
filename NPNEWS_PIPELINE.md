# NPNews Daily Pipeline

## Default behavior

All scripts default to the previous day in Shanghai time.

- If today is `2026-03-29`, the pipeline target date is `2026-03-28`.
- The webpage reads a stable file:
  - `./npnews_daily_table_latest_scored.csv`

## End-to-end flow

### 1. Export previous-day data

Source tables:

- `cplatform_article_npnews`
- `clabeler_annotations_new_new`
- `drug_earth`

Rules:

- Pull previous-day NPNews articles by `spider_publish_time`
- Exclude `collapsed=true`
- Keep `_id` as `id_news`
- Enrich baseline drug fields from `drug_earth`
- Output daily CSV

Command:

```powershell
cd C:\Users\YYMF\Desktop\medical-priority-site
.\export_npnews_daily_table.ps1
```

### 2. Score with LLM

Rules:

- Read the exported CSV
- Score with `grok-4-fast` by default
- Keep JSONL intermediate results
- Write final scored CSV

Command:

```powershell
cd C:\Users\YYMF\Desktop\medical-priority-site
.\score_npnews_daily_table.ps1
```

### 3. Publish to webpage

Rules:

- Copy the final scored CSV to:
  - `C:\Users\YYMF\Desktop\medical-priority-site\npnews_daily_table_latest_scored.csv`
- The webpage always reads this stable file name

## One-command daily run

Recommended command:

```powershell
cd C:\Users\YYMF\Desktop\medical-priority-site
.\run_npnews_pipeline.ps1
```

What it does:

1. Uses previous day as `--date`
2. Exports the daily CSV
3. Runs LLM scoring
4. Publishes the final scored CSV to the webpage file

## Optional overrides

Specify a custom date:

```powershell
.\run_npnews_pipeline.ps1 --date 2026-03-26
```

Skip webpage publish:

```powershell
.\run_npnews_pipeline.ps1 --skip-web-publish
```

Keep the intermediate exported CSV:

```powershell
.\run_npnews_pipeline.ps1 --keep-intermediate
```

## Webpage behavior

The medical-news page now expects CSV files with the same schema as:

- `npnews_daily_table_latest_scored.csv`

Main table rules:

- Drug: prefer `drug_name_meta`, fallback to `drug_name_original`
- Company subtext: `baseline_company`
- Score: `llm_total_score`
- Update content: `label`
- News title: `title_news`
- News link: `url_news`
- Date: `time` as `YYYY-MM-DD`
- ID: `id_news`

Detail modal:

- Opens from the score cell
- Left side shows source fields and baseline fields
- Right side keeps LLM scoring details
