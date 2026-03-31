import argparse
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any

from npnews_pipeline_utils import (
    build_export_csv_path,
    build_log_txt_path,
    build_scored_csv_path,
    build_scored_jsonl_path,
    build_web_csv_path,
    get_previous_day_str,
    merge_scored_rows,
    read_csv_rows,
    write_csv_rows,
)
from export_npnews_daily_table import (
    ARTICLE_INDEX,
    close_prod_client,
    export_daily_table,
    get_prod_client,
    scroll_search_all,
)
from npnews_pipeline_utils import TZ_SHANGHAI
from score_npnews_daily_table import score_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export NPNews rows for a day and score them with the internal LLM."
    )
    parser.add_argument("--date", default=get_previous_day_str())
    parser.add_argument("--prompt", type=Path, default=Path(r"C:\Users\YYMF\Desktop\prompt.md"))
    parser.add_argument("--model", default="grok-4-fast")
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--reasoning-effort", choices=["low", "medium", "high"], default="medium")
    parser.add_argument("--max-tokens", type=int, default=2500)
    parser.add_argument("--disable-search", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--keep-intermediate", action="store_true")
    parser.add_argument(
        "--export-csv",
        type=Path,
        default=None,
        help="Optional path for the filtered exported daily table.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Final scored csv path.",
    )
    parser.add_argument(
        "--output-jsonl",
        type=Path,
        default=None,
        help="Optional intermediate jsonl path used during scoring.",
    )
    parser.add_argument(
        "--web-csv",
        type=Path,
        default=None,
        help="Optional stable CSV path used by the webpage. Defaults to npnews_daily_table_latest_scored.csv.",
    )
    parser.add_argument(
        "--skip-web-publish",
        action="store_true",
        help="Skip copying the final scored CSV to the webpage CSV path.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    date_tag = args.date
    base_dir = Path(__file__).parent
    export_csv = args.export_csv or build_export_csv_path(base_dir, date_tag)
    output_csv = args.output_csv or build_scored_csv_path(base_dir, date_tag)
    output_jsonl = args.output_jsonl or build_scored_jsonl_path(base_dir, date_tag)
    web_csv = args.web_csv or build_web_csv_path(base_dir)
    log_txt = build_log_txt_path(base_dir, date_tag)
    prod_client = get_prod_client()
    try:
        rows, stats = await export_daily_table(
            target_date=args.date,
            output_path=export_csv,
            client=prod_client,
        )
        print(f"exported rows={len(rows)} to {export_csv}")
        print(
            f"article_count={stats['article_count']} "
            f"label_doc_count={stats['label_doc_count']} "
            f"drug_doc_count={stats['drug_doc_count']}"
        )

        csv_rows = await score_rows(
            rows=rows,
            prompt_path=args.prompt,
            output_jsonl=output_jsonl,
            output_csv=output_csv,
            model_name=args.model,
            concurrency=args.concurrency,
            reasoning_effort=args.reasoning_effort,
            max_tokens=args.max_tokens,
            enable_search=not args.disable_search,
            resume=args.resume,
            row_offset=0,
        )

        ok_count = sum(1 for item in csv_rows if item["status"] == "ok")
        error_count = len(csv_rows) - ok_count
        print(f"final csv: {output_csv}")
        print(f"completed rows={len(csv_rows)} ok={ok_count} error={error_count}")

        if not args.skip_web_publish:
            existing_rows = read_csv_rows(web_csv)
            merged_rows = merge_scored_rows(existing_rows, csv_rows)
            state_by_id = await fetch_wangyunyi_state_map(merged_rows, client=prod_client)
            apply_state_by_id(csv_rows, state_by_id)
            apply_state_by_id(merged_rows, state_by_id)
            write_csv_rows(output_csv, csv_rows)
            write_csv_rows(web_csv, merged_rows)
            print(
                f"published web csv: {web_csv} "
                f"(existing={len(existing_rows)} daily={len(csv_rows)} merged={len(merged_rows)})"
            )
        else:
            existing_rows = []
            merged_rows = []
            state_by_id = await fetch_wangyunyi_state_map(csv_rows, client=prod_client)
            apply_state_by_id(csv_rows, state_by_id)
            write_csv_rows(output_csv, csv_rows)

        state_filled_count = sum(1 for row in csv_rows if str(row.get("state") or "").strip())

        if not args.keep_intermediate and export_csv.exists():
            export_csv.unlink()
            print(f"removed intermediate export: {export_csv}")

        log_lines = build_run_log_lines(
            date_tag=date_tag,
            args=args,
            stats=stats,
            export_csv=export_csv,
            output_csv=output_csv,
            output_jsonl=output_jsonl,
            web_csv=web_csv,
            log_txt=log_txt,
            csv_rows=csv_rows,
            ok_count=ok_count,
            error_count=error_count,
            state_filled_count=state_filled_count,
            existing_rows=existing_rows,
            merged_rows=merged_rows,
        )
        log_txt.parent.mkdir(parents=True, exist_ok=True)
        log_txt.write_text("\n".join(log_lines) + "\n", encoding="utf-8-sig")
        print(f"saved log: {log_txt}")
    finally:
        await close_prod_client(prod_client)

def build_run_log_lines(
    *,
    date_tag: str,
    args: argparse.Namespace,
    stats: dict[str, int],
    export_csv: Path,
    output_csv: Path,
    output_jsonl: Path,
    web_csv: Path,
    log_txt: Path,
    csv_rows: list[dict[str, str]],
    ok_count: int,
    error_count: int,
    state_filled_count: int,
    existing_rows: list[dict[str, str]],
    merged_rows: list[dict[str, str]],
) -> list[str]:
    timestamp = datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S %z")
    status_counts: dict[str, int] = {}
    for row in csv_rows:
        key = str(row.get("status") or "unknown").strip() or "unknown"
        status_counts[key] = status_counts.get(key, 0) + 1

    return [
        f"run_timestamp: {timestamp}",
        f"target_date: {date_tag}",
        f"model: {args.model}",
        f"concurrency: {args.concurrency}",
        f"reasoning_effort: {args.reasoning_effort}",
        f"max_tokens: {args.max_tokens}",
        f"search_enabled: {not args.disable_search}",
        f"resume_mode: {args.resume}",
        f"keep_intermediate: {args.keep_intermediate}",
        f"skip_web_publish: {args.skip_web_publish}",
        "",
        "[export]",
        f"article_count: {stats.get('article_count', 0)}",
        f"collapsed_article_count: {stats.get('collapsed_article_count', 0)}",
        f"label_doc_count: {stats.get('label_doc_count', 0)}",
        f"drug_doc_count: {stats.get('drug_doc_count', 0)}",
        f"exported_row_count: {stats.get('row_count', len(csv_rows))}",
        f"export_csv: {export_csv}",
        "",
        "[score]",
        f"scored_row_count: {len(csv_rows)}",
        f"score_ok_count: {ok_count}",
        f"score_error_count: {error_count}",
        f"state_filled_count: {state_filled_count}",
        f"score_status_breakdown: {format_status_counts(status_counts)}",
        f"output_csv: {output_csv}",
        f"output_jsonl: {output_jsonl}",
        "",
        "[publish]",
        f"existing_web_row_count_before_merge: {len(existing_rows)}",
        f"merged_web_row_count_after_merge: {len(merged_rows)}",
        f"web_csv: {web_csv}",
        "",
        "[log_file]",
        f"log_txt: {log_txt}",
    ]


def format_status_counts(status_counts: dict[str, int]) -> str:
    if not status_counts:
        return "none"
    return ", ".join(
        f"{key}={status_counts[key]}"
        for key in sorted(status_counts.keys())
    )


async def fetch_wangyunyi_state_map(
    rows: list[dict[str, Any]],
    *,
    client: Any | None = None,
) -> dict[str, str]:
    doc_ids = sorted(
        {
            str(row.get("id_news") or "").strip()
            for row in rows
            if str(row.get("id_news") or "").strip()
        }
    )
    if not doc_ids:
        return {}

    managed_client = client or get_prod_client()
    owns_client = client is None
    try:
        state_by_id: dict[str, str] = {}
        chunk_size = 500
        for start in range(0, len(doc_ids), chunk_size):
            chunk = doc_ids[start : start + chunk_size]
            hits = await scroll_search_all(
                managed_client,
                index=ARTICLE_INDEX,
                query={"ids": {"values": chunk}},
                size=chunk_size,
                source=["system_assignment"],
            )
            for hit in hits:
                doc_id = str(hit.get("_id") or "").strip()
                if not doc_id:
                    continue
                source = hit.get("_source", {}) or {}
                assignments = source.get("system_assignment") or []
                state_by_id[doc_id] = extract_wangyunyi_state(assignments)
        return state_by_id
    finally:
        if owns_client:
            await close_prod_client(managed_client)


def extract_wangyunyi_state(assignments: Any) -> str:
    if not isinstance(assignments, list):
        return ""
    for item in assignments:
        if not isinstance(item, dict):
            continue
        assignee = str(item.get("assignee") or "").strip().lower()
        if assignee == "wangyunyi":
            return str(item.get("state") or "").strip()
    return ""


def apply_state_by_id(rows: list[dict[str, Any]], state_by_id: dict[str, str]) -> None:
    for row in rows:
        doc_id = str(row.get("id_news") or "").strip()
        row["state"] = state_by_id.get(doc_id, "")


if __name__ == "__main__":
    asyncio.run(main())
