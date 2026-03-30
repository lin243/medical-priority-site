import argparse
import asyncio
from datetime import datetime
from pathlib import Path

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
from export_npnews_daily_table import export_daily_table
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

    rows, stats = await export_daily_table(target_date=args.date, output_path=export_csv)
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
        write_csv_rows(web_csv, merged_rows)
        print(
            f"published web csv: {web_csv} "
            f"(existing={len(existing_rows)} daily={len(csv_rows)} merged={len(merged_rows)})"
        )
    else:
        existing_rows = []
        merged_rows = []

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
        existing_rows=existing_rows,
        merged_rows=merged_rows,
    )
    log_txt.write_text("\n".join(log_lines) + "\n", encoding="utf-8-sig")
    print(f"saved log: {log_txt}")

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


if __name__ == "__main__":
    asyncio.run(main())
