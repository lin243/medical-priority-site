from datetime import datetime, timedelta, timezone
from pathlib import Path
import csv


TZ_SHANGHAI = timezone(timedelta(hours=8))


def build_history_dir(base_dir: Path) -> Path:
    return base_dir / "history"


def get_previous_day_str() -> str:
    return (datetime.now(TZ_SHANGHAI).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def build_export_csv_path(base_dir: Path, date_str: str) -> Path:
    return build_history_dir(base_dir) / f"npnews_daily_table_{date_str}.csv"


def build_scored_jsonl_path(base_dir: Path, date_str: str) -> Path:
    return build_history_dir(base_dir) / f"npnews_daily_table_{date_str}_scored.jsonl"


def build_scored_csv_path(base_dir: Path, date_str: str) -> Path:
    return build_history_dir(base_dir) / f"npnews_daily_table_{date_str}_scored.csv"


def build_web_csv_path(base_dir: Path) -> Path:
    return base_dir / "npnews_daily_table_latest_scored.csv"


def build_log_txt_path(base_dir: Path, date_str: str) -> Path:
    return build_history_dir(base_dir) / f"npnews_daily_log_{date_str}.txt"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return

    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def merge_scored_rows(
    existing_rows: list[dict[str, str]],
    daily_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    merged: dict[tuple[str, str, str], dict[str, str]] = {}
    order: list[tuple[str, str, str]] = []

    def make_key(row: dict[str, str]) -> tuple[str, str, str]:
        return (
            str(row.get("id_news") or "").strip(),
            str(row.get("time") or "").strip(),
            str(row.get("drug_name_original") or "").strip(),
        )

    for row in existing_rows:
        key = make_key(row)
        if key not in merged:
            order.append(key)
        merged[key] = row

    for row in daily_rows:
        key = make_key(row)
        if key not in merged:
            order.append(key)
        merged[key] = row

    result = [merged[key] for key in order]
    result.sort(
        key=lambda row: (
            str(row.get("time") or "").strip(),
            _safe_float(row.get("llm_total_score")),
            str(row.get("id_news") or "").strip(),
        ),
        reverse=True,
    )
    return result


def _safe_float(value: object) -> float:
    try:
        return float(str(value or "").strip())
    except (TypeError, ValueError):
        return float("-inf")
