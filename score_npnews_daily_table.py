import argparse
import asyncio
import csv
import json
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from npnews_pipeline_utils import (  # noqa: E402
    build_export_csv_path,
    build_scored_csv_path,
    build_scored_jsonl_path,
    get_previous_day_str,
)

DEFAULT_DATE = get_previous_day_str()
DEFAULT_INPUT = build_export_csv_path(Path(__file__).resolve().parent, DEFAULT_DATE)
DEFAULT_PROMPT = Path(r"C:\Users\YYMF\Desktop\prompt.md")
DEFAULT_OUTPUT_JSONL = build_scored_jsonl_path(Path(__file__).resolve().parent, DEFAULT_DATE)
DEFAULT_OUTPUT_CSV = build_scored_csv_path(Path(__file__).resolve().parent, DEFAULT_DATE)
PROJECT_ROOT = Path(__file__).resolve().parent
REQUIRED_LLM_ENVS = [
    "CPLATFORM_LLM_API_KEY",
    "CPLATFORM_BASE_URL",
    "LLM_PROJECT",
    "USER",
]
GEMINI_SEARCH_BACKUP_ENVS = [
    "GEMINI_2D5_FLASH_BKUP",
    "GEMINI_2D5_FLASH_LITE_BKUP",
    "GEMINI_2D5_PRO_BKUP",
    "GEMINI_3_PRO_BKUP",
    "GEMINI_3_FLASH_BKUP",
]
LEGACY_MODEL_ALIASES = {
    "gemini2.5flash": "gemini2.5-flash",
    "gemini25flash": "gemini2.5-flash",
    "gemini-2.5-flash": "gemini2.5-flash",
}

INPUT_COLUMNS = [
    "id_news",
    "collapsed",
    "drug_name_original",
    "drug_name_meta",
    "drug_synonym_original",
    "drug_synonym_meta",
    "target_original",
    "target_meta",
    "MOA_original",
    "MOA_target",
    "label",
    "title_news",
    "url_news",
    "time",
    "source_news",
    "baseline_name",
    "baseline_allname",
    "baseline_target",
    "baseline_MOA",
    "baseline_company",
]

OUTPUT_COLUMNS = INPUT_COLUMNS + [
    "row_id",
    "model_name",
    "status",
    "llm_drug_name",
    "llm_total_score",
    "llm_importance",
    "llm_weighted_score",
    "llm_bonus_score",
    "llm_summary",
    "llm_raw_json",
    "llm_error",
]


class DimensionScore(BaseModel):
    dimension: str = Field(alias="\u7ef4\u5ea6")
    score: float = Field(alias="\u5206\u6570")
    reason: str = Field(alias="\u7406\u7531")


class ScoreResult(BaseModel):
    drug_name: str = Field(alias="\u836f\u54c1\u540d\u79f0")
    total_score: float = Field(alias="\u603b\u5206")
    importance: Literal["\u9ad8", "\u4e2d", "\u4f4e"] = Field(alias="\u91cd\u8981\u7a0b\u5ea6")
    weighted_score: float | None = Field(default=None, alias="\u52a0\u6743\u603b\u5206")
    bonus_score: float | None = Field(
        default=None,
        alias="\u5176\u4ed6\u53ef\u52a0\u5206\u9879\u5206\u6570",
    )
    summary: str | None = Field(default=None, alias="\u7b80\u8981\u7ed3\u8bba")
    details: list[DimensionScore] = Field(
        default_factory=list,
        alias="\u7ef4\u5ea6\u6253\u5206\u8be6\u60c5",
    )

    model_config = {"populate_by_name": True}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score NPNews daily rows with the internal LLM.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT)
    parser.add_argument("--output-jsonl", type=Path, default=DEFAULT_OUTPUT_JSONL)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--model", default="grok-4-fast")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--reasoning-effort", choices=["low", "medium", "high"], default="medium")
    parser.add_argument("--max-tokens", type=int, default=2500)
    parser.add_argument("--disable-search", action="store_true")
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def read_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig").strip()


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def slice_rows(rows: list[dict[str, str]], offset: int, limit: int | None) -> list[dict[str, str]]:
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit is None:
        return rows[offset:]
    if limit < 0:
        raise ValueError("limit must be >= 0")
    return rows[offset : offset + limit]


def load_completed_ids(path: Path) -> set[int]:
    if not path.exists():
        return set()
    completed: set[int] = set()
    with path.open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            row_id = item.get("row_id")
            if isinstance(row_id, int):
                completed.add(row_id)
    return completed


def load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                records.append(item)
    return records


def compact_row(row: dict[str, str]) -> dict[str, str]:
    return {key: (row.get(key) or "").strip() for key in INPUT_COLUMNS}


def build_user_prompt(row_id: int, row: dict[str, str]) -> str:
    payload = {
        "row_id": row_id,
        "input_data": compact_row(row),
    }
    return (
        "\u8bf7\u57fa\u4e8e\u7cfb\u7edf\u63d0\u793a\u8bcd\u5904\u7406\u4e0b\u9762\u8fd9\u6761\u8f93\u5165\u6570\u636e\u3002\n"
        "\u8981\u6c42\uff1a\n"
        "1. \u5fc5\u987b\u6267\u884c\u8054\u7f51\u8865\u5145\u68c0\u7d22\uff0c\u9664\u975e\u6a21\u578b\u672c\u8eab\u4e0d\u652f\u6301\u3002\n"
        "2. \u53ea\u8fd4\u56de\u4e25\u683c JSON \u5bf9\u8c61\uff0c\u4e0d\u8981 Markdown\u3001\u4e0d\u8981\u8868\u683c\u3001\u4e0d\u8981\u89e3\u91ca\u6027\u524d\u7f00\u3002\n"
        "3. JSON \u5b57\u6bb5\u5fc5\u987b\u4e0e schema \u4e00\u81f4\u3002\n\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )


@lru_cache(maxsize=1)
def bootstrap_llm_runtime() -> tuple[Any, Any, Any, Any]:
    load_dotenv(PROJECT_ROOT / ".env")

    missing_envs = [name for name in REQUIRED_LLM_ENVS if not os.getenv(name)]
    if missing_envs:
        raise RuntimeError(
            "Missing required LLM environment variables: "
            f"{', '.join(missing_envs)}. "
            f"Add them to {PROJECT_ROOT / '.env'} before importing llm."
        )

    try:
        from llm import FallbackLLM, LLMFactory  # type: ignore[import-not-found]
        from llm.schema import TokenUsage  # type: ignore[import-not-found]
        from llm.utils import simple_parallel  # type: ignore[import-not-found]
    except Exception as exc:
        raise RuntimeError(
            "Failed to import Pharmcube llm package. "
            "Install/use the project runtime where `llm` is available, then retry."
        ) from exc

    return LLMFactory, FallbackLLM, TokenUsage, simple_parallel


def normalize_model_key(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def resolve_model_name(requested_name: str, available_aliases: list[str]) -> str:
    if requested_name == "auto-search":
        return requested_name

    if requested_name in available_aliases:
        return requested_name

    remapped_name = LEGACY_MODEL_ALIASES.get(requested_name.lower())
    if remapped_name and remapped_name in available_aliases:
        return remapped_name

    normalized_requested = normalize_model_key(requested_name)
    alias_by_key = {normalize_model_key(alias): alias for alias in available_aliases}
    if normalized_requested in alias_by_key:
        return alias_by_key[normalized_requested]

    raise RuntimeError(
        f"Unknown model alias `{requested_name}`. "
        f"Available aliases include: {', '.join(available_aliases[:20])}"
    )


def model_supports_search(model_name: str) -> bool:
    lowered = model_name.lower()
    return "gemini" in lowered or "grok" in lowered or "search" in lowered


def search_ready_for_model(model_name: str) -> tuple[bool, str]:
    lowered = model_name.lower()
    if "gemini" not in lowered:
        return True, ""

    if any(os.getenv(name) for name in GEMINI_SEARCH_BACKUP_ENVS):
        return True, ""

    return (
        False,
        "Gemini search routing is not configured. Missing backup envs like "
        + ", ".join(GEMINI_SEARCH_BACKUP_ENVS),
    )


def build_model(model_name: str) -> tuple[Any, str]:
    LLMFactory, FallbackLLM, _, _ = bootstrap_llm_runtime()
    available_aliases = list(LLMFactory.dispatch_table.keys())

    if model_name == "auto-search":
        fallback_candidates = []
        for candidate in ("gemini2.5-flash", "grok-4-fast", "gpt4.1mini"):
            try:
                fallback_candidates.append(resolve_model_name(candidate, available_aliases))
            except RuntimeError:
                continue
        if not fallback_candidates:
            raise RuntimeError("No fallback models are available in the current llm runtime.")
        return FallbackLLM(fallback_candidates, logger_prefix="NPNewsScore"), ",".join(fallback_candidates)

    resolved_name = resolve_model_name(model_name, available_aliases)
    return LLMFactory.load_model(resolved_name), resolved_name


async def score_one_row(
    *,
    model: Any,
    prompt_text: str,
    row_id: int,
    row: dict[str, str],
    model_name: str,
    enable_search: bool,
    reasoning_effort: str,
    max_tokens: int,
) -> dict[str, Any]:
    _, _, TokenUsage, _ = bootstrap_llm_runtime()
    tracker = TokenUsage()
    try:
        effective_enable_search = enable_search
        if enable_search:
            if not model_supports_search(model_name):
                raise RuntimeError(f"Model `{model_name}` does not support enable_search=True.")
            search_ready, reason = search_ready_for_model(model_name)
            if not search_ready:
                effective_enable_search = False
                print(f"[search-disabled] model={model_name} reason={reason}", file=sys.stderr)

        result = await model.chat(
            build_user_prompt(row_id, row),
            ScoreResult,
            tracker=tracker,
            system_msg=prompt_text,
            enable_search=effective_enable_search,
            use_json_object=True,
            reasoning_effort=reasoning_effort,
            max_tokens=max_tokens,
        )
        return {
            "row_id": row_id,
            "model_name": getattr(model, "name", None) or getattr(model, "model_names", None),
            "usage": tracker.model_dump(),
            "result": result.model_dump(by_alias=True),
        }
    except Exception as exc:
        return {
            "row_id": row_id,
            "model_name": getattr(model, "name", None) or getattr(model, "model_names", None),
            "error": str(exc),
        }


def append_jsonl(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8-sig") as handle:
        handle.write(json.dumps(item, ensure_ascii=False) + "\n")


def extract_fields_from_llm_raw_json(llm_raw_json: str) -> tuple[dict[str, Any], str]:
    if not llm_raw_json.strip():
        return {}, ""
    try:
        payload = json.loads(llm_raw_json)
    except json.JSONDecodeError as exc:
        return {}, f"Invalid llm_raw_json: {exc}"

    if not isinstance(payload, dict):
        return {}, "Invalid llm_raw_json: top-level value is not an object"

    return {
        "llm_drug_name": payload.get("药品名称", ""),
        "llm_total_score": payload.get("总分", ""),
        "llm_importance": payload.get("重要程度", ""),
        "llm_weighted_score": payload.get("加权总分", ""),
        "llm_bonus_score": payload.get("其他可加分项分数", ""),
        "llm_summary": payload.get("简要结论", ""),
    }, ""


def normalize_csv_row_from_llm_raw_json(row: dict[str, Any]) -> dict[str, Any]:
    llm_raw_json = str(row.get("llm_raw_json") or "")
    parsed_fields, parse_error = extract_fields_from_llm_raw_json(llm_raw_json)

    if parsed_fields:
        row.update(parsed_fields)
        if row.get("status") == "ok":
            row["llm_error"] = ""
    elif parse_error and not row.get("llm_error"):
        row["llm_error"] = parse_error

    return row


def flatten_for_csv(row: dict[str, str], scored: dict[str, Any]) -> dict[str, Any]:
    result = scored.get("result") or {}
    model_name = scored.get("model_name")
    if isinstance(model_name, list):
        model_name = ",".join(model_name)
    csv_row = {
        **{column: row.get(column, "") for column in INPUT_COLUMNS},
        "row_id": scored.get("row_id"),
        "model_name": model_name or "",
        "status": "ok",
        "llm_drug_name": "",
        "llm_total_score": "",
        "llm_importance": "",
        "llm_weighted_score": "",
        "llm_bonus_score": "",
        "llm_summary": "",
        "llm_raw_json": json.dumps(result, ensure_ascii=False),
        "llm_error": "",
    }
    return normalize_csv_row_from_llm_raw_json(csv_row)


def flatten_error_for_csv(row: dict[str, str], row_id: int, model_name: str, error: str) -> dict[str, Any]:
    csv_row = {
        **{column: row.get(column, "") for column in INPUT_COLUMNS},
        "row_id": row_id,
        "model_name": model_name,
        "status": "error",
        "llm_drug_name": "",
        "llm_total_score": "",
        "llm_importance": "",
        "llm_weighted_score": "",
        "llm_bonus_score": "",
        "llm_summary": "",
        "llm_raw_json": "",
        "llm_error": error,
    }
    return normalize_csv_row_from_llm_raw_json(csv_row)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(normalize_csv_row_from_llm_raw_json(dict(row)) for row in rows)


async def score_rows(
    *,
    rows: list[dict[str, str]],
    prompt_path: Path = DEFAULT_PROMPT,
    output_jsonl: Path = DEFAULT_OUTPUT_JSONL,
    output_csv: Path = DEFAULT_OUTPUT_CSV,
    model_name: str = "grok-4-fast",
    concurrency: int = 3,
    reasoning_effort: str = "medium",
    max_tokens: int = 2500,
    enable_search: bool = True,
    resume: bool = False,
    row_offset: int = 0,
) -> list[dict[str, Any]]:
    _, _, _, simple_parallel = bootstrap_llm_runtime()
    prompt_text = read_prompt(prompt_path)
    completed_ids = load_completed_ids(output_jsonl) if resume else set()
    model, resolved_model_name = build_model(model_name)
    pending_items = [
        (local_idx, row)
        for local_idx, row in enumerate(rows, start=row_offset)
        if local_idx not in completed_ids
    ]

    if pending_items:
        call_args = [
            {
                "args": [],
                "kwargs": {
                    "model": model,
                    "prompt_text": prompt_text,
                    "row_id": row_id,
                    "row": row,
                    "model_name": resolved_model_name,
                    "enable_search": enable_search,
                    "reasoning_effort": reasoning_effort,
                    "max_tokens": max_tokens,
                },
            }
            for row_id, row in pending_items
        ]
        results = await simple_parallel(score_one_row, call_args, concurrency=max(1, concurrency))
        for (row_id, _), scored in zip(pending_items, results):
            if scored is not None and "result" in scored:
                append_jsonl(output_jsonl, scored)
                total = scored["result"].get("\u603b\u5206")
                level = scored["result"].get("\u91cd\u8981\u7a0b\u5ea6")
                drug_name = scored["result"].get("\u836f\u54c1\u540d\u79f0")
                print(f"[score] row={row_id} score={total} level={level} drug={drug_name}")
                continue

            error_item = scored or {
                "row_id": row_id,
                "model_name": resolved_model_name,
                "error": "LLM call failed before returning an error payload.",
            }
            append_jsonl(output_jsonl, error_item)
            print(f"[score] row={row_id} failed: {error_item.get('error', 'unknown error')}", file=sys.stderr)

    records_by_row_id: dict[int, dict[str, Any]] = {}
    for item in load_jsonl_records(output_jsonl):
        row_id = item.get("row_id")
        if isinstance(row_id, int):
            records_by_row_id[row_id] = item

    csv_rows: list[dict[str, Any]] = []
    for row_id in sorted(records_by_row_id):
        local_index = row_id - row_offset
        if local_index < 0 or local_index >= len(rows):
            continue
        source_row = rows[local_index]
        item = records_by_row_id[row_id]
        if "result" in item:
            csv_rows.append(flatten_for_csv(source_row, item))
        else:
            csv_rows.append(
                flatten_error_for_csv(
                    source_row,
                    row_id,
                    str(item.get("model_name") or ""),
                    str(item.get("error") or ""),
                )
            )

    csv_rows.sort(key=lambda item: item["row_id"])
    write_csv(output_csv, csv_rows)
    return csv_rows


async def main() -> None:
    args = parse_args()
    all_rows = read_rows(args.input)
    rows = slice_rows(all_rows, args.offset, args.limit)
    if not rows:
        print("No rows to process.")
        return

    csv_rows = await score_rows(
        rows=rows,
        prompt_path=args.prompt,
        output_jsonl=args.output_jsonl,
        output_csv=args.output_csv,
        model_name=args.model,
        concurrency=args.concurrency,
        reasoning_effort=args.reasoning_effort,
        max_tokens=args.max_tokens,
        enable_search=not args.disable_search,
        resume=args.resume,
        row_offset=args.offset,
    )
    ok_count = sum(1 for item in csv_rows if item["status"] == "ok")
    error_count = len(csv_rows) - ok_count
    print(
        f"Completed rows={len(csv_rows)} ok={ok_count} error={error_count} "
        f"jsonl={args.output_jsonl} csv={args.output_csv}"
    )


if __name__ == "__main__":
    asyncio.run(main())
