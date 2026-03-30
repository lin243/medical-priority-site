import asyncio
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(r"E:\llm-master")
sys.path.insert(0, str(REPO_ROOT / "src"))

from llm.elasticsearch import get_prod_client  # noqa: E402
from npnews_pipeline_utils import build_export_csv_path, get_previous_day_str  # noqa: E402


ARTICLE_INDEX = "cplatform_article_npnews"
LABEL_INDEX = "clabeler_annotations_new_new"
DRUG_INDEX = "drug_earth"
TARGET_DATE = get_previous_day_str()
OUTPUT_PATH = build_export_csv_path(Path(__file__).resolve().parent, TARGET_DATE)

TZ_SHANGHAI = timezone(timedelta(hours=8))

BASE_COLUMNS = [
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
]

DRUG_COLUMNS = [
    "baseline_name",
    "baseline_allname",
    "baseline_target",
    "baseline_MOA",
    "baseline_company",
]

COLUMNS = BASE_COLUMNS + DRUG_COLUMNS


def has_nonempty_label(row: dict[str, str]) -> bool:
    return bool((row.get("label") or "").strip())


def is_collapsed_article(article_source: dict[str, Any]) -> bool:
    return bool(article_source.get("collapsed"))


def day_range_ms(date_str: str) -> tuple[int, int]:
    start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=TZ_SHANGHAI)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def format_publish_time(timestamp_ms: Any) -> str:
    if not timestamp_ms:
        return ""
    try:
        dt = datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=TZ_SHANGHAI)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return str(timestamp_ms)


def format_meta(meta: list[dict[str, Any]]) -> str:
    if not meta:
        return ""
    return " | ".join(
        json.dumps(
            {
                "text": item.get("text"),
                "id": item.get("id"),
                "index": item.get("index"),
                "field": item.get("field"),
                "type": item.get("type"),
            },
            ensure_ascii=False,
        )
        for item in meta
    )


def first_drug_meta_id(meta: list[dict[str, Any]]) -> str:
    for item in meta:
        if item.get("index") == DRUG_INDEX and item.get("id"):
            return str(item["id"])
    return ""


def clean_hash_suffix(value: str) -> str:
    return value.split("##", 1)[0].strip()


def stringify(value: Any, clean_hash: bool = False) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        items = []
        for item in value:
            text = str(item)
            if clean_hash:
                text = clean_hash_suffix(text)
            items.append(text)
        return " | ".join(items)
    text = str(value)
    return clean_hash_suffix(text) if clean_hash else text


def extract_source_news(article_source: dict[str, Any]) -> str:
    candidate_keys = [
        "source",
        "source_name",
        "media_name",
        "media",
        "account_name",
        "bizname",
        "biz_name",
        "wechat_name",
        "site_name",
        "publisher",
        "source_news",
        "author",
    ]
    for key in candidate_keys:
        value = article_source.get(key)
        if not value:
            continue
        if isinstance(value, dict):
            for nested_key in ("name", "title", "text", "value"):
                nested_value = value.get(nested_key)
                if nested_value:
                    return str(nested_value).strip()
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, list):
            parts = [str(item).strip() for item in value if str(item).strip()]
            if parts:
                return " | ".join(parts)
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def blank_row(article_source: dict[str, Any], doc_id: str) -> dict[str, str]:
    row = {column: "" for column in COLUMNS}
    row["id_news"] = doc_id
    row["collapsed"] = "true" if is_collapsed_article(article_source) else "false"
    row["title_news"] = article_source.get("title") or ""
    row["url_news"] = article_source.get("url") or ""
    row["time"] = format_publish_time(article_source.get("spider_publish_time"))
    row["source_news"] = extract_source_news(article_source)
    row["_doc_id"] = doc_id
    row["_drug_meta_id"] = ""
    return row


async def scroll_search_all(
    client: Any,
    *,
    index: str,
    query: dict[str, Any],
    size: int = 500,
    source: list[str] | None = None,
) -> list[dict[str, Any]]:
    kwargs: dict[str, Any] = {
        "index": index,
        "body": {
            "size": size,
            "query": query,
        },
        "scroll": "2m",
        "request_timeout": 180,
    }
    if source is not None:
        kwargs["_source"] = source

    response = await client.search(**kwargs)
    scroll_id = response.get("_scroll_id")
    hits = response.get("hits", {}).get("hits", [])
    results = list(hits)

    try:
        while hits:
            response = await client.scroll(scroll_id=scroll_id, scroll="2m", request_timeout=180)
            scroll_id = response.get("_scroll_id", scroll_id)
            hits = response.get("hits", {}).get("hits", [])
            results.extend(hits)
    finally:
        if scroll_id:
            try:
                await client.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass

    return results


def build_rows_from_label_doc(
    article_source: dict[str, Any],
    doc_id: str,
    label_source: dict[str, Any],
) -> list[dict[str, str]]:
    anno_result = label_source.get("anno_result") or []
    item_by_region = {str(item.get("id")): item for item in anno_result if item.get("id")}
    group_items = [
        item
        for item in anno_result
        if item.get("from_name") == "pharma_news.drug_info"
        and isinstance((item.get("value") or {}).get("data"), list)
    ]

    if group_items:
        rows: list[dict[str, str]] = []
        for group in group_items:
            row = blank_row(article_source, doc_id)
            data_items = (group.get("value") or {}).get("data") or []
            for data_item in data_items:
                key = data_item.get("key")
                text = data_item.get("text") or ""
                region_item = item_by_region.get(str(data_item.get("region")))
                meta = (region_item or {}).get("meta") or []
                meta_str = format_meta(meta)
                drug_meta_id = first_drug_meta_id(meta)

                if key == "pharma_news.drug_info.drug":
                    row["drug_name_original"] = text
                    row["drug_name_meta"] = meta_str
                    row["_drug_meta_id"] = drug_meta_id
                elif key == "pharma_news.drug_info.drug_synonyms":
                    row["drug_synonym_original"] = text
                    row["drug_synonym_meta"] = meta_str
                    if not row["_drug_meta_id"]:
                        row["_drug_meta_id"] = drug_meta_id
                elif key == "pharma_news.drug_info.target":
                    row["target_original"] = text
                    row["target_meta"] = meta_str
                elif key == "pharma_news.drug_info.pharmacological_type":
                    row["MOA_original"] = text
                    row["MOA_target"] = meta_str
                elif key == "pharma_news.drug_info.assign_hint":
                    row["label"] = text

            rows.append(row)
        return rows

    rows: list[dict[str, str]] = []
    current_row: dict[str, str] | None = None
    for item in anno_result:
        from_name = item.get("from_name")
        value = item.get("value") or {}
        text = value.get("text") or ""
        meta = item.get("meta") or []
        meta_str = format_meta(meta)
        drug_meta_id = first_drug_meta_id(meta)

        if from_name == "pharma_news.drug_info.drug":
            current_row = blank_row(article_source, doc_id)
            current_row["drug_name_original"] = text
            current_row["drug_name_meta"] = meta_str
            current_row["_drug_meta_id"] = drug_meta_id
            rows.append(current_row)
            continue

        if current_row is None:
            continue

        if from_name == "pharma_news.drug_info.drug_synonyms" and not current_row["drug_synonym_original"]:
            current_row["drug_synonym_original"] = text
            current_row["drug_synonym_meta"] = meta_str
            if not current_row["_drug_meta_id"]:
                current_row["_drug_meta_id"] = drug_meta_id
        elif from_name == "pharma_news.drug_info.target" and not current_row["target_original"]:
            current_row["target_original"] = text
            current_row["target_meta"] = meta_str
        elif (
            from_name == "pharma_news.drug_info.pharmacological_type"
            and not current_row["MOA_original"]
        ):
            current_row["MOA_original"] = text
            current_row["MOA_target"] = meta_str
        elif from_name == "pharma_news.drug_info.assign_hint" and not current_row["label"]:
            current_row["label"] = text

    return rows


def enrich_with_drug_earth(rows: list[dict[str, str]], drug_docs: dict[str, dict[str, Any]]) -> None:
    for row in rows:
        source = drug_docs.get(row.get("_drug_meta_id", ""))
        if not source:
            continue
        row["baseline_name"] = stringify(source.get("name_show_cn"))
        row["baseline_allname"] = stringify(source.get("all_name_for_search"))
        row["baseline_target"] = stringify(source.get("targets_for_secondsearch"), clean_hash=True)
        row["baseline_MOA"] = stringify(source.get("pharmacological_name"))
        row["baseline_company"] = stringify(
            source.get("company_for_secondsearch"),
            clean_hash=True,
        )


async def fetch_articles(client: Any) -> list[dict[str, Any]]:
    start_ms, end_ms = day_range_ms(TARGET_DATE)
    query = {
        "range": {
            "spider_publish_time": {
                "gte": start_ms,
                "lt": end_ms,
            }
        }
    }
    return await scroll_search_all(
        client,
        index=ARTICLE_INDEX,
        query=query,
        size=200,
        source=[
            "collapsed",
            "title",
            "url",
            "spider_publish_time",
            "source",
            "source_name",
            "media_name",
            "media",
            "account_name",
            "bizname",
            "biz_name",
            "wechat_name",
            "site_name",
            "publisher",
            "source_news",
            "author",
        ],
    )


async def fetch_label_docs(client: Any, doc_ids: list[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    chunk_size = 500
    for start in range(0, len(doc_ids), chunk_size):
        chunk = doc_ids[start : start + chunk_size]
        query = {
            "terms": {
                "doc_id": chunk,
            }
        }
        results.extend(await scroll_search_all(client, index=LABEL_INDEX, query=query, size=500))
    return results


async def fetch_drug_docs(client: Any, drug_ids: list[str]) -> dict[str, dict[str, Any]]:
    docs: dict[str, dict[str, Any]] = {}
    chunk_size = 200
    for start in range(0, len(drug_ids), chunk_size):
        chunk = drug_ids[start : start + chunk_size]
        query = {
            "ids": {
                "values": chunk,
            }
        }
        hits = await scroll_search_all(client, index=DRUG_INDEX, query=query, size=200)
        for hit in hits:
            docs[str(hit.get("_id"))] = hit.get("_source", {})
    return docs


async def export_daily_table(
    *,
    target_date: str = TARGET_DATE,
    output_path: Path = OUTPUT_PATH,
) -> tuple[list[dict[str, str]], dict[str, int]]:
    global TARGET_DATE, OUTPUT_PATH
    old_target_date = TARGET_DATE
    old_output_path = OUTPUT_PATH
    TARGET_DATE = target_date
    OUTPUT_PATH = output_path

    client = get_prod_client()
    try:
        article_hits = await fetch_articles(client)
        filtered_article_hits = [
            hit
            for hit in article_hits
            if not is_collapsed_article(hit.get("_source", {}))
        ]
        article_by_id = {
            str(hit.get("_id")): hit.get("_source", {})
            for hit in filtered_article_hits
            if hit.get("_id")
        }
        doc_ids = list(article_by_id.keys())

        label_hits = await fetch_label_docs(client, doc_ids) if doc_ids else []
        label_by_doc_id: dict[str, list[dict[str, Any]]] = {}
        for hit in label_hits:
            source = hit.get("_source", {})
            doc_id = source.get("doc_id")
            if doc_id:
                label_by_doc_id.setdefault(str(doc_id), []).append(source)

        rows: list[dict[str, str]] = []
        for doc_id, article_source in article_by_id.items():
            article_rows: list[dict[str, str]] = []
            for label_source in label_by_doc_id.get(doc_id, []):
                article_rows.extend(build_rows_from_label_doc(article_source, doc_id, label_source))
            rows.extend(article_rows)

        rows = [row for row in rows if row.get("drug_name_original", "").strip()]
        rows = [row for row in rows if has_nonempty_label(row)]

        drug_ids = sorted({row.get("_drug_meta_id", "") for row in rows if row.get("_drug_meta_id")})
        drug_docs = await fetch_drug_docs(client, drug_ids) if drug_ids else {}
        enrich_with_drug_earth(rows, drug_docs)

        with output_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=COLUMNS,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(rows)

        stats = {
            "article_count": len(article_by_id),
            "collapsed_article_count": len(article_hits) - len(filtered_article_hits),
            "label_doc_count": len(label_hits),
            "row_count": len(rows),
            "drug_doc_count": len(drug_docs),
        }
        return rows, stats
    finally:
        TARGET_DATE = old_target_date
        OUTPUT_PATH = old_output_path
        await client.close()


async def main() -> None:
    rows, stats = await export_daily_table(target_date=TARGET_DATE, output_path=OUTPUT_PATH)
    print(f"saved to: {OUTPUT_PATH}")
    print(f"article_count: {stats['article_count']}")
    print(f"collapsed_article_count: {stats['collapsed_article_count']}")
    print(f"label_doc_count: {stats['label_doc_count']}")
    print(f"row_count: {len(rows)}")
    print(f"drug_doc_count: {stats['drug_doc_count']}")


if __name__ == "__main__":
    asyncio.run(main())
