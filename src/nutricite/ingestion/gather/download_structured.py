"""Download structured datasets for PostgreSQL ingestion + rule-based safety checks.

Outputs under data/raw/ (override root with NUTRICITE_DATA_DIR):
  csv/usda/<dataset>/*.csv                 USDA FoodData Central (foundation, sr_legacy, fndds; branded opt-in)
  csv/openfoodfacts/off_<country>.csv|jsonl  packaged foods: ingredients, allergens, additives, nutrition per 100 g
  json/openfda/drug_label_interactions.jsonl openFDA drug-label interaction / administration text per drug
  csv/recipes/<name>/                       Indian recipe datasets via Kaggle CLI (opt-in, needs credentials)
  csv/_manifest.json                        provenance (release names, URLs, counts)

Note: USDA files now go in per-dataset folders (foundation/, sr_legacy/ ...) because the datasets
share file names such as food.csv and nutrient.csv.

Usage:
    uv run python -m nutricite.ingestion.gather.download_structured
    uv run python -m nutricite.ingestion.gather.download_structured --usda foundation sr_legacy
    uv run python -m nutricite.ingestion.gather.download_structured --skip-usda --off-pages 40
    uv run python -m nutricite.ingestion.gather.download_structured --kaggle
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

DATA_DIR = Path(os.getenv("NUTRICITE_DATA_DIR", "data/raw"))
CSV_DIR = DATA_DIR / "csv"
USDA_DIR = CSV_DIR / "usda"
OFF_DIR = CSV_DIR / "openfoodfacts"
RECIPES_DIR = CSV_DIR / "recipes"
FDA_DIR = DATA_DIR / "json" / "openfda"
MANIFEST_PATH = CSV_DIR / "_manifest.json"

CONTACT = os.getenv("NUTRICITE_CONTACT_EMAIL", "gourav@example.com")
OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY", "")

USDA_PAGE = "https://fdc.nal.usda.gov/download-datasets"
USDA_BASE = "https://fdc.nal.usda.gov/fdc-datasets/"
USDA_DATASETS: dict[str, dict[str, Any]] = {
    "foundation": {
        "token": "foundation_food_csv",
        "fallbacks": [
            "FoodData_Central_foundation_food_csv_2025-04-24.zip",
            "FoodData_Central_foundation_food_csv_2024-10-31.zip",
            "FoodData_Central_foundation_food_csv_2024-04-18.zip",
        ],
    },
    "sr_legacy": {
        "token": "sr_legacy_food_csv",
        "fallbacks": ["FoodData_Central_sr_legacy_food_csv_2018-04.zip"],
    },
    "fndds": {
        "token": "survey_food_csv",
        "fallbacks": [
            "FoodData_Central_survey_food_csv_2024-10-31.zip",
            "FoodData_Central_survey_food_csv_2022-10-28.zip",
        ],
    },
    "branded": {  # ~3 GB unzipped - opt-in only
        "token": "branded_food_csv",
        "fallbacks": [
            "FoodData_Central_branded_food_csv_2025-04-24.zip",
            "FoodData_Central_branded_food_csv_2024-10-31.zip",
        ],
    },
}
DEFAULT_USDA = ["foundation", "sr_legacy", "fndds"]

OFF_SEARCH = "https://world.openfoodfacts.org/api/v2/search"
OFF_FIELDS = (
    "code,product_name,brands,quantity,serving_size,categories_tags,ingredients_text,allergens_tags,"
    "traces_tags,additives_tags,nutriscore_grade,nova_group,nutriments"
)

FDA_LABEL_URL = "https://api.fda.gov/drug/label.json"
# (generic name, nutrient/food focus) - drugs with well-known food/nutrient interactions
FDA_DRUGS: list[tuple[str, str]] = [
    ("warfarin", "vitamin K, cranberry, alcohol"),
    ("atorvastatin", "grapefruit"),
    ("simvastatin", "grapefruit"),
    ("lovastatin", "grapefruit"),
    ("rosuvastatin", "antacids"),
    ("metformin", "vitamin B12, alcohol"),
    ("glimepiride", "alcohol, hypoglycemia"),
    ("glipizide", "alcohol, hypoglycemia"),
    ("levothyroxine", "calcium, iron, soy, fibre, coffee timing"),
    ("lisinopril", "potassium, salt substitutes"),
    ("enalapril", "potassium, salt substitutes"),
    ("losartan", "potassium, salt substitutes"),
    ("spironolactone", "potassium"),
    ("hydrochlorothiazide", "potassium, calcium"),
    ("furosemide", "potassium, magnesium"),
    ("amlodipine", "grapefruit"),
    ("digoxin", "potassium, fibre"),
    ("omeprazole", "vitamin B12, magnesium, iron, calcium"),
    ("pantoprazole", "vitamin B12, magnesium, iron"),
    ("alendronate", "calcium, food timing"),
    ("ciprofloxacin", "dairy, calcium, iron"),
    ("doxycycline", "dairy, iron, calcium"),
    ("phenelzine", "tyramine-rich foods"),
    ("lithium", "sodium, hydration"),
    ("phenytoin", "folate, calcium"),
    ("methotrexate", "folate, alcohol"),
    ("carbidopa", "protein"),
    ("allopurinol", "hydration, alcohol"),
]
FDA_FIELDS = (
    "drug_interactions",
    "dosage_and_administration",
    "information_for_patients",
    "indications_and_usage",
)
FOOD_NUTRIENT_TERMS = [
    "vitamin k", "grapefruit", "potassium", "calcium", "iron", "magnesium", "zinc", "vitamin b12",
    "vitamin b-12", "folate", "folic acid", "vitamin d", "sodium", "salt substitute", "alcohol",
    "tyramine", "caffeine", "coffee", "fiber", "fibre", "dairy", "milk", "antacid", "soy",
    "high-fat", "high fat", "protein", "green tea", "cranberry", "st. john",
]
_TERM_RE = re.compile("|".join(re.escape(t) for t in FOOD_NUTRIENT_TERMS), re.IGNORECASE)

KAGGLE_DATASETS = {
    "indian_food_101": "nehaprabhavalkar/indian-food-101",
    "indian_recipes_6000": "kanishk307/6000-indian-food-recipes-dataset",
}


# --------------------------------------------------------------------------- utils
class PermanentError(Exception):
    """Do not retry (404, invalid archive, ...)."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_manifest() -> dict[str, Any]:
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {}


def save_manifest(manifest: dict[str, Any]) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")


def make_client(timeout: float = 120.0) -> httpx.Client:
    headers = {
        "User-Agent": f"NutriCite-RAG/1.1 (research; mailto:{CONTACT})",
        "Accept": "application/json,text/html,application/zip,*/*;q=0.8",
    }
    return httpx.Client(timeout=httpx.Timeout(timeout, connect=20.0), headers=headers, follow_redirects=True)


def request_with_retry(
    client: httpx.Client,
    url: str,
    params: dict[str, Any] | None = None,
    retries: int = 4,
    backoff_sec: float = 2.0,
) -> httpx.Response:
    last = "unknown"
    for attempt in range(1, retries + 1):
        wait = backoff_sec * (2 ** (attempt - 1))
        try:
            resp = client.get(url, params=params)
            if resp.status_code in (408, 429) or resp.status_code >= 500:
                last = f"HTTP {resp.status_code}"
                try:
                    wait = max(float(resp.headers.get("Retry-After", wait)), 1.0)
                except ValueError:
                    pass
            elif resp.status_code >= 400:
                raise PermanentError(f"HTTP {resp.status_code} for {resp.url}")
            else:
                return resp
        except httpx.HTTPError as exc:
            last = repr(exc)
        if attempt < retries:
            print(f"[RETRY] {url} ({last}); sleeping {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"failed after {retries} attempts: {url} ({last})")


def stream_download(client: httpx.Client, url: str, dest: Path, retries: int = 3, backoff_sec: float = 3.0) -> int:
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            print(f"[GET] {url} (attempt {attempt}/{retries})")
            size = 0
            with client.stream("GET", url) as resp:
                if resp.status_code in (408, 429) or resp.status_code >= 500:
                    resp.raise_for_status()
                if resp.status_code >= 400:
                    raise PermanentError(f"HTTP {resp.status_code}")
                with tmp.open("wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=1 << 20):
                        f.write(chunk)
                        size += len(chunk)
            tmp.replace(dest)
            return size
        except PermanentError:
            tmp.unlink(missing_ok=True)
            raise
        except (httpx.HTTPError, OSError) as exc:
            tmp.unlink(missing_ok=True)
            if attempt == retries:
                raise RuntimeError(f"failed after {retries} attempts: {exc!r}") from exc
            wait = backoff_sec * (2 ** (attempt - 1))
            print(f"[RETRY] {exc!r}; sleeping {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError("unreachable")


# --------------------------------------------------------------------------- USDA FoodData Central
def discover_usda_files(client: httpx.Client) -> list[str]:
    try:
        html = request_with_retry(client, USDA_PAGE).text
    except Exception as exc:
        print(f"[WARN] could not read USDA download page ({exc}); using built-in fallbacks")
        return []
    return sorted(set(re.findall(r"FoodData_Central_[a-z_]+?_csv_\d{4}-\d{2}(?:-\d{2})?\.zip", html)))


def usda_candidates(key: str, discovered: list[str]) -> list[str]:
    cfg = USDA_DATASETS[key]

    def stamp(name: str) -> str:
        m = re.search(r"\d{4}-\d{2}(?:-\d{2})?", name)
        return m.group(0) if m else ""

    found = sorted((f for f in discovered if cfg["token"] in f), key=stamp, reverse=True)
    return found + [f for f in cfg["fallbacks"] if f not in found]


def extract_csvs(zip_path: Path, out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            if member.lower().endswith(".csv"):
                with zf.open(member) as src, (out_dir / Path(member).name).open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                n += 1
    return n


def download_usda(client: httpx.Client, keys: list[str], force: bool, manifest: dict[str, Any]) -> None:
    discovered: list[str] | None = None
    for key in keys:
        out_dir = USDA_DIR / key
        if (out_dir / "food.csv").exists() and not force:
            print(f"[SKIP] USDA {key}: CSVs already in {out_dir}")
            continue
        if discovered is None:
            discovered = discover_usda_files(client)
        zip_path = USDA_DIR / f"{key}.zip"
        USDA_DIR.mkdir(parents=True, exist_ok=True)
        done = False
        for filename in usda_candidates(key, discovered):
            url = USDA_BASE + filename
            try:
                size = stream_download(client, url, zip_path)
                if not zipfile.is_zipfile(zip_path):
                    zip_path.unlink(missing_ok=True)
                    raise PermanentError("downloaded file is not a zip")
                print(f"[UNZIP] {filename} -> {out_dir}")
                count = extract_csvs(zip_path, out_dir)
                zip_path.unlink(missing_ok=True)
                manifest[f"usda_{key}"] = {
                    "status": "ok",
                    "release": filename,
                    "url": url,
                    "zip_bytes": size,
                    "csv_files": count,
                    "downloaded_at": utc_now(),
                }
                print(f"[OK] USDA {key}: {count} CSV files")
                done = True
                break
            except (PermanentError, RuntimeError, zipfile.BadZipFile) as exc:
                print(f"[WARN] USDA {key}: {filename}: {exc}")
        if not done:
            manifest[f"usda_{key}"] = {"status": "failed", "tried_at": utc_now()}
            print(f"[ERR] USDA {key}: no working URL - check {USDA_PAGE}")


# --------------------------------------------------------------------------- Open Food Facts
def _tags(value: Any) -> str:
    if not isinstance(value, list):
        return ""
    return "|".join(str(t).split(":", 1)[-1] for t in value)


def off_row(p: dict[str, Any]) -> dict[str, Any]:
    n = p.get("nutriments") or {}
    code = str(p.get("code", ""))
    return {
        "code": code,
        "product_name": (p.get("product_name") or "").strip(),
        "brands": (p.get("brands") or "").strip(),
        "quantity": p.get("quantity", ""),
        "serving_size": p.get("serving_size", ""),
        "categories": _tags(p.get("categories_tags")),
        "ingredients_text": re.sub(r"\s+", " ", p.get("ingredients_text") or "").strip(),
        "allergens": _tags(p.get("allergens_tags")),
        "traces": _tags(p.get("traces_tags")),
        "additives": _tags(p.get("additives_tags")),
        "nutriscore_grade": p.get("nutriscore_grade", ""),
        "nova_group": p.get("nova_group", ""),
        "energy_kcal_100g": n.get("energy-kcal_100g"),
        "fat_100g": n.get("fat_100g"),
        "saturated_fat_100g": n.get("saturated-fat_100g"),
        "carbohydrates_100g": n.get("carbohydrates_100g"),
        "sugars_100g": n.get("sugars_100g"),
        "fiber_100g": n.get("fiber_100g"),
        "proteins_100g": n.get("proteins_100g"),
        "salt_100g": n.get("salt_100g"),
        "sodium_100g": n.get("sodium_100g"),
        "url": f"https://world.openfoodfacts.org/product/{code}",
    }


def download_off(
    client: httpx.Client, pages: int, country: str, force: bool, manifest: dict[str, Any], delay: float = 6.5
) -> None:
    OFF_DIR.mkdir(parents=True, exist_ok=True)
    jsonl_path = OFF_DIR / f"off_{country}.jsonl"
    csv_path = OFF_DIR / f"off_{country}.csv"
    if jsonl_path.exists() and csv_path.exists() and not force:
        print(f"[SKIP] Open Food Facts {country} already downloaded.")
        return

    params: dict[str, Any] = {
        "countries_tags_en": country,
        "fields": OFF_FIELDS,
        "page_size": 100,
        "sort_by": "unique_scans_n",
    }
    rows: list[dict[str, Any]] = []
    total: int | None = None
    page = 1
    while page <= pages:
        params["page"] = page
        print(f"[GET] Open Food Facts {country} page {page}/{pages}")
        try:
            data = request_with_retry(client, OFF_SEARCH, params=params).json()
        except PermanentError as exc:
            if "sort_by" in params:  # some deployments reject the sort key; retry without it
                params.pop("sort_by")
                print(f"[WARN] {exc}; retrying without sort_by")
                continue
            print(f"[ERR] Open Food Facts: {exc}")
            break
        except Exception as exc:
            print(f"[ERR] Open Food Facts: {exc}")
            break
        products = data.get("products") or []
        if not products:
            break
        total = int(data.get("count") or 0) or total
        rows.extend(r for r in (off_row(p) for p in products) if r["product_name"])
        if total and page * 100 >= total:
            break
        page += 1
        time.sleep(delay)  # OFF search limit: ~10 requests/minute

    if not rows:
        print("[WARN] Open Food Facts: no rows collected; nothing written")
        manifest["openfoodfacts"] = {"status": "failed", "tried_at": utc_now()}
        return

    tmp_jsonl, tmp_csv = jsonl_path.with_suffix(".jsonl.part"), csv_path.with_suffix(".csv.part")
    with tmp_jsonl.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    with tmp_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    tmp_jsonl.replace(jsonl_path)
    tmp_csv.replace(csv_path)
    manifest["openfoodfacts"] = {
        "status": "ok",
        "country": country,
        "rows": len(rows),
        "total_available": total,
        "api": OFF_SEARCH,
        "license": "ODbL (database) - attribute Open Food Facts contributors",
        "downloaded_at": utc_now(),
    }
    print(f"[OK] Open Food Facts: {len(rows)} products -> {csv_path}")
    print("     (full India-wide coverage: use the official CSV/Parquet dump at world.openfoodfacts.org/data)")


# --------------------------------------------------------------------------- openFDA
def fda_trim(rec: dict[str, Any], drug: str, focus: str) -> dict[str, Any]:
    openfda = rec.get("openfda") or {}
    out: dict[str, Any] = {
        "drug_query": drug,
        "focus": focus,
        "set_id": rec.get("set_id", ""),
        "id": rec.get("id", ""),
        "effective_time": rec.get("effective_time", ""),
        "brand_name": openfda.get("brand_name", []),
        "generic_name": openfda.get("generic_name", []),
        "manufacturer_name": openfda.get("manufacturer_name", []),
        "route": openfda.get("route", []),
    }
    blob: list[str] = []
    for field in FDA_FIELDS:
        value = rec.get(field)
        if isinstance(value, list):
            out[field] = value
            blob.extend(str(v) for v in value)
    out["nutrient_mentions"] = sorted({m.lower() for m in _TERM_RE.findall(" ".join(blob))})
    return out


def download_openfda(client: httpx.Client, per_drug: int, force: bool, manifest: dict[str, Any]) -> None:
    FDA_DIR.mkdir(parents=True, exist_ok=True)
    dest = FDA_DIR / "drug_label_interactions.jsonl"
    if dest.exists() and dest.stat().st_size > 0 and not force:
        print(f"[SKIP] {dest.name} already exists.")
        return

    seen: set[str] = set()
    records: list[dict[str, Any]] = []
    for drug, focus in FDA_DRUGS:
        params: dict[str, Any] = {
            "search": f'openfda.generic_name:"{drug}" AND _exists_:drug_interactions',
            "limit": per_drug,
            "sort": "effective_time:desc",
        }
        if OPENFDA_API_KEY:
            params["api_key"] = OPENFDA_API_KEY
        try:
            print(f"[GET] openFDA labels: {drug}")
            data = request_with_retry(client, FDA_LABEL_URL, params=params).json()
            added = 0
            for rec in data.get("results", []):
                key = rec.get("set_id") or rec.get("id") or ""
                if key in seen:
                    continue
                seen.add(key)
                records.append(fda_trim(rec, drug, focus))
                added += 1
            print(f"[OK] {drug}: {added} labels")
        except PermanentError as exc:  # 404 = no matches
            print(f"[WARN] {drug}: {exc}")
        except Exception as exc:
            print(f"[ERR] {drug}: {exc}")
        time.sleep(0.5)

    if not records:
        manifest["openfda"] = {"status": "failed", "tried_at": utc_now()}
        print("[WARN] openFDA: nothing collected")
        return
    tmp = dest.with_suffix(".jsonl.part")
    with tmp.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(dest)
    manifest["openfda"] = {
        "status": "ok",
        "labels": len(records),
        "drugs": len(FDA_DRUGS),
        "api": FDA_LABEL_URL,
        "downloaded_at": utc_now(),
    }
    print(f"[OK] openFDA: {len(records)} labels -> {dest}")


# --------------------------------------------------------------------------- Kaggle (optional)
def download_kaggle(force: bool, manifest: dict[str, Any]) -> None:
    exe = shutil.which("kaggle")
    if not exe:
        print("[SKIP] Kaggle CLI not found. Install: uv add kaggle, then put kaggle.json in ~/.kaggle/")
        return
    for name, slug in KAGGLE_DATASETS.items():
        out_dir = RECIPES_DIR / name
        if out_dir.exists() and any(out_dir.glob("*.csv")) and not force:
            print(f"[SKIP] Kaggle {name} already downloaded.")
            continue
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f"[GET] Kaggle {slug}")
        proc = subprocess.run(
            [exe, "datasets", "download", "-d", slug, "-p", str(out_dir), "--unzip"],
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            manifest[f"kaggle_{name}"] = {"status": "ok", "slug": slug, "downloaded_at": utc_now()}
            print(f"[OK] {name} -> {out_dir}")
        else:
            manifest[f"kaggle_{name}"] = {"status": "failed", "slug": slug, "error": proc.stderr.strip()[:300]}
            print(f"[ERR] Kaggle {slug}: {proc.stderr.strip()[:200]}")


# --------------------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Download structured nutrition datasets.")
    ap.add_argument("--usda", nargs="*", default=DEFAULT_USDA, choices=list(USDA_DATASETS), help="USDA datasets")
    ap.add_argument("--skip-usda", action="store_true")
    ap.add_argument("--skip-off", action="store_true")
    ap.add_argument("--skip-openfda", action="store_true")
    ap.add_argument("--kaggle", action="store_true", help="also fetch Indian recipe datasets via Kaggle CLI")
    ap.add_argument("--off-pages", type=int, default=20, help="Open Food Facts pages of 100 products")
    ap.add_argument("--off-country", default="india", help="OFF country tag (English name)")
    ap.add_argument("--fda-per-drug", type=int, default=5, help="max labels per drug")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()
    with make_client() as client:
        if not args.skip_usda and args.usda:
            download_usda(client, args.usda, args.force, manifest)
            save_manifest(manifest)
        if not args.skip_off:
            download_off(client, args.off_pages, args.off_country, args.force, manifest)
            save_manifest(manifest)
        if not args.skip_openfda:
            download_openfda(client, args.fda_per_drug, args.force, manifest)
            save_manifest(manifest)
    if args.kaggle:
        download_kaggle(args.force, manifest)
        save_manifest(manifest)
    print(f"\nManifest: {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
