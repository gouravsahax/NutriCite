"""Download authoritative nutrition PDFs into data/raw/pdfs/.

Features: multiple fallback URLs per document, streaming + retry/backoff, %PDF magic-byte
validation (rejects HTML error pages), skip-if-exists, and a provenance manifest
(data/raw/pdfs/_manifest.json) with title/publisher/year/topic/sha256 for citations.

Usage (from project root):
    uv run python -m nutricite.ingestion.gather.download_pdfs
    uv run python -m nutricite.ingestion.gather.download_pdfs --only who icmr
    uv run python -m nutricite.ingestion.gather.download_pdfs --force --insecure
    uv run python -m nutricite.ingestion.gather.download_pdfs --list
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

DATA_DIR = Path(os.getenv("NUTRICITE_DATA_DIR", "data/raw"))
RAW_PDF_DIR = DATA_DIR / "pdfs"
MANIFEST_PATH = RAW_PDF_DIR / "_manifest.json"
CONTACT = os.getenv("NUTRICITE_CONTACT_EMAIL", "gourav@example.com")
MIN_PDF_BYTES = 20_000


class PermanentError(Exception):
    """Do not retry this URL (404, not a PDF, too small, ...)."""


@dataclass(frozen=True)
class PdfSource:
    filename: str
    title: str
    publisher: str
    year: int
    topic: str
    urls: tuple[str, ...]  # tried in order


IRIS = "https://iris.who.int/bitstream/handle/10665"
IRIS_OLD = "https://apps.who.int/iris/bitstream/handle/10665"

SOURCES: tuple[PdfSource, ...] = (
    # ---------------- India: ICMR-NIN ----------------
    PdfSource(
        "icmr_nin_2024.pdf",
        "Dietary Guidelines for Indians 2024",
        "ICMR-NIN",
        2024,
        "dietary-guidelines",
        (
            "https://www.nin.res.in/dietaryguidelines/pdfjs/locale/DGI07052024P.pdf",
            "https://nin.res.in/dietaryguidelines/pdfjs/locale/DGI_2024.pdf",
        ),
    ),
    PdfSource(
        "ifct_2017.pdf",
        "Indian Food Composition Tables 2017",
        "ICMR-NIN",
        2017,
        "food-composition",
        (
            "https://www.nin.res.in/ebooks/IFCT2017.pdf",
            "https://nin.res.in/ebooks/IFCT2017.pdf",
        ),
    ),
    PdfSource(
        "icmr_nin_rda_2020.pdf",
        "Nutrient Requirements for Indians: RDA and EAR 2020",
        "ICMR-NIN",
        2020,
        "nutrient-requirements",
        (
            "https://www.nin.res.in/RDA_Full_Report_2020.pdf",
            "https://www.nin.res.in/downloads/RDA_Full_Report_2020.pdf",
        ),
    ),
    PdfSource(
        "nin_dietary_guidelines_2011.pdf",
        "Dietary Guidelines for Indians - A Manual (2nd ed.)",
        "ICMR-NIN",
        2011,
        "dietary-guidelines",
        ("https://www.nin.res.in/downloads/DietaryGuidelinesforNINwebsite.pdf",),
    ),
    PdfSource(
        "icmr_type2_diabetes_guidelines_2018.pdf",
        "ICMR Guidelines for Management of Type 2 Diabetes",
        "ICMR",
        2018,
        "diabetes",
        (
            "https://main.icmr.nic.in/sites/default/files/guidelines/ICMR_GuidelinesType2diabetes2018_0.pdf",
        ),
    ),
    # ---------------- WHO / FAO ----------------
    PdfSource(
        "who_sodium_intake_2012.pdf",
        "Guideline: Sodium intake for adults and children",
        "WHO",
        2012,
        "sodium-hypertension",
        (
            f"{IRIS}/77985/9789241504836_eng.pdf",
            f"{IRIS_OLD}/77985/9789241504836_eng.pdf",
        ),
    ),
    PdfSource(
        "who_potassium_intake_2012.pdf",
        "Guideline: Potassium intake for adults and children",
        "WHO",
        2012,
        "potassium-hypertension",
        (
            f"{IRIS}/77986/9789241504829_eng.pdf",
            f"{IRIS_OLD}/77986/9789241504829_eng.pdf",
        ),
    ),
    PdfSource(
        "who_sugars_intake_2015.pdf",
        "Guideline: Sugars intake for adults and children",
        "WHO",
        2015,
        "sugars",
        (
            f"{IRIS}/149782/9789241549028_eng.pdf",
            f"{IRIS_OLD}/149782/9789241549028_eng.pdf",
        ),
    ),
    PdfSource(
        "who_saturated_trans_fat_2023.pdf",
        "Saturated fatty acid and trans-fatty acid intake for adults and children",
        "WHO",
        2023,
        "fats-cardiovascular",
        (f"{IRIS}/370419/9789240073630-eng.pdf",),
    ),
    PdfSource(
        "who_carbohydrate_intake_2023.pdf",
        "Carbohydrate intake for adults and children: WHO guideline",
        "WHO",
        2023,
        "carbohydrates-fibre",
        (f"{IRIS}/370420/9789240073593-eng.pdf",),
    ),
    PdfSource(
        "fao_who_vitamin_mineral_requirements_2004.pdf",
        "Vitamin and mineral requirements in human nutrition (2nd ed.)",
        "FAO/WHO",
        2004,
        "micronutrient-requirements",
        (
            f"{IRIS}/42716/9241546123.pdf",
            "https://www.fao.org/3/y2809e/y2809e.pdf",
        ),
    ),
    PdfSource(
        "fao_fats_fatty_acids_2010.pdf",
        "Fats and fatty acids in human nutrition (FAO Food and Nutrition Paper 91)",
        "FAO",
        2010,
        "fats",
        ("https://www.fao.org/3/i1953e/i1953e00.pdf",),
    ),
    PdfSource(
        "who_protein_amino_acid_requirements_2007.pdf",
        "Protein and amino acid requirements in human nutrition (TRS 935)",
        "WHO/FAO/UNU",
        2007,
        "protein",
        (
            f"{IRIS}/43411/WHO_TRS_935_eng.pdf",
            f"{IRIS_OLD}/43411/WHO_TRS_935_eng.pdf",
        ),
    ),
    PdfSource(
        "who_diet_chronic_disease_2003.pdf",
        "Diet, nutrition and the prevention of chronic diseases (TRS 916)",
        "WHO/FAO",
        2003,
        "chronic-disease",
        (
            f"{IRIS}/42665/WHO_TRS_916.pdf",
            f"{IRIS_OLD}/42665/WHO_TRS_916.pdf",
        ),
    ),
    # ---------------- USA ----------------
    PdfSource(
        "dga_2020_2025.pdf",
        "Dietary Guidelines for Americans, 2020-2025",
        "USDA/HHS",
        2020,
        "dietary-guidelines",
        (
            "https://www.dietaryguidelines.gov/sites/default/files/2021-03/"
            "Dietary_Guidelines_for_Americans-2020-2025.pdf",
        ),
    ),
    PdfSource(
        "nhlbi_dash_eating_plan.pdf",
        "Your Guide to Lowering Your Blood Pressure with DASH",
        "NHLBI",
        2006,
        "hypertension-dash",
        ("https://www.nhlbi.nih.gov/files/docs/public/heart/new_dash.pdf",),
    ),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        try:
            return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {}


def save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")


def download_pdf(
    client: httpx.Client,
    url: str,
    dest: Path,
    retries: int = 3,
    backoff_sec: float = 2.0,
) -> tuple[int, str]:
    """Stream url -> dest. Returns (bytes, sha256). Raises PermanentError / RuntimeError."""
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            print(f"[GET] {dest.name} <- {url} (attempt {attempt}/{retries})")
            sha, size = hashlib.sha256(), 0
            with client.stream("GET", url) as resp:
                if resp.status_code in (408, 429) or resp.status_code >= 500:
                    resp.raise_for_status()  # retryable
                if resp.status_code >= 400:
                    raise PermanentError(f"HTTP {resp.status_code}")
                with tmp.open("wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=65536):
                        if size == 0 and b"%PDF-" not in chunk[:1024]:
                            raise PermanentError("response is not a PDF (HTML error/login page?)")
                        f.write(chunk)
                        sha.update(chunk)
                        size += len(chunk)
            if size < MIN_PDF_BYTES:
                raise PermanentError(f"file too small ({size} bytes)")
            tmp.replace(dest)
            return size, sha.hexdigest()
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


def fetch_source(client: httpx.Client, src: PdfSource, force: bool, manifest: dict) -> str:
    dest = RAW_PDF_DIR / src.filename
    if dest.exists() and dest.stat().st_size >= MIN_PDF_BYTES and not force:
        print(f"[SKIP] {src.filename} already exists.")
        entry = manifest.get(src.filename) or {}
        if entry.get("status") != "ok":
            manifest[src.filename] = {
                "status": "ok",
                "title": src.title,
                "publisher": src.publisher,
                "year": src.year,
                "topic": src.topic,
                "url": entry.get("url", ""),
                "bytes": dest.stat().st_size,
                "sha256": sha256_file(dest),
                "downloaded_at": entry.get("downloaded_at", utc_now()),
            }
        return "cached"

    errors: list[str] = []
    for url in src.urls:
        try:
            size, digest = download_pdf(client, url, dest)
            manifest[src.filename] = {
                "status": "ok",
                "title": src.title,
                "publisher": src.publisher,
                "year": src.year,
                "topic": src.topic,
                "url": url,
                "bytes": size,
                "sha256": digest,
                "downloaded_at": utc_now(),
            }
            print(f"[OK] {dest} ({size / 1_048_576:.1f} MB)")
            return "ok"
        except (PermanentError, RuntimeError) as exc:
            errors.append(f"{url} -> {exc}")
            print(f"[WARN] {src.filename}: {exc}")
    manifest[src.filename] = {
        "status": "failed",
        "title": src.title,
        "publisher": src.publisher,
        "errors": errors,
        "tried_at": utc_now(),
    }
    return "failed"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Download nutrition guideline PDFs.")
    ap.add_argument("--only", nargs="*", default=[], help="substring filters on filename/publisher/topic")
    ap.add_argument("--force", action="store_true", help="re-download even if file exists")
    ap.add_argument("--insecure", action="store_true", help="disable TLS verification (broken gov certs)")
    ap.add_argument("--list", action="store_true", help="list configured sources and exit")
    args = ap.parse_args(argv)

    sources = [
        s
        for s in SOURCES
        if not args.only
        or any(k.lower() in f"{s.filename} {s.publisher} {s.topic}".lower() for k in args.only)
    ]
    if args.list:
        for s in sources:
            print(f"{s.filename:<48} {s.publisher:<12} {s.year}  {s.topic}")
        return 0

    RAW_PDF_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()
    headers = {
        "User-Agent": f"NutriCite-RAG/1.1 (research; mailto:{CONTACT})",
        "Accept": "application/pdf,*/*;q=0.8",
    }
    results: dict[str, str] = {}
    with httpx.Client(
        timeout=httpx.Timeout(180.0, connect=20.0),
        headers=headers,
        follow_redirects=True,
        verify=not args.insecure,
    ) as client:
        for src in sources:
            results[src.filename] = fetch_source(client, src, args.force, manifest)
            save_manifest(manifest)
            time.sleep(1.0)

    ok = sum(v in ("ok", "cached") for v in results.values())
    print(f"\nDone: {ok}/{len(results)} PDFs available in {RAW_PDF_DIR}")
    for name, status in results.items():
        if status == "failed":
            print(f"  FAILED: {name} - open the publisher page, copy the real PDF link into SOURCES")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
