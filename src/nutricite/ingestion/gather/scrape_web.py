"""Scrape authoritative nutrition web pages + PubMed abstracts + PMC open-access full text.

Outputs under data/raw/ (override root with NUTRICITE_DATA_DIR):
  html/<name>.html          raw page                      html/_manifest.json  provenance
  html_clean/<name>.md      cleaned markdown (headings, lists, tables) with front-matter
  pubmed/<topic>.json       parsed abstracts per topic    pubmed/_all_pubmed.jsonl  de-duplicated
  xml/<topic>.xml           raw efetch XML
  pmc/<PMCID>.json|.md      open-access full text split into sections

Env: NUTRICITE_CONTACT_EMAIL (sent to NCBI + in User-Agent), NCBI_API_KEY (optional, 10 req/s).

Usage:
    uv run python -m nutricite.ingestion.gather.scrape_web
    uv run python -m nutricite.ingestion.gather.scrape_web --skip-pmc --retmax 50
    uv run python -m nutricite.ingestion.gather.scrape_web --skip-pages --skip-pubmed --pmc-max 100
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple

import httpx
from bs4 import BeautifulSoup, Tag
from lxml import etree

DATA_DIR = Path(os.getenv("NUTRICITE_DATA_DIR", "data/raw"))
HTML_DIR = DATA_DIR / "html"
CLEAN_DIR = DATA_DIR / "html_clean"
PUBMED_DIR = DATA_DIR / "pubmed"
PMC_DIR = DATA_DIR / "pmc"
XML_DIR = DATA_DIR / "xml"

CONTACT = os.getenv("NUTRICITE_CONTACT_EMAIL", "gourav@example.com")
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "")
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
NCBI_DELAY = 0.12 if NCBI_API_KEY else 0.4


# --------------------------------------------------------------------------- sources
class Page(NamedTuple):
    name: str
    url: str
    publisher: str
    topic: str


ODS_SLUGS = {
    "vitamin_a": "VitaminA",
    "vitamin_c": "VitaminC",
    "vitamin_d": "VitaminD",
    "vitamin_e": "VitaminE",
    "vitamin_k": "VitaminK",
    "vitamin_b6": "VitaminB6",
    "vitamin_b12": "VitaminB12",
    "thiamin": "Thiamin",
    "riboflavin": "Riboflavin",
    "niacin": "Niacin",
    "folate": "Folate",
    "biotin": "Biotin",
    "pantothenic_acid": "PantothenicAcid",
    "choline": "Choline",
    "calcium": "Calcium",
    "iron": "Iron",
    "magnesium": "Magnesium",
    "zinc": "Zinc",
    "potassium": "Potassium",
    "phosphorus": "Phosphorus",
    "iodine": "Iodine",
    "selenium": "Selenium",
    "copper": "Copper",
    "chromium": "Chromium",
    "manganese": "Manganese",
    "omega3": "Omega3FattyAcids",
}

WHO_FACT_SHEETS = {
    "who_healthy_diet": "healthy-diet",
    "who_salt_reduction": "salt-reduction",
    "who_obesity_overweight": "obesity-and-overweight",
    "who_diabetes": "diabetes",
    "who_hypertension": "hypertension",
    "who_anaemia": "anaemia",
    "who_malnutrition": "malnutrition",
    "who_cardiovascular_diseases": "cardiovascular-diseases-(cvds)",
}

OTHER_PAGES = [
    Page(
        "nhlbi_dash_eating_plan",
        "https://www.nhlbi.nih.gov/education/dash-eating-plan",
        "NHLBI",
        "hypertension-dash",
    ),
    Page(
        "niddk_diabetes_diet",
        "https://www.niddk.nih.gov/health-information/diabetes/overview/diet-eating-physical-activity",
        "NIDDK",
        "diabetes",
    ),
    Page(
        "niddk_nafld_diet",
        "https://www.niddk.nih.gov/health-information/liver-disease/nafld-nash/eating-diet-nutrition",
        "NIDDK",
        "fatty-liver",
    ),
    Page(
        "niddk_ckd_nutrition",
        "https://www.niddk.nih.gov/health-information/kidney-disease/chronic-kidney-disease-ckd/eating-nutrition",
        "NIDDK",
        "kidney",
    ),
]


def build_pages() -> list[Page]:
    pages = [
        Page(n, f"https://ods.od.nih.gov/factsheets/{slug}-HealthProfessional/", "NIH ODS", "micronutrient")
        for n, slug in ODS_SLUGS.items()
    ]
    pages += [
        Page(n, f"https://www.who.int/news-room/fact-sheets/detail/{slug}", "WHO", "fact-sheet")
        for n, slug in WHO_FACT_SHEETS.items()
    ]
    return pages + OTHER_PAGES


REVIEW_FILTER = '("meta-analysis"[pt] OR "systematic review"[pt] OR "practice guideline"[pt] OR "guideline"[pt])'

PUBMED_TOPICS: dict[str, str] = {
    "diabetes_diet": '"diabetes mellitus, type 2"[mh] AND (diet[tiab] OR dietary[tiab] OR nutrition[tiab])',
    "hypertension_dash": '(DASH[tiab] OR "dietary approaches to stop hypertension"[tiab]) AND hypertension[tiab]',
    "sodium_potassium_bp": '(sodium[tiab] OR potassium[tiab]) AND "blood pressure"[tiab] AND (diet[tiab] OR intake[tiab])',
    "pcos_nutrition": '"polycystic ovary syndrome"[mh] AND (diet[tiab] OR nutrition[tiab])',
    "lipids_diet": '(cholesterol[tiab] OR triglycerides[tiab] OR dyslipidemia[tiab]) AND (diet[tiab] OR dietary[tiab])',
    "masld_nafld_diet": '("fatty liver"[tiab] OR MASLD[tiab] OR NAFLD[tiab]) AND (diet[tiab] OR dietary[tiab])',
    "iron_deficiency_anemia": '("iron deficiency"[tiab] OR anemia[tiab] OR anaemia[tiab]) AND (diet[tiab] OR nutrition[tiab] OR iron[tiab])',
    "vegetarian_b12_iron_protein": '(vegetarian[tiab] OR vegan[tiab] OR "plant-based"[tiab]) AND (B12[tiab] OR iron[tiab] OR protein[tiab] OR zinc[tiab])',
    "vitamin_d_deficiency": '"vitamin D deficiency"[tiab] AND (supplementation[tiab] OR diet[tiab] OR intake[tiab])',
    "b12_metformin": 'metformin[tiab] AND ("vitamin B12"[tiab] OR cobalamin[tiab])',
    "ckd_diet": '"chronic kidney disease"[tiab] AND (diet[tiab] OR dietary[tiab] OR protein[tiab] OR potassium[tiab] OR phosphorus[tiab])',
    "pregnancy_nutrition": 'pregnancy[tiab] AND (nutrition[tiab] OR diet[tiab] OR "dietary supplement*"[tiab])',
    "gestational_diabetes_diet": '"gestational diabetes"[tiab] AND (diet[tiab] OR nutrition[tiab] OR "glycemic index"[tiab])',
    "glycemic_index_load": '("glycemic index"[tiab] OR "glycemic load"[tiab]) AND (diabetes[tiab] OR "glycemic control"[tiab])',
    "millets_indian_diet": '(millet*[tiab] OR "Indian diet"[tiab] OR "South Asian"[tiab]) AND (glycemic[tiab] OR diabetes[tiab] OR nutrition[tiab])',
    "low_carb_diet": '("low carbohydrate"[tiab] OR "low-carb"[tiab] OR ketogenic[tiab]) AND (diabetes[tiab] OR "weight loss"[tiab])',
    "intermittent_fasting": '("intermittent fasting"[tiab] OR "time-restricted eating"[tiab]) AND (weight[tiab] OR glucose[tiab] OR cardiometabolic[tiab])',
    "mediterranean_cvd": '"mediterranean diet"[tiab] AND (cardiovascular[tiab] OR mortality[tiab] OR diabetes[tiab])',
    "ultra_processed_food": '("ultra-processed"[tiab] OR "ultraprocessed"[tiab]) AND (health[tiab] OR mortality[tiab] OR obesity[tiab])',
    "obesity_weight_loss_diet": '(obesity[tiab] OR overweight[tiab]) AND ("weight loss"[tiab]) AND (diet[tiab] OR "energy restriction"[tiab])',
    "protein_sarcopenia": '(protein[tiab]) AND (sarcopenia[tiab] OR "muscle mass"[tiab]) AND (older[tiab] OR elderly[tiab])',
    "fibre_gut_health": '("dietary fiber"[tiab] OR "dietary fibre"[tiab]) AND (mortality[tiab] OR "glycemic control"[tiab] OR "gut microbiota"[tiab])',
    "omega3_cardiometabolic": '("omega-3"[tiab] OR "fish oil"[tiab]) AND (triglycerides[tiab] OR cardiovascular[tiab])',
    "thyroid_iodine_selenium": '(iodine[tiab] OR selenium[tiab]) AND (thyroid[tiab] OR hypothyroidism[tiab])',
    "calcium_bone_health": '(calcium[tiab] OR "vitamin D"[tiab]) AND (osteoporosis[tiab] OR fracture[tiab] OR "bone density"[tiab])',
    "sugar_sweetened_beverages": '("sugar-sweetened beverages"[tiab] OR "added sugar*"[tiab]) AND (diabetes[tiab] OR obesity[tiab])',
    "fortification_micronutrients_india": '(fortification[tiab] OR "micronutrient"[tiab]) AND (India[tiab] OR "low- and middle-income"[tiab])',
    "child_nutrition_india": '(India[tiab] OR "South Asia"[tiab]) AND (stunting[tiab] OR "child nutrition"[tiab] OR "complementary feeding"[tiab])',
}


# --------------------------------------------------------------------------- http utils
class PermanentHTTPError(Exception):
    """4xx (other than 408/429): retrying will not help."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _retry_after(resp: httpx.Response, default: float) -> float:
    try:
        return max(float(resp.headers.get("Retry-After", default)), 0.5)
    except ValueError:
        return default


def request_with_retry(
    client: httpx.Client,
    url: str,
    params: dict[str, Any] | None = None,
    retries: int = 4,
    backoff_sec: float = 1.5,
) -> httpx.Response:
    last = "unknown"
    for attempt in range(1, retries + 1):
        wait = backoff_sec * (2 ** (attempt - 1))
        try:
            resp = client.get(url, params=params)
            if resp.status_code in (408, 429) or resp.status_code >= 500:
                last = f"HTTP {resp.status_code}"
                wait = _retry_after(resp, wait)
            elif resp.status_code >= 400:
                raise PermanentHTTPError(f"HTTP {resp.status_code} for {resp.url}")
            else:
                return resp
        except httpx.HTTPError as exc:
            last = repr(exc)
        if attempt < retries:
            print(f"[RETRY] {url} ({last}); sleeping {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"failed after {retries} attempts: {url} ({last})")


def make_client(timeout: float = 60.0) -> httpx.Client:
    headers = {
        "User-Agent": f"NutriCite-RAG/1.1 (research; mailto:{CONTACT})",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en",
    }
    return httpx.Client(
        timeout=httpx.Timeout(timeout, connect=15.0), headers=headers, follow_redirects=True
    )


def load_json(path: Path, default: Any) -> Any:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return default


# --------------------------------------------------------------------------- html -> markdown
_STRIP_TAGS = ["script", "style", "noscript", "nav", "aside", "form", "iframe", "svg", "button"]
_MAIN_SELECTORS = ("main", "#main-content", "article", "[role=main]", "div.region-content")


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_main(soup: BeautifulSoup) -> Tag:
    best: Tag | None = None
    best_len = 0
    for sel in _MAIN_SELECTORS:
        for el in soup.select(sel):
            n = len(el.get_text(" ", strip=True))
            if n > best_len:
                best, best_len = el, n
    if best is None or best_len < 500:
        best = soup.body or soup
        for t in best.find_all(["header", "footer"]):
            if not getattr(t, "decomposed", False):
                t.decompose()
    for t in best.find_all(_STRIP_TAGS):
        if not getattr(t, "decomposed", False):
            t.decompose()
    return best


def _render_list(lst: Tag, level: int = 0) -> list[str]:
    lines: list[str] = []
    ordered = lst.name == "ol"
    for i, li in enumerate(lst.find_all("li", recursive=False), 1):
        nested = [x for x in li.find_all(["ul", "ol"]) if x.find_parent("li") is li]
        text = _clean(li.get_text(" ", strip=True))
        for n in nested:
            text = text.replace(_clean(n.get_text(" ", strip=True)), "")
        text = _clean(text)
        if text:
            marker = f"{i}." if ordered else "-"
            lines.append(f"{'  ' * level}{marker} {text}")
        for n in nested:
            lines.extend(_render_list(n, level + 1))
    return lines


def _render_table(table: Tag) -> list[str]:
    rows: list[list[str]] = []
    for tr in table.find_all("tr"):
        cells = [_clean(c.get_text(" ", strip=True)).replace("|", "/") for c in tr.find_all(["th", "td"])]
        if any(cells):
            rows.append(cells)
    if not rows:
        return []
    width = max(len(r) for r in rows)
    rows = [r + [""] * (width - len(r)) for r in rows]
    out = ["| " + " | ".join(rows[0]) + " |", "|" + "---|" * width]
    out += ["| " + " | ".join(r) + " |" for r in rows[1:]]
    return out


def html_to_markdown(html: str) -> tuple[str, str]:
    """Return (markdown, page_title)."""
    soup = BeautifulSoup(html, "lxml")
    title = _clean(soup.title.get_text(" ", strip=True)) if soup.title else ""
    root = _extract_main(soup)
    blocks: list[str] = []
    for el in root.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "ul", "ol", "table", "pre"]):
        if getattr(el, "decomposed", False):
            continue
        name = el.name
        if name in ("p", "pre") and el.find_parent(["li", "table"]):
            continue
        if name in ("ul", "ol") and el.find_parent(["ul", "ol", "table"]):
            continue
        if name == "table" and el.find_parent("table"):
            continue
        if name.startswith("h") and el.find_parent("table"):
            continue
        if name.startswith("h") and len(name) == 2:
            text = _clean(el.get_text(" ", strip=True))
            if text:
                blocks.append(f"{'#' * int(name[1])} {text}")
        elif name in ("ul", "ol"):
            lines = _render_list(el)
            if lines:
                blocks.append("\n".join(lines))
        elif name == "table":
            lines = _render_table(el)
            if lines:
                blocks.append("\n".join(lines))
        else:
            text = _clean(el.get_text(" ", strip=True))
            if text:
                blocks.append(text)
    deduped: list[str] = []
    for b in blocks:
        if not deduped or deduped[-1] != b:
            deduped.append(b)
    return "\n\n".join(deduped) + "\n", title


# --------------------------------------------------------------------------- web pages
def scrape_pages(client: httpx.Client, pages: list[Page], force: bool) -> None:
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = HTML_DIR / "_manifest.json"
    manifest: dict[str, Any] = load_json(manifest_path, {})

    for pg in pages:
        html_path = HTML_DIR / f"{pg.name}.html"
        md_path = CLEAN_DIR / f"{pg.name}.md"
        try:
            if html_path.exists() and html_path.stat().st_size > 0 and not force:
                if md_path.exists():
                    print(f"[SKIP] {pg.name} already scraped.")
                    continue
                html = html_path.read_text(encoding="utf-8")
                print(f"[CLEAN] {pg.name} (from cached html)")
            else:
                print(f"[GET] {pg.name} <- {pg.url}")
                html = request_with_retry(client, pg.url).text
                html_path.write_text(html, encoding="utf-8")
                time.sleep(0.8)

            md, title = html_to_markdown(html)
            if len(md) < 400:
                print(f"[WARN] {pg.name}: very little text extracted ({len(md)} chars) - JS-rendered or blocked?")
            front = (
                "---\n"
                f"source: {json.dumps(pg.url)}\n"
                f"title: {json.dumps(title, ensure_ascii=False)}\n"
                f"publisher: {json.dumps(pg.publisher)}\n"
                f"topic: {json.dumps(pg.topic)}\n"
                f"retrieved: {utc_now()}\n"
                "---\n\n"
            )
            md_path.write_text(front + md, encoding="utf-8")
            manifest[pg.name] = {
                "status": "ok",
                "url": pg.url,
                "title": title,
                "publisher": pg.publisher,
                "topic": pg.topic,
                "chars": len(md),
                "sha256": hashlib.sha256(html.encode("utf-8")).hexdigest(),
                "retrieved_at": utc_now(),
            }
            print(f"[OK] {md_path} ({len(md)} chars)")
        except Exception as exc:  # keep going; record failure
            print(f"[ERR] {pg.name}: {exc}")
            manifest[pg.name] = {"status": "failed", "url": pg.url, "error": str(exc), "tried_at": utc_now()}
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")


# --------------------------------------------------------------------------- PubMed
def _xml_parser() -> etree.XMLParser:
    return etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=True, recover=True)


def _text(el: etree._Element | None) -> str:
    return _clean("".join(el.itertext())) if el is not None else ""


def _pub_year(art: etree._Element, article: etree._Element) -> str:
    year = (article.findtext("./Journal/JournalIssue/PubDate/Year") or "").strip()
    if not year:
        medline_date = article.findtext("./Journal/JournalIssue/PubDate/MedlineDate") or ""
        m = re.search(r"(?:19|20)\d{2}", medline_date)
        year = m.group(0) if m else ""
    if not year:
        year = (article.findtext("./ArticleDate/Year") or "").strip()
    if not year:
        year = (art.findtext(".//PubMedPubDate[@PubStatus='pubmed']/Year") or "").strip()
    return year


def parse_pubmed_xml(xml: bytes, topic: str, require_abstract: bool = True) -> list[dict[str, Any]]:
    root = etree.fromstring(xml, parser=_xml_parser())
    records: list[dict[str, Any]] = []
    for art in root.findall(".//PubmedArticle"):
        article = art.find("./MedlineCitation/Article")
        if article is None:
            continue
        pmid = _text(art.find("./MedlineCitation/PMID"))

        parts: list[str] = []
        for ab in article.findall("./Abstract/AbstractText"):
            txt = _text(ab)
            if not txt:
                continue
            label = (ab.get("Label") or "").strip()
            parts.append(f"{label}: {txt}" if label else txt)
        abstract = "\n".join(parts)
        if require_abstract and not abstract:
            continue

        authors: list[str] = []
        for a in article.findall("./AuthorList/Author"):
            last, ini, coll = a.findtext("LastName"), a.findtext("Initials"), a.findtext("CollectiveName")
            if last:
                authors.append(f"{last} {ini or ''}".strip())
            elif coll:
                authors.append(coll.strip())

        records.append(
            {
                "pmid": pmid,
                "title": _text(article.find("ArticleTitle")),
                "abstract": abstract,
                "journal": _text(article.find("./Journal/Title")),
                "year": _pub_year(art, article),
                "authors": authors[:6],
                "doi": (art.findtext(".//PubmedData/ArticleIdList/ArticleId[@IdType='doi']") or "").strip(),
                "pmcid": (art.findtext(".//PubmedData/ArticleIdList/ArticleId[@IdType='pmc']") or "").strip(),
                "pub_types": [_text(x) for x in article.findall("./PublicationTypeList/PublicationType")],
                "mesh": [
                    _text(x) for x in art.findall("./MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName")
                ],
                "language": (article.findtext("./Language") or "").strip(),
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                "topic": topic,
                "source": "PubMed",
            }
        )
    return records


def ncbi_params(**kw: Any) -> dict[str, Any]:
    params: dict[str, Any] = {"tool": "nutricite", "email": CONTACT, **kw}
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    return params


def fetch_pubmed(client: httpx.Client, retmax: int, min_year: int, force: bool) -> None:
    PUBMED_DIR.mkdir(parents=True, exist_ok=True)
    XML_DIR.mkdir(parents=True, exist_ok=True)
    for name, term in PUBMED_TOPICS.items():
        json_dest = PUBMED_DIR / f"{name}.json"
        if json_dest.exists() and json_dest.stat().st_size > 2 and not force:
            print(f"[SKIP] pubmed/{json_dest.name} already exists.")
            continue
        try:
            print(f"[GET] PubMed search: {name}")
            search = request_with_retry(
                client,
                f"{EUTILS}/esearch.fcgi",
                params=ncbi_params(
                    db="pubmed",
                    term=f"({term}) AND {REVIEW_FILTER}",
                    retmax=retmax,
                    retmode="json",
                    sort="relevance",
                    datetype="pdat",
                    mindate=str(min_year),
                    maxdate="3000",
                ),
            )
            ids = search.json().get("esearchresult", {}).get("idlist", [])
            if not ids:
                print(f"[WARN] no PubMed results for {name}")
                continue
            time.sleep(NCBI_DELAY)
            fetched = request_with_retry(
                client,
                f"{EUTILS}/efetch.fcgi",
                params=ncbi_params(db="pubmed", id=",".join(ids), retmode="xml"),
            )
            (XML_DIR / f"{name}.xml").write_bytes(fetched.content)
            records = parse_pubmed_xml(fetched.content, name)
            json_dest.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"[OK] {len(records)}/{len(ids)} articles with abstracts -> {json_dest}")
        except Exception as exc:
            print(f"[ERR] PubMed topic {name}: {exc}")
        time.sleep(NCBI_DELAY)


def merge_pubmed() -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for path in sorted(PUBMED_DIR.glob("*.json")):
        for rec in load_json(path, []):
            pmid = rec.get("pmid")
            if not pmid:
                continue
            topic = rec.get("topic", path.stem)
            if pmid in merged:
                merged[pmid]["topics"] = sorted(set(merged[pmid]["topics"]) | {topic})
            else:
                item = {k: v for k, v in rec.items() if k != "topic"}
                item["topics"] = [topic]
                merged[pmid] = item
    out = PUBMED_DIR / "_all_pubmed.jsonl"
    with out.open("w", encoding="utf-8") as f:
        for rec in merged.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"[OK] {len(merged)} unique abstracts -> {out}")
    return merged


# --------------------------------------------------------------------------- PMC full text (JATS)
def _jats_text(el: etree._Element) -> str:
    """Text of a JATS element without bibliographic citation markers like '1, 2'."""
    parts = [el.text or ""]
    for child in el:
        if isinstance(child.tag, str):
            if not (child.tag == "xref" and child.get("ref-type") == "bibr"):
                parts.append(_jats_text(child))
        parts.append(child.tail or "")
    return "".join(parts)


def parse_pmc_xml(xml: bytes) -> dict[str, Any] | None:
    root = etree.fromstring(xml, parser=_xml_parser())
    art = root if root.tag == "article" else root.find(".//article")
    if art is None:
        return None
    body = art.find("body")
    if body is None:
        return None

    sections: list[dict[str, str]] = []
    loose: list[str] = []
    for child in body:
        if not isinstance(child.tag, str):
            continue
        if child.tag == "sec":
            paras = [_clean(_jats_text(p)) for p in child.iter("p")]
            paras = [p for p in paras if p]
            if paras:
                sections.append({"title": _text(child.find("title")) or "Section", "text": "\n\n".join(paras)})
        elif child.tag == "p":
            t = _clean(_jats_text(child))
            if t:
                loose.append(t)
    if loose:
        sections.insert(0, {"title": "Body", "text": "\n\n".join(loose)})

    meta = art.find("./front/article-meta")
    ids = {}
    if meta is not None:
        for aid in meta.findall("article-id"):
            ids[aid.get("pub-id-type") or ""] = (aid.text or "").strip()
    pmcid = ids.get("pmcid") or ids.get("pmc") or ""
    if pmcid and not pmcid.upper().startswith("PMC"):
        pmcid = f"PMC{pmcid}"
    return {
        "pmcid": pmcid,
        "pmid": ids.get("pmid", ""),
        "doi": ids.get("doi", ""),
        "title": _text(art.find(".//article-meta/title-group/article-title")),
        "abstract": _clean(_jats_text(art.find(".//article-meta/abstract"))) if art.find(".//article-meta/abstract") is not None else "",
        "sections": sections,
        "text_chars": sum(len(s["text"]) for s in sections),
    }


def pmc_to_markdown(doc: dict[str, Any], meta: dict[str, Any]) -> str:
    lines = [f"# {doc['title'] or meta.get('title', '')}", ""]
    bits = [f"PMCID: {doc['pmcid']}"]
    if doc.get("pmid"):
        bits.append(f"PMID: {doc['pmid']}")
    if meta.get("journal"):
        bits.append(f"Journal: {meta['journal']} ({meta.get('year', '')})")
    lines += [" | ".join(bits), ""]
    if doc["abstract"]:
        lines += ["## Abstract", "", doc["abstract"], ""]
    for s in doc["sections"]:
        lines += [f"## {s['title']}", "", s["text"], ""]
    return "\n".join(lines)


def fetch_pmc(client: httpx.Client, merged: dict[str, dict[str, Any]], pmc_max: int, force: bool) -> None:
    PMC_DIR.mkdir(parents=True, exist_ok=True)
    candidates = sorted(
        (r for r in merged.values() if r.get("pmcid")),
        key=lambda r: r.get("year") or "0",
        reverse=True,
    )
    saved = 0
    for rec in candidates:
        if saved >= pmc_max:
            break
        pmcid = rec["pmcid"]
        json_dest = PMC_DIR / f"{pmcid}.json"
        if json_dest.exists() and not force:
            if load_json(json_dest, {}).get("status") != "no_fulltext":
                saved += 1
            continue
        try:
            print(f"[GET] PMC full text {pmcid}")
            resp = request_with_retry(
                client, f"{EUTILS}/efetch.fcgi", params=ncbi_params(db="pmc", id=pmcid, retmode="xml")
            )
            doc = parse_pmc_xml(resp.content)
            if not doc or doc["text_chars"] < 3000:
                print(f"[SKIP] {pmcid}: no open-access body text")
                json_dest.write_text(json.dumps({"pmcid": pmcid, "status": "no_fulltext"}), encoding="utf-8")
                continue
            doc["pmcid"] = doc["pmcid"] or pmcid
            doc.update(
                {
                    "journal": rec.get("journal", ""),
                    "year": rec.get("year", ""),
                    "topics": rec.get("topics", []),
                    "url": f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/",
                }
            )
            json_dest.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
            (PMC_DIR / f"{pmcid}.md").write_text(pmc_to_markdown(doc, rec), encoding="utf-8")
            saved += 1
            print(f"[OK] {pmcid}: {doc['text_chars']} chars, {len(doc['sections'])} sections")
        except Exception as exc:
            print(f"[ERR] {pmcid}: {exc}")
        time.sleep(NCBI_DELAY)
    print(f"[OK] PMC full-text articles available: {saved}")


# --------------------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Scrape nutrition pages, PubMed abstracts and PMC full text.")
    ap.add_argument("--skip-pages", action="store_true")
    ap.add_argument("--skip-pubmed", action="store_true")
    ap.add_argument("--skip-pmc", action="store_true")
    ap.add_argument("--retmax", type=int, default=40, help="PubMed results per topic (max ~100)")
    ap.add_argument("--min-year", type=int, default=2012, help="earliest publication year for PubMed")
    ap.add_argument("--pmc-max", type=int, default=60, help="max PMC open-access full texts")
    ap.add_argument("--force", action="store_true", help="re-fetch even if outputs exist")
    args = ap.parse_args(argv)

    with make_client() as client:
        if not args.skip_pages:
            scrape_pages(client, build_pages(), args.force)
        if not args.skip_pubmed:
            fetch_pubmed(client, min(args.retmax, 100), args.min_year, args.force)
        merged = merge_pubmed() if PUBMED_DIR.exists() else {}
        if not args.skip_pmc and merged:
            fetch_pmc(client, merged, args.pmc_max, args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
