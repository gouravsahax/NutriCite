import json
from pathlib import Path
from bs4 import BeautifulSoup
from lxml import etree

RAW_HTML_DIR = Path("data/raw/html")
RAW_XML_DIR = Path("data/raw/xml")
RAW_PUBMED_DIR = Path("data/raw/pubmed")
CLEAN_TEXT_DIR = Path("data/clean/text")


def parse_nih_htmls(force: bool = False) -> None:
    CLEAN_TEXT_DIR.mkdir(parents=True, exist_ok=True)

    for html_path in sorted(RAW_HTML_DIR.glob("*.html")):
        out_path = CLEAN_TEXT_DIR / f"nih_{html_path.stem}.md"
        if out_path.exists() and not force:
            print(f"[SKIP] {out_path.name}")
            continue

        soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "lxml")
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        main = soup.find("main") or soup.body
        if main is None:
            continue

        lines: list[str] = []
        for el in main.find_all(["h1", "h2", "h3", "p", "li", "table"]):
            if el.name == "table":
                rows = []
                for tr in el.find_all("tr"):
                    cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
                    if any(cells):
                        rows.append("| " + " | ".join(cells) + " |")
                if len(rows) >= 2:
                    col_count = rows[0].count("|") - 1
                    sep = "| " + " | ".join(["---"] * max(col_count, 1)) + " |"
                    lines.extend(["", rows[0], sep, *rows[1:], ""])
                continue

            if el.find_parent("table") is not None:
                continue

            text = el.get_text(" ", strip=True)
            if not text:
                continue
            if el.name == "h1":
                lines.append(f"# {text}")
            elif el.name == "h2":
                lines.append(f"\n## {text}")
            elif el.name == "h3":
                lines.append(f"\n### {text}")
            elif el.name == "li":
                lines.append(f"- {text}")
            else:
                lines.append(text)

        out_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        print(f"[OK] {out_path}")


def _pubmed_records_from_xml(xml_path: Path) -> list[dict[str, str]]:
    root = etree.fromstring(xml_path.read_bytes())
    records: list[dict[str, str]] = []
    for article in root.findall(".//PubmedArticle"):
        pmid = (article.findtext(".//MedlineCitation/PMID") or "").strip()
        title_el = article.find(".//Article/ArticleTitle")
        title = "".join(title_el.itertext()).strip() if title_el is not None else ""
        abs_parts = []
        for abs_el in article.findall(".//Article/Abstract/AbstractText"):
            label = (abs_el.get("Label") or "").strip()
            txt = "".join(abs_el.itertext()).strip()
            if txt:
                abs_parts.append(f"**{label}:** {txt}" if label else txt)
        journal = (article.findtext(".//Article/Journal/Title") or "").strip()
        year = (
            article.findtext(".//Article/Journal/JournalIssue/PubDate/Year")
            or article.findtext(".//Article/Journal/JournalIssue/PubDate/MedlineDate")
            or ""
        ).strip()
        records.append({
            "pmid": pmid,
            "title": title,
            "abstract": "\n".join(abs_parts),
            "journal": journal,
            "year": year,
        })
    return records


def parse_pubmed(force: bool = False) -> None:
    CLEAN_TEXT_DIR.mkdir(parents=True, exist_ok=True)
    sources = sorted(RAW_PUBMED_DIR.glob("*.json")) or sorted(RAW_XML_DIR.glob("*.xml"))

    for src_path in sources:
        out_path = CLEAN_TEXT_DIR / f"pubmed_{src_path.stem}.md"
        if out_path.exists() and not force:
            print(f"[SKIP] {out_path.name}")
            continue

        if src_path.suffix == ".json":
            records = json.loads(src_path.read_text(encoding="utf-8"))
        else:
            records = _pubmed_records_from_xml(src_path)

        lines = [f"# PubMed Evidence: {src_path.stem.replace('_', ' ').title()}\n"]
        for rec in records:
            pmid = rec.get("pmid", "")
            title = rec.get("title", "Untitled")
            journal = rec.get("journal", "")
            year = rec.get("year", "")
            abstract = rec.get("abstract", "")
            lines.append(f"## {title}")
            lines.append(f"- **PMID:** {pmid} | **Journal:** {journal} ({year})")
            lines.append(abstract)
            lines.append("")

        out_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        print(f"[OK] {out_path}")


def parse_all_web(force: bool = False) -> None:
    parse_nih_htmls(force=force)
    parse_pubmed(force=force)


if __name__ == "__main__":
    parse_all_web()
