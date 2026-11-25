from pathlib import Path
import pypdfium2 as pdfium
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption

RAW_PDF_DIR = Path("data/raw/pdfs")
CLEAN_TEXT_DIR = Path("data/clean/text")

# Tabular food composition PDFs handled separately by clean_food_csv.py
SKIP_PDFS = {"ifct_2017.pdf"}
# Dense multi-hundred-page books that stall Docling layout inference on CPU
FAST_EXTRACT_PDFS = {"fao_who_vitamin_mineral_requirements_2004.pdf"}


def _build_converter() -> DocumentConverter:
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = False
    pipeline_options.do_table_structure = False
    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )


def _extract_fast_pdf(pdf_path: Path) -> str:
    pdf = pdfium.PdfDocument(str(pdf_path))
    pages: list[str] = [f"# {pdf_path.stem.replace('_', ' ').title()}\n"]
    for idx in range(len(pdf)):
        page = pdf[idx]
        textpage = page.get_textpage()
        txt = (textpage.get_text_range() or "").strip()
        if txt:
            pages.append(f"## Page {idx + 1}\n\n{txt}")
    return "\n\n".join(pages).strip() + "\n"


def parse_pdfs(force: bool = False) -> None:
    CLEAN_TEXT_DIR.mkdir(parents=True, exist_ok=True)
    converter = _build_converter()

    for pdf_path in sorted(RAW_PDF_DIR.glob("*.pdf")):
        if pdf_path.name in SKIP_PDFS:
            print(f"[SKIP-TABLE-PDF] {pdf_path.name}")
            continue

        out_path = CLEAN_TEXT_DIR / f"{pdf_path.stem}.md"
        if out_path.exists() and not force:
            print(f"[SKIP] {out_path.name}")
            continue

        tmp_path = out_path.with_suffix(".md.part")

        if pdf_path.name in FAST_EXTRACT_PDFS:
            print(f"[FAST-PARSE] {pdf_path.name} -> {out_path.name}...")
            md_text = _extract_fast_pdf(pdf_path)
        else:
            print(f"[DOCLING] {pdf_path.name} -> {out_path.name}...")
            result = converter.convert(str(pdf_path))
            md_text = result.document.export_to_markdown()

        tmp_path.write_text(md_text, encoding="utf-8")
        tmp_path.replace(out_path)
        print(f"[OK] {out_path}")


if __name__ == "__main__":
    parse_pdfs()