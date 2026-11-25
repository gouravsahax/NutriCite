import re
from pathlib import Path
import pandas as pd
import pdfplumber
from pydantic import BaseModel, Field

RAW_USDA_DIR = Path("data/raw/csv/usda")
RAW_IFCT_PDF = Path("data/raw/pdfs/ifct_2017.pdf")
CLEAN_CSV = Path("data/clean/csv/foods_clean.csv")

USDA_NUTRIENT_MAP = {
    1008: "calories_kcal",
    1003: "protein_g",
    1004: "fat_g",
    1005: "carbs_g",
    1079: "fiber_g",
    1093: "sodium_mg",
    1092: "potassium_mg",
    1087: "calcium_mg",
    1089: "iron_mg",
}


class FoodRecord(BaseModel):
    food_id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    name: str = Field(min_length=1)
    calories_kcal: float = Field(default=0.0, ge=0.0)
    protein_g: float = Field(default=0.0, ge=0.0)
    fat_g: float = Field(default=0.0, ge=0.0)
    carbs_g: float = Field(default=0.0, ge=0.0)
    fiber_g: float = Field(default=0.0, ge=0.0)
    sodium_mg: float = Field(default=0.0, ge=0.0)
    potassium_mg: float = Field(default=0.0, ge=0.0)
    calcium_mg: float = Field(default=0.0, ge=0.0)
    iron_mg: float = Field(default=0.0, ge=0.0)


def _to_float(val: object) -> float:
    if val is None:
        return 0.0
    cleaned = re.sub(r"[^0-9.\-]", "", str(val).split("±")[0].strip())
    try:
        num = float(cleaned)
        return max(num, 0.0)
    except ValueError:
        return 0.0


def load_usda_clean() -> pd.DataFrame:
    food_csv = RAW_USDA_DIR / "food.csv"
    fn_csv = RAW_USDA_DIR / "food_nutrient.csv"
    if not (food_csv.exists() and fn_csv.exists()):
        return pd.DataFrame()

    foods = pd.read_csv(food_csv, usecols=["fdc_id", "description"])
    fn = pd.read_csv(fn_csv, usecols=["fdc_id", "nutrient_id", "amount"])
    fn = fn[fn["nutrient_id"].isin(USDA_NUTRIENT_MAP.keys())].copy()
    fn["col"] = fn["nutrient_id"].map(USDA_NUTRIENT_MAP)

    pivoted = fn.pivot_table(
        index="fdc_id", columns="col", values="amount", aggfunc="first"
    ).reset_index()

    merged = foods.merge(pivoted, on="fdc_id", how="inner").fillna(0.0)
    merged["food_id"] = "usda_" + merged["fdc_id"].astype(str)
    merged["source"] = "USDA"
    merged.rename(columns={"description": "name"}, inplace=True)

    for col in USDA_NUTRIENT_MAP.values():
        if col not in merged.columns:
            merged[col] = 0.0

    cols = ["food_id", "source", "name", *USDA_NUTRIENT_MAP.values()]
    return merged[cols]


def extract_ifct_clean() -> pd.DataFrame:
    if not RAW_IFCT_PDF.exists():
        return pd.DataFrame()

    records: list[dict[str, object]] = []
    code_pattern = re.compile(r"^[A-Z]\d{3}$")

    with pdfplumber.open(RAW_IFCT_PDF) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                for row in table:
                    if not row or len(row) < 6:
                        continue
                    code = (row[0] or "").strip()
                    if not code_pattern.match(code):
                        continue
                    name = re.sub(r"\s+", " ", (row[1] or "").strip())
                    if not name:
                        continue
                    records.append({
                        "food_id": f"ifct_{code}",
                        "source": "IFCT_2017",
                        "name": name,
                        "protein_g": _to_float(row[3]) if len(row) > 3 else 0.0,
                        "fat_g": _to_float(row[5]) if len(row) > 5 else 0.0,
                        "fiber_g": _to_float(row[6]) if len(row) > 6 else 0.0,
                        "carbs_g": _to_float(row[7]) if len(row) > 7 else 0.0,
                        "calories_kcal": _to_float(row[8]) if len(row) > 8 else 0.0,
                        "sodium_mg": 0.0,
                        "potassium_mg": 0.0,
                        "calcium_mg": 0.0,
                        "iron_mg": 0.0,
                    })

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records).drop_duplicates(subset=["food_id"])


def clean_food_csv() -> None:
    usda_df = load_usda_clean()
    ifct_df = extract_ifct_clean()

    combined = pd.concat([usda_df, ifct_df], ignore_index=True)
    if combined.empty:
        return

    validated = [FoodRecord(**row).model_dump() for row in combined.to_dict(orient="records")]
    CLEAN_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(validated).to_csv(CLEAN_CSV, index=False)
    print(f"[OK] {CLEAN_CSV}")


if __name__ == "__main__":
    clean_food_csv()
