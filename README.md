# 🥗 NutriCite

**NutriCite** is an evidence-based Nutrition & Diet Assistant. It combines exact food macro calculations with cited clinical guidelines so users get accurate dietary advice without LLM hallucinations.

---

## Step 1: Data Ingestion Architecture

![NutriCite Data Ingestion Pipeline](docs/ingestion-pipeline.png)

### 📚 What Data We Gather
* **Clinical Guidelines (Unstructured $\rightarrow$ Qdrant):**
  * **ICMR-NIN 2024 (PDF):** Official Indian dietary guidelines, balanced plates, and RDAs.
  * **NIH ODS Fact Sheets (HTML):** Micronutrient dosages, deficiencies, and upper limits.
  * **PubMed Central (XML):** Meta-analyses on Type 2 Diabetes, Hypertension, and PCOS.
* **Food & Nutrition Data (Structured $\rightarrow$ PostgreSQL):**
  * **USDA FoodData Central (CSV):** Exact macro and micronutrient values for global foods.
  * **IFCT 2017 (PDF Tables):** Exact nutrient composition for 528+ Indian foods and spices.

### 🔀 Why Dual-Track (PostgreSQL + Qdrant)?
Vector databases are terrible at exact math. Instead of dumping calorie tables into a vector store, NutriCite splits the pipeline:
* **PostgreSQL** handles exact numbers (calories, protein, sodium, allergens) via deterministic SQL queries.
* **Qdrant** handles unstructured medical text for clinical reasoning and cited explanations.

### 🔍 Why Hybrid Search (Vector Embeddings + BM25)?
Every clinical text chunk is indexed with both methods in Qdrant:
* **Vector Embeddings (Dense)** understand meaning and synonyms (e.g., *"high blood sugar"* matches *"glycemic control"*).
* **BM25 (Sparse)** locks onto exact medical terms, biomarkers, and drug names (e.g., *"HbA1c"*, *"Vitamin B12"*, *"Metformin"*).