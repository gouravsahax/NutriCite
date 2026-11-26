## Step 1: Contextual Hybrid Indexing

![Contextual Hybrid Indexing Pipeline](<docs/Contextual-Hybrid-Indexing-Pipeline.png>)

### 🔀 Why Dual-Track (PostgreSQL + Qdrant)?
Vector databases are terrible at exact math. Instead of dumping calorie tables into a vector store, NutriCite splits the pipeline:
* **PostgreSQL** handles exact numbers (calories, protein, sodium, allergens) via deterministic SQL queries.
* **Qdrant** handles unstructured medical text for clinical reasoning and cited explanations.

### 🔍 Why Hybrid Search (Vector Embeddings + BM25)?
Every clinical text chunk is indexed with both methods in Qdrant:
* **Vector Embeddings (Dense)** understand meaning and synonyms (e.g., *"high blood sugar"* matches *"glycemic control"*).
* **BM25 (Sparse)** locks onto exact medical terms, biomarkers, and drug names (e.g., *"HbA1c"*, *"Vitamin B12"*, *"Metformin"*).
