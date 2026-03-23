# Thesis References

Academic papers, industry standards, and technical sources used to justify design decisions in the DQ Engineering Pipeline.

Organised by topic. Each entry includes: source, URL, and the decision it supports.

---

## 1. Data Quality Standards and Frameworks

### DAMA DMBOK / ISO/IEC 25012 — Data Quality Dimensions

**Source:** DAMA DMBOK (Data Management Body of Knowledge) — six practitioner DQ dimensions: completeness, validity, uniqueness, consistency, accuracy, timeliness. ISO/IEC 25012 defines 15 data quality characteristics; this project implements four of them (completeness, uniqueness, validity, consistency).

> **Note on ISO 8000:** ISO 8000 governs data quality for supply-chain master data (GDSN product records, asset registers) and is not applicable to general-purpose CSV profiling. An earlier version of this project incorrectly cited ISO 8000 alignment — that has been corrected.

- [ISO 8000-1:2022 Overview](https://www.iso.org/obp/ui/#iso:std:iso:8000:-1:ed-1:v1:en)
- [ISO 8000 Wikipedia summary](https://en.wikipedia.org/wiki/ISO_8000)
- [Data Quality Dimensions — Soda.io practical guide](https://soda.io/blog/guide-to-data-quality-dimensions)
- [Comparison of DQ Frameworks — MDPI 2025](https://www.mdpi.com/2504-2289/9/4/93)
- [EWSolutions — Complete 2025 DQ Guide](https://www.ewsolutions.com/data-quality-quide/)

**Supports:** Engineering Decision #37 — structuring the recommendation framework around DAMA DMBOK / ISO/IEC 25012 dimensions rather than an ad-hoc taxonomy.

---

### Business Rules vs Data Quality Rules

**Source:** Data Quality Pro interview with Ronald G. Ross; Profisee DQ rules guide.

- [Business Rules for Data Quality — Data Quality Pro](https://www.dataqualitypro.com/blog/business-rules-for-data-quality-ronald-g-ross)
- [Data Quality Rules Examples — Profisee](https://profisee.com/blog/data-quality-rules/)
- [Clarifying Business Rules vs Data Quality Rules — LinkedIn](https://www.linkedin.com/pulse/clarifying-business-rules-vs-data-quality-rules-sam-benes-xs68e)
- [Semantic Quality — Business Rules Community](https://www.brcommunity.com/articles.php?id=c140)

**Supports:** The distinction between deterministic rules (code) and semantic judgments (LLM) — Engineering Decision #38.

---

## 2. Missing Data — Mechanisms and Imputation

### MCAR / MAR / MNAR — The Three Missing Data Mechanisms

**Source:** van Buuren (2018), "Flexible Imputation of Missing Data" — the canonical reference for the three-mechanism taxonomy.

- [Flexible Imputation of Missing Data — van Buuren (free online)](https://stefvanbuuren.name/fimd/sec-MCAR.html)
- [MCAR/MAR/MNAR — apxml.com course chapter](https://apxml.com/courses/intro-feature-engineering/chapter-2-handling-missing-data/missing-data-mechanisms)
- [Missing Data — Wikipedia](https://en.wikipedia.org/wiki/Missing_data)
- [Managing Missing Data in Patient Registries — NCBI](https://www.ncbi.bookshelf.ncbi.nlm.nih.gov/books/NBK493614/)
- [Missing Data Evaluation — Bookdown MI Guide](https://bookdown.org/mwheymans/bookmi/missing-data-evaluation.html)
- [Statistical Test for MCAR in Python — Towards Data Science](https://towardsdatascience.com/statistical-test-for-mcar-in-python-9fb617a76eac/)
- [A Comprehensive Review of Special Missing Mechanisms — arXiv 2024](https://arxiv.org/html/2404.04905v1)

**Supports:**
- The `leave_null` strategy for MAR-detected columns (Engineering Decision #37, #38)
- The rationale that MAR detection belongs in code (statistical), MNAR detection belongs in LLM (semantic)

---

### Imputation Strategy Selection — Mean, Median, Mode

**Source:** Feature Engine documentation; scikit-learn imputation guide; various ML education sources.

- [Mean/Median Imputation — Feature Engine docs](https://feature-engine.trainindata.com/en/1.8.x/user_guide/imputation/MeanMedianImputer.html)
- [Imputation of Missing Values — scikit-learn](https://scikit-learn.org/stable/modules/impute.html)
- [When to Use Mean vs Median — apxml.com](https://apxml.com/courses/intro-data-cleaning-preprocessing/chapter-2-handling-missing-data/basic-imputation-mean-median-mode)
- [Handling Missing Data — Shiksha Online](https://www.shiksha.com/online-courses/articles/handling-missing-data-mean-median-mode/)

**Supports:**
- Median for skewed numeric columns (outliers present), mean for symmetric — Engineering Decision #38
- The baseline recommendation engine's strategy selection rules (docs/recommendations.md)

---

### When to Drop Columns vs Impute — Threshold Research

**Source:** Multiple practitioner guides agree on ~80% missing as the threshold for column removal.

- [Comprehensive Guide on Handling Missing Values — Medium/ByCodeGarage](https://medium.com/bycodegarage/a-comprehensive-guide-on-handling-missing-values-b1257a4866d1)
- [Why You Should Handle Missing Data — Towards Data Science](https://towardsdatascience.com/why-you-should-handle-missing-data-and-heres-how-to-do-it-270c321a4d6f/)
- [Handling Missing Values in Machine Learning — GeeksforGeeks](https://www.geeksforgeeks.org/machine-learning/handling-missing-values-machine-learning/)

**Supports:** The 50% threshold for `drop_column` in `_fill_strategy()` — the pipeline uses a single threshold at ≥50% null. The ~80% figure cited in these sources represents an extreme case that is already covered by the 50% rule and is not a separate threshold in the implementation.

---

### LLM-Based Imputation — Recent Research (2024–2025)

**Source:** Recent academic work on using LLMs for tabular data imputation.

- [LLM-Forest for Health Tabular Data Imputation — arXiv 2024](https://arxiv.org/html/2410.21520v1)
- [Missing Value Imputation via Pre-trained LMs — VLDB 2024](https://vldb.org/workshops/2024/proceedings/DATAI/DATAI-2.pdf)
- [Context-Driven Missing Data Imputation via LLM — OpenReview](https://openreview.net/forum?id=b2oLgk5XRE)
- [Does Prompt Design Impact Quality of Data Imputation by LLMs? — arXiv 2025](https://arxiv.org/html/2506.04172v1)
- [On LLM-Enhanced Mixed-Type Data Imputation — VLDB 2025](https://www.vldb.org/pvldb/vol18/p3421-wang.pdf)
- [Data Wrangling Task Automation Using Code-Generating LLMs — AAAI 2025](https://ojs.aaai.org/index.php/AAAI/article/view/35344/37499)

**Supports:** The decision to use an LLM for semantic decisions (MNAR detection, rename suggestions) rather than value imputation — Engineering Decision #38. The research confirms LLMs excel at context-driven semantic decisions, not at reading tabular statistics.

---

## 3. Outlier Detection and Treatment

**Source:** Standard statistical methods — IQR (Tukey, 1977) and Z-score for outlier detection.

- [Outlier Detection — Z-score, IQR, and Robust Methods — Medium](https://medium.com/@aakash013/outlier-detection-treatment-z-score-iqr-and-robust-methods-398c99450ff3)
- [Outlier Detection and Treatment — certometer.com](https://www.certometer.com/blogs/machine-learning/outlier-detection-and-treatment)
- [IQR Method — Penn State STAT 200](https://online.stat.psu.edu/stat200/lesson/3/3.2)
- [Outlier Detection: IQR, Z-score, LOF, DBSCAN — Analytics Vidhya](https://www.analyticsvidhya.com/blog/2022/10/outliers-detection-using-iqr-z-score-lof-and-dbscan/)
- [A Practical Guide to Outlier Detection — Medium](https://medium.com/@inkollusrivarsha0287/a-practical-guide-to-outlier-detection-and-treatment-in-data-science-43e853614fe8)

**Supports:**
- Engineering Decision #9 — IQR chosen as the default outlier detection method (more robust than Z-score for skewed real-world data)
- The outlier treatment options (winsorise, remove, cap, keep) in docs/recommendations.md
- The distinction between statistical outliers and domain validity violations (age = -1 is invalid, not an outlier)

### Outlier Treatment Strategy Selection — Heuristic Thresholds

**Source:** Practitioner guides on winsorization and outlier prevalence as a signal for treatment choice.

- [How to Identify Outliers in Your Data — Statistics By Jim](https://statisticsbyjim.com/basics/outliers/)
- [How to Find Outliers — Statistics By Jim](https://statisticsbyjim.com/basics/find-outliers/)
- [Winsorization — DataCamp Data Prep guide](https://www.datacamp.com/tutorial/how-to-use-the-winsorize-function-in-python)
- [IQR Outlier Detection — Analytics Vidhya](https://www.analyticsvidhya.com/blog/2022/10/outliers-detection-using-iqr-z-score-lof-and-dbscan/)
- [Dealing with Outliers Using the Z-score Method — KDnuggets](https://www.kdnuggets.com/2017/02/removing-outliers-standard-deviation-python.html)

**Key finding:** No academic standard specifies numeric thresholds (e.g. "remove if <1%") for outlier treatment selection. The thresholds used in `_outlier_strategy()` (<1% → remove, 1–5% → winsorise, >5% → keep) are grounded in general practitioner guidance: small counts at extreme bounds are typical of data entry errors (favour removal), moderate prevalence warrants capping to preserve rows (winsorise), and high prevalence suggests a naturally heavy-tailed distribution (keep). These thresholds are disclosed as project heuristics, not a formal statistical rule.

**Supports:**
- Engineering Decision #71 — outlier strategy heuristic thresholds and the hybrid code-heuristic + LLM domain note design
- `_outlier_strategy()` in `dq_logic.py`

---

## 4. Duplicate Detection

**Source:** Research on exact and fuzzy/near duplicate detection in data warehouses.

- [Eliminating Fuzzy Duplicates in Data Warehouses — VLDB 2002](https://www.vldb.org/conf/2002/S17P01.pdf)
- [Identifying Duplicates in Health Data — Medium](https://medium.com/@tarangds/identifying-duplicates-and-near-duplicates-in-health-data-a-guide-for-data-professionals-9b085b6a4138)
- [Fuzzy Data Deduplication — LatentView Analytics](https://www.latentview.com/blog/understanding-fuzzy-data-deduplication/)
- [Fuzzy Matching 101 — Data Ladder](https://dataladder.com/fuzzy-matching-101/])

**Supports:**
- The current scope: exact duplicate detection only (Engineering Decision #37 gap analysis)
- Fuzzy deduplication is documented as a known gap requiring Levenshtein/Soundex/DBSCAN — future work

---

## 5. Sentinel / Invalid Values

**Source:** Academic definition of sentinel values; pandas and vaex documentation on invalid vs missing data; IoT sensor literature; general DQ practice.

- [Sentinel Value — Wikipedia](https://en.wikipedia.org/wiki/Sentinel_value)
- [Handling Missing or Invalid Data — vaex docs](https://vaex.readthedocs.io/en/latest/guides/missing_or_invalid_data.html)
- [Handling Missing Data — O'Reilly](https://www.oreilly.com/content/handling-missing-data/)
- [Missing Value Imputation of Wireless Sensor Data — MDPI Sensors 2024](https://www.mdpi.com/1424-8220/24/8/2416)
- [Missing Data Imputation in IoT Sensor Networks — MDPI Future Internet 2022](https://www.mdpi.com/1999-5903/14/5/143)
- [Imputation of Missing Values — scikit-learn docs](https://scikit-learn.org/stable/modules/impute.html)
- [Data Cleaning and Imputation — CS5702 Modern Data Book](https://bookdown.org/martin_shepperd/ModernDataBook/C5-Cleaning.html)
- [DAMA-DMBOK Framework Overview](https://www.damadmbok.org/copy-of-about-dama-dmbok)

**Key finding on DMBOK scope:** DAMA DMBOK is a governance framework — it defines DQ dimensions and management processes, not specific cleansing algorithms. It does not prescribe a procedure for sentinel value handling. The separation of "validity cleansing" (sentinel → null) from "imputation" (null → fill value) is established general data science practice, consistent with DMBOK's validity and completeness dimensions but not derived from a specific DMBOK rule.

**Supports:**
- The distinction between missing data (null) and invalid data (sentinel values stored as real values like `-999`, `"N/A"`) — these require different treatment paths (docs/recommendations.md, Section 2 — Validity).
- Sentinel values are primarily a **validity** violation (value is outside the column's valid domain), and secondarily a **completeness** issue (they mask true "no data" state). Classification from DMBOK Ch. 13 DQ dimensions.
- The correct processing order: detect out-of-domain values during profiling → replace with proper null in a dedicated cleansing step → then apply the imputation strategy. The IoT sensor literature and scikit-learn both treat these as distinct pipeline stages.
- For columns with both real nulls AND sentinel values: both become NaN after sentinel replacement, and the imputation strategy applies uniformly. The missing data mechanism (MCAR/MAR/MNAR) should be assessed from the true nulls only, not from sentinel-encoded missing — this is why MAR detection runs on the original data before sentinel replacement in apply_recommendations().

---

## 6. Normalization and Standardisation

**Source:** Standard preprocessing references; Sebastian Raschka's feature scaling article; DataCamp guides.

- [About Feature Scaling and Normalization — Sebastian Raschka](https://sebastianraschka.com/Articles/2014_about_feature_scaling.html)
- [Normalization vs Standardization — DataCamp](https://www.datacamp.com/tutorial/normalization-vs-standardization)
- [Min-Max and Z-Score Normalization — Codecademy](https://www.codecademy.com/article/min-max-zscore-normalization)
- [Z-Score Normalization for Hybrid Search — OpenSearch](https://opensearch.org/blog/introducing-the-z-score-normalization-technique-for-hybrid-search/)

**Supports:**
- Normalization applies beyond ML: cross-feature comparison, clustering, PCA, multi-source standardization — docs/recommendations.md
- Min-max for bounded distributions with few outliers; Z-score when outliers are present or distribution is unknown

---

## 7. Column Naming and Metadata Quality

**Source:** Metadata management best practices; data catalog standards (Dublin Core, DCAT).

- [Metadata Management Best Practices — Alation](https://www.alation.com/blog/metadata-management-best-practices/)
- [How to Standardize Metadata Naming Conventions — LinkedIn](https://www.linkedin.com/advice/0/how-can-you-standardize-metadata-naming-conventions-slnhc)
- [Metadata Quality — lakeFS](https://lakefs.io/blog/metadata-quality/)

**Supports:** Column renaming as a metadata quality improvement — naming conventions improve discoverability and self-documentation of datasets (docs/recommendations.md, Section 7).

---

## 8. Tech Stack Choices

### Workflow Orchestration — Prefect vs Airflow

- [Prefect 3 Documentation](https://docs.prefect.io/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

**Supports:** Engineering Decision #1 — Prefect 3 chosen over Airflow for lower operational overhead and Python-native task definition. The original project spec named Airflow; the switch is justified by development speed for a research prototype.

### Object Storage — MinIO

- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

**Supports:** Engineering Decision #4 — MinIO chosen as an S3-compatible object store for raw and curated file zones.

### LLM Provider — Groq + Llama

- [Groq API Documentation](https://console.groq.com/docs/openai)
- [Llama 3.3 70B — Meta AI](https://ai.meta.com/blog/meta-llama-3/)

**Supports:** Engineering Decision #31 — Groq chosen for sub-second inference latency and zero cost during development. Llama 3.3 70B chosen for strong instruction following on structured JSON output tasks.

---

## 9. LLM Prompting and Reliability

- [Does Prompt Design Impact Quality of Data Imputation by LLMs? — arXiv 2025](https://arxiv.org/html/2506.04172v1)
- [Design Space for Critical Validation of LLM Tabular Data — EuroVA 2025](https://diglib.eg.org/bitstream/handle/10.2312/eurova20251101/eurova20251101.pdf)
- [Optimized Feature Generation for Tabular Data via LLMs — NeurIPS 2024](https://proceedings.neurips.cc/paper_files/paper/2024/file/a7ebe2e8d8cfd2fcec6cd77f9e6fd34d-Paper-Conference.pdf)

**Supports:**
- The partial diff format (LLM returns only changes) — Engineering Decision #33
- Post-processing guards as a reliability layer over probabilistic LLM output — Engineering Decision #34
- The narrowed LLM scope: semantic decisions only, not statistical computations — Engineering Decision #38

---

## 10. LLM Task Decomposition and Model Cascading

**Source:** Research on splitting complex LLM tasks into smaller specialised sub-calls, and on routing simpler queries to smaller models.

- [How Task Decomposition and Smaller LLMs Can Make AI More Affordable — Amazon Science (2024)](https://www.amazon.science/blog/how-task-decomposition-and-smaller-llms-can-make-ai-more-affordable)
- [An Approach for Systematic Decomposition of Complex LLM Tasks — arXiv:2510.07772 (2025)](https://arxiv.org/html/2510.07772v1)
- [A Unified Approach to Routing and Cascading for LLMs — arXiv:2410.10347 (ICLR 2025)](https://arxiv.org/abs/2410.10347)
- [Dynamic Model Routing and Cascading for Efficient LLM Inference: A Survey — arXiv:2603.04445 (2026)](https://arxiv.org/html/2603.04445)
- [ADaPT: As-Needed Decomposition and Planning with Language Models — Allen AI (2024)](https://allenai.github.io/adaptllm/)

**Key findings:**
- Amazon Science (2024): "task decomposition using multiple smaller focused LLM calls can match or exceed the performance of a single large call while reducing cost."
- ADaPT (Allen AI): recursive task decomposition improves success rates by up to 33% over single-call approaches across three benchmarks.
- arXiv:2410.10347: routing simpler queries to smaller models achieves comparable accuracy at roughly half the inference cost; cascade routing outperforms both routing-only and cascading-only strategies.

**Supports:** Engineering Decision #61 — splitting the LLM enrichment call into a strategy-only call and a separate rename-only call, with simultaneous model downgrade from `llama-3.3-70b-versatile` (70B) to `llama-4-scout-17b-16e-instruct` (17B). The rename task is a pattern-matching classification that does not require the reasoning depth of the 70B model; keeping a single model for both calls maintains consistency while the task decomposition recovers quality lost when both concerns competed in one prompt.

## 11. LLM Non-Determinism
https://thinkingmachines.ai/blog/defeating-nondeterminism-in-llm-inference/
