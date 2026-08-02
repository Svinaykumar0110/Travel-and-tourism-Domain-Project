# ClientX.com — Analytics Enablement Program
## Technical Architecture & Engineering Document

| | |
|---|---|
| **Program Name** | ClientX Unified Analytics Platform (MUAP) |
| **Document Type** | Technical / Engineering Design Document |
| **Prepared For** | ClientX.com Data Engineering, Platform, Security & Compliance Teams |
| **Version** | 1.0 |
| **Date** | 2025-07-22 |
| **Companion Document** | `01_Business_and_Core_Document.md`, `03_ETL_Dataflow_Architecture.md` |
| **Core Technology Stack** | Snowflake (cloud data platform), Apache Airflow (orchestration), dbt (transformation), Cloud Object Storage (staging), 3 structured source systems |

---

## 1. Purpose & How to Read This Document

This document is the **single technical source of truth** for how the ClientX Unified Analytics Platform (MUAP) is engineered. It is written so a new data engineer, DBA, or platform SRE joining the project can answer, unaided:

- **WHAT** exists — every Snowflake object, every Airflow DAG, every schema, every data quality rule.
- **WHERE** it lives — which Snowflake database/schema/warehouse, which storage bucket, which Airflow environment.
- **WHEN** it runs — exact schedule, dependency order, SLAs.
- **HOW** it works — extraction, staging, loading, transformation, orchestration, incremental logic, backfill logic, security enforcement, monitoring.
- **WHY** each decision was made — the rationale behind Snowflake feature choices, warehouse sizing, schema design, and orchestration patterns.
- **WHO** owns/operates each component.

Section numbering is kept stable across revisions so it can be referenced in tickets/PRs (e.g., "see §6.3 for backfill logic").

---

## 2. Architecture Principles (Why These Choices Were Made)

| Principle | Rationale |
|---|---|
| **ELT over ETL** | Land raw data in Snowflake first, transform inside Snowflake using its compute — avoids a separate transformation cluster, leverages Snowflake's elastic compute, and preserves raw history for re-processing/auditing. |
| **Medallion layering (Raw → Staging/Bronze → Curated/Silver → Mart/Gold)** | Clean separation of concerns: raw immutability, standardized cleansing, conformed business entities, and consumption-ready marts — each layer independently testable and re-runnable. |
| **Batch, not streaming** | Business SLA is 4 hours (§Business Doc §6); all 3 sources are structured/relational batch-friendly systems. Streaming (Kafka/Snowpipe Streaming) would add operational cost/complexity disproportionate to the actual business need. Architecture is designed so a future move to micro-batch/streaming for a specific source is additive, not a rewrite. |
| **Idempotent, re-runnable pipelines** | Every load (incremental or backfill) must produce the same result if run twice for the same window — critical for reliability and safe backfills. |
| **Config-driven, metadata-driven pipeline design** | One generic Airflow DAG factory + per-source YAML/JSON config, rather than 3 hand-built bespoke DAGs — reduces maintenance burden and onboarding time for a future 4th source. |
| **Security enforced in the data platform, not the BI tool** | Masking, row-access, and RBAC are implemented as Snowflake policies so protection holds regardless of which BI/reporting tool queries the warehouse. |
| **Cost-aware compute isolation** | Dedicated, right-sized virtual warehouses per workload (ingestion, transformation, BI) so a heavy dashboard query never competes with — or is blocked by — a pipeline load, and each is independently monitored for credit consumption. |

---

## 3. High-Level Architecture Overview

```
 ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐
 │  SOURCE 1           │   │  SOURCE 2           │   │  SOURCE 3           │
 │  OTA Booking DB     │   │  ClientX Business    │   │  Retail / POS DB     │
 │  (PostgreSQL/MySQL) │   │  (TMC) DB            │   │  (branch systems)    │
 └─────────┬──────────┘   └─────────┬──────────┘   └─────────┬──────────┘
           │  batch extract (JDBC/DB replica), every 4h        │
           ▼                        ▼                        ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │        CLOUD STAGING (Object Storage: S3 / Azure Blob / ADLS)      │
 │        Partitioned by source/entity/extract_date/batch_id           │
 │        Format: Parquet (compressed, columnar)                       │
 └───────────────────────────────┬──────────────────────────────────┘
                                  │  Snowflake external stage + COPY INTO
                                  ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │  SNOWFLAKE — RAW LAYER  (DB: MUSAFIR_RAW)                           │
 │  1:1 with source, append-only, VARIANT + typed columns, load metadata │
 └───────────────────────────────┬──────────────────────────────────┘
                                  │  Streams + Tasks / dbt (incremental)
                                  ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │  SNOWFLAKE — STAGING / BRONZE LAYER (DB: MUSAFIR_STAGING)           │
 │  Deduplicated, typed, standardized, source-system-shaped            │
 └───────────────────────────────┬──────────────────────────────────┘
                                  │  dbt transformations
                                  ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │  SNOWFLAKE — CURATED / SILVER LAYER (DB: MUSAFIR_CURATED)           │
 │  Conformed entities: customer, booking, payment, supplier, branch    │
 │  Identity resolution, business rules, SCD Type 2 dimensions          │
 └───────────────────────────────┬──────────────────────────────────┘
                                  │  dbt transformations
                                  ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │  SNOWFLAKE — MART / GOLD LAYER (DB: MUSAFIR_MART)                    │
 │  Star-schema facts & dimensions per business domain (§8)             │
 └───────────────────────────────┬──────────────────────────────────┘
                                  │  RBAC + masking + row-access policies
                                  ▼
                 ┌───────────────────────────────┐
                 │   BI / Consumption Layer        │
                 │   (Sigma / Tableau / PowerBI /   │
                 │    Snowsight dashboards)         │
                 └───────────────────────────────┘

 Orchestration:  Apache Airflow (MWAA/Composer/self-hosted) drives EVERY arrow above,
                 every 4 hours, per-source DAGs + downstream transformation DAG,
                 with dedicated backfill DAGs sharing the same task logic.

 Observability:  Airflow SLA/alerting + Snowflake query/task history + dbt test results
                 + a dedicated MUSAFIR_OPS monitoring schema (pipeline run log, DQ results).
```

See `03_ETL_Dataflow_Architecture.md` for the fully detailed, diagram-per-concern breakdown (extraction, DAG dependency graph, incremental vs. backfill flow, ERD).

---

## 4. Source Systems — Detailed Technical Profile

### 4.1 Source 1: OTA Booking Platform

| Attribute | Detail |
|---|---|
| **Connection method** | Read-only JDBC/ODBC connection to a **reporting replica** (never the primary OLTP DB) provided by ClientX.com engineering |
| **Change tracking mechanism** | `updated_at` (UTC timestamp) column on all mutable tables + monotonically increasing `booking_event_id` on an append-only booking-status-change log table |
| **Key entities extracted** | `customers`, `bookings`, `booking_items` (flight/hotel/holiday/visa line items), `passengers`, `itineraries`, `payments`, `refunds_cancellations`, `promotions_applied`, `support_tickets` |
| **Approx. volume** | Highest of the three sources — tens of thousands of booking events per day |
| **Extraction pattern** | Incremental pull: `WHERE updated_at > :last_watermark AND updated_at <= :batch_end_ts` |
| **Primary/business keys** | `customer_id`, `booking_id`/PNR, `payment_id` |
| **Timezone handling** | Source stores UTC; local display timezone (Asia/Dubai etc.) is a derived attribute, never used for watermarking |

### 4.2 Source 2: ClientX Business (TMC) Platform

| Attribute | Detail |
|---|---|
| **Connection method** | Read-only JDBC/ODBC connection to reporting replica / reporting API export |
| **Change tracking mechanism** | `last_modified_ts` column + explicit status-transition history table (`trip_status_history`) capturing requested → approved → booked → invoiced states |
| **Key entities extracted** | `corporate_accounts`, `travelers`, `travel_policies`, `trip_requests`, `trip_approvals`, `corporate_bookings`, `mice_events`, `invoices`, `account_managers` |
| **Approx. volume** | Medium volume, higher value per record; multiple state-change updates per trip record |
| **Extraction pattern** | Incremental pull on `last_modified_ts`; full status-history table extracted append-only (event log, no updates) |
| **Primary/business keys** | `corporate_account_id`, `trip_id`/PNR, `traveler_id`, `invoice_id` |
| **Special handling** | Multi-currency invoicing (AED, QAR, INR, SAR, USD) — currency conversion is standardized in the Curated layer, never in Raw |

### 4.3 Source 3: Retail / Branch POS System

| Attribute | Detail |
|---|---|
| **Connection method** | Read-only connection to the branch POS aggregation database (a central DB that each branch's till/POS syncs to) |
| **Change tracking mechanism** | `txn_updated_at` column; branch end-of-day (EOD) settlement batch flag `is_eod_final` |
| **Key entities extracted** | `branches`, `agents`, `walkin_customers`, `retail_transactions`, `retail_transaction_items`, `settlements` (cash/card breakdown), `footfall_log` |
| **Approx. volume** | Lowest volume of the three, but highest data-quality sensitivity (manual entry involved at branch level) |
| **Extraction pattern** | Incremental pull on `txn_updated_at`; a **daily reconciliation backfill** re-pulls the full prior business day once `is_eod_final = TRUE` is confirmed at each branch, to correct any late manual corrections |
| **Primary/business keys** | `branch_id`, `agent_id`, `transaction_id`, `walkin_customer_id` |
| **Timezone handling** | Branch-local timezone is a first-class attribute (UAE/Qatar/India spans 2 timezones); watermark logic always operates in UTC internally |

### 4.4 Source Onboarding Data Contract (applies to all 3)

Every source must supply (agreed during Phase 0 discovery, per Business Doc §10):
1. A documented entity list with primary keys and change-tracking column(s).
2. A **data dictionary** (column name, type, nullable, business definition) — versioned.
3. Confirmation of **read-only credential** provisioning (least privilege).
4. An agreed **retention/availability window** for historical extraction (backfill feasibility).
5. A named **technical point of contact** for schema-change notifications.

---

## 5. Snowflake Platform Design

### 5.1 Account & Database Topology

```
Snowflake Account: MUSAFIR_ANALYTICS
│
├── Databases
│   ├── MUSAFIR_RAW          -- 1:1 landing of source data, append-only, VARIANT-friendly
│   │     ├── SCHEMA OTA_RAW
│   │     ├── SCHEMA TMC_RAW
│   │     └── SCHEMA RETAIL_RAW
│   ├── MUSAFIR_STAGING      -- typed, deduplicated, source-shaped (Bronze)
│   │     ├── SCHEMA OTA_STG
│   │     ├── SCHEMA TMC_STG
│   │     └── SCHEMA RETAIL_STG
│   ├── MUSAFIR_CURATED      -- conformed, business-entity level (Silver)
│   │     ├── SCHEMA CORE          -- dim_customer, dim_supplier, dim_geography, dim_date
│   │     ├── SCHEMA BOOKINGS      -- unified booking/payment/cancellation entities
│   │     └── SCHEMA FINANCE       -- reconciliation staging entities
│   ├── MUSAFIR_MART         -- consumption-ready star schemas (Gold)
│   │     ├── SCHEMA EXEC_SCORECARD
│   │     ├── SCHEMA OTA_MART
│   │     ├── SCHEMA TMC_MART
│   │     ├── SCHEMA RETAIL_MART
│   │     └── SCHEMA FINANCE_MART
│   └── MUSAFIR_OPS          -- pipeline metadata, DQ results, audit logs
│         ├── SCHEMA PIPELINE_CONTROL   -- watermarks, run log, backfill registry
│         ├── SCHEMA DATA_QUALITY       -- DQ test results, anomaly flags
│         └── SCHEMA AUDIT              -- access logs, masking policy audit
│
├── Warehouses (compute, right-sized & isolated per workload)
│   ├── WH_INGEST_XS     (X-Small, auto-suspend 60s)  — Raw COPY INTO loads
│   ├── WH_TRANSFORM_S   (Small→Medium, auto-scale)   — dbt Staging→Curated→Mart runs
│   ├── WH_BI_M          (Medium, multi-cluster 1-3)  — BI tool / Snowsight queries
│   └── WH_ADHOC_S       (Small, auto-suspend 60s)    — analyst ad-hoc queries
│
└── Roles (RBAC — see §9)
      SYSADMIN → per-domain functional roles → per-segment/business-unit roles
```

**Why separate databases per layer (not schemas in one DB):** cleaner access-boundary enforcement (a Retail branch manager role can be granted on `MUSAFIR_MART.RETAIL_MART` without any implicit path to `MUSAFIR_RAW`), simpler zero-copy cloning per layer for dev/test, and clear cost-attribution via `WAREHOUSE`/`DATABASE` usage views.

### 5.2 Raw Layer Design

- Tables are **append-only**; every row carries ingestion metadata:
  `_load_id`, `_batch_run_ts`, `_source_system`, `_source_extracted_at`, `_file_name`, `_is_backfill BOOLEAN`.
- Semi-structured safety net: a `_raw_payload VARIANT` column stores the full row as landed (in addition to typed columns) — protects against silent data loss if a source adds a column before the Staging layer is updated to handle it.
- **No transformation logic** in Raw beyond type casting at COPY INTO time — Raw is the immutable, replayable foundation for all backfills.
- Retention: Raw tables use Snowflake **Time Travel** (default 1 day, extended to **90 days** on critical tables via `DATA_RETENTION_TIME_IN_DAYS`) plus **Fail-safe** (7 days, Snowflake-managed) as a last-resort recovery net.

### 5.3 Staging (Bronze) Layer Design

- One-to-one with Raw entities, but: deduplicated (latest record per business key per batch via `QUALIFY ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY updated_at DESC) = 1`), typed/cast, standardized column naming (`snake_case`, consistent date/timestamp types), soft-deletes flagged (`is_deleted`).
- Implemented as **dbt models materialized as incremental tables**, using Snowflake **Streams** on the Raw tables to identify only newly landed rows since the last Staging run — avoids full-table rescans every 4 hours.

### 5.4 Curated (Silver) Layer Design

- **Conformed dimensions** across all three sources: `dim_customer` (with cross-source identity resolution — see §5.4.1), `dim_supplier`, `dim_geography`, `dim_date`, `dim_agent_or_account_manager`.
- **Conformed fact staging**: standardized booking/payment/cancellation events regardless of originating segment, tagged with a `segment` attribute (`OTA` / `CORPORATE` / `RETAIL`).
- **Slowly Changing Dimensions (SCD Type 2)** for `dim_customer`, `dim_corporate_account`, and `dim_agent` — implemented via dbt's `snapshot` feature, preserving full history of e.g. a customer's tier/segment changes over time.
- **Business rules & derived flags** applied here (e.g., `is_policy_compliant`, `is_repeat_customer`, `booking_lead_time_days`), so Gold-layer marts stay simple aggregations, not rule engines.

#### 5.4.1 Cross-Source Identity Resolution (Customer 360 Engine)

1. **Deterministic match** (first pass): normalize and match on `email` (lowercased, trimmed) OR `phone` (E.164 normalized) OR government ID (`passport_no`/`emirates_id`, normalized).
2. **Confidence scoring**: each match type carries a confidence weight (email exact = high, phone exact = high, name+DOB fuzzy = medium).
3. **Survivorship rules**: when multiple source records merge into one `customer_master_key`, field-level survivorship rules pick the "best" value per attribute (e.g., most recently updated wins for contact info; OTA is authoritative for marketing consent).
4. **Low-confidence match queue**: matches below the confidence threshold are NOT auto-merged; they land in `MUSAFIR_OPS.DATA_QUALITY.IDENTITY_REVIEW_QUEUE` for periodic manual/analyst review (per Business Doc §9.3 risk mitigation).
5. Implemented as a dedicated dbt model chain (`int_customer_identity_matches` → `dim_customer`) using Snowflake native `MERGE` for deterministic idempotent re-runs.

### 5.5 Mart (Gold) Layer Design

- Pure **star schemas** per business domain (see §8 Data Model), materialized as Snowflake **Dynamic Tables** (preferred) or dbt incremental models, refreshed at the tail end of each 4-hour pipeline run.
- **Why Dynamic Tables**: declarative target-lag (`TARGET_LAG = '4 hours'`) lets Snowflake auto-manage incremental refresh of Gold aggregates without hand-written merge logic for straightforward rollups, reducing pipeline code for simpler marts (e.g., daily revenue summaries); more complex marts (identity-resolved, multi-source joins) remain dbt-managed incremental models for full control and testability.
- Gold objects are what BI tools and Snowsight dashboards query directly — no BI tool ever queries Raw/Staging/Curated.

### 5.6 Key Snowflake Features Used & Why

| Feature | Used For | Why |
|---|---|---|
| **External Stages + COPY INTO** | Loading Parquet files from cloud storage into Raw | Native, low-cost, highly parallel bulk batch loading — fits our 4-hour batch model exactly |
| **Streams** | Change detection between Raw→Staging and Staging→Curated | Native CDC-like mechanism inside Snowflake without external tooling; guarantees exactly-once consumption of new rows per run |
| **Tasks** | Scheduling lightweight in-Snowflake SQL steps chained to Streams | Used as a safety-net/complement to Airflow for tightly-coupled micro-orchestration (e.g., Stream consumption merge) — Airflow remains the master orchestrator (§6) |
| **Dynamic Tables** | Gold-layer aggregates with declarative freshness (`TARGET_LAG`) | Reduces hand-maintained incremental merge logic for straightforward rollups |
| **Zero-Copy Cloning** | Dev/QA environment provisioning, pre-backfill safety snapshots | Instant, storage-efficient clones of full databases before risky backfill operations |
| **Time Travel** | Point-in-time recovery, "what did this table look like before last night's load" debugging | Core safety net for a program with heavy incremental+backfill activity |
| **Dynamic Data Masking Policies** | PII protection (passport numbers, Emirates ID, email, phone) | Enforced at the warehouse level regardless of querying tool — see §9 |
| **Row Access Policies** | Regional data isolation (a Qatar branch manager cannot see UAE branch financials) | Centralized, tamper-resistant enforcement vs. per-dashboard filtering |
| **Resource Monitors** | Cost governance per warehouse | Hard/soft credit-quota alerts and suspension — see §11 |
| **Object Tagging** | Classifying PII columns, cost-center attribution | Powers automated masking policy assignment and FinOps reporting |
| **Query Tag / Query History** | Pipeline run traceability, performance tuning | Every Airflow-submitted query is tagged with `dag_id`, `task_id`, `run_id` for full traceability |

---

## 6. Orchestration — Apache Airflow Design

### 6.1 Why Airflow, and How It Fits

Airflow is the **master orchestrator** for the entire platform — it decides *when* each pipeline stage runs, *in what order*, *with what parameters* (including backfill windows), and is the system of record for **pipeline SLA, retries, and alerting**. Snowflake Tasks (§5.6) only handle tightly-coupled, sub-second-latency internal chaining; all cross-system, cross-source, and business-schedule-driven logic lives in Airflow.

### 6.2 Environment & Deployment

- **Airflow deployment:** Managed Airflow (e.g., Amazon MWAA / Astronomer / Cloud Composer) or self-hosted on Kubernetes (KubernetesExecutor) — sized for the modest DAG count/frequency of this program (batch every 4 hours, not high-frequency streaming).
- **Environments:** `dev` → `staging` → `prod`, each with its own Airflow metadata DB and its own Snowflake role/warehouse set (never share `prod` Snowflake credentials into `dev`).
- **Connections:** Airflow `Connections` store Snowflake credentials via **key-pair authentication** (not password), managed through Airflow's **Secrets Backend** integration with a cloud secrets manager (AWS Secrets Manager / Azure Key Vault / GCP Secret Manager) — no plaintext secrets in DAG code or Airflow UI.
- **Provider:** `apache-airflow-providers-snowflake` (SnowflakeOperator/SnowflakeSqlApiOperator/SnowflakeHook) and `apache-airflow-providers-amazon` (or equivalent) for stage file operations.

### 6.3 DAG Structure — Per-Source Extraction DAGs

One **DAG factory pattern**: a single Python module (`source_ingestion_dag_factory.py`) generates a DAG per source from a config file, rather than 3 duplicated hand-written DAGs.

```
dags/
├── config/
│   ├── source_ota.yml
│   ├── source_tmc.yml
│   └── source_retail.yml
├── factories/
│   └── source_ingestion_dag_factory.py     # generates ingest_ota_v1, ingest_tmc_v1, ingest_retail_v1
├── ingest_ota_v1.py             # thin wrapper invoking the factory with source_ota.yml
├── ingest_tmc_v1.py             # thin wrapper invoking the factory with source_tmc.yml
├── ingest_retail_v1.py          # thin wrapper invoking the factory with source_retail.yml
├── transform_curated_gold_v1.py # dbt-run DAG, triggered after all 3 ingestion DAGs succeed
├── backfill_source_v1.py        # parameterized manual-trigger backfill DAG (any source, any date range)
├── dq_monitoring_v1.py          # standalone data-quality + reconciliation checks & alerting
└── platform_maintenance_v1.py   # warehouse/resource-monitor housekeeping, Time Travel checks
```

Each generated **per-source ingestion DAG** (schedule: `0 */4 * * *`, i.e., every 4 hours) executes these tasks in order:

1. `check_source_availability` (SQLSensor/HttpSensor — confirm source replica is reachable and not mid-maintenance)
2. `get_last_watermark` (PythonOperator — reads `MUSAFIR_OPS.PIPELINE_CONTROL.WATERMARKS` for this source/entity)
3. `extract_incremental_to_stage` (PythonOperator/custom operator — pulls rows where `change_col > watermark`, writes Parquet to cloud storage landing path `s3://ClientX-lake/raw/{source}/{entity}/{yyyy}/{mm}/{dd}/{batch_id}/`)
4. `copy_into_raw` (SnowflakeOperator — `COPY INTO MUSAFIR_RAW.<SOURCE>_RAW.<entity> FROM @stage ... FILE_FORMAT = parquet_format`)
5. `validate_row_counts` (SnowflakeOperator/PythonOperator — source extract count vs. loaded count reconciliation; fails task on mismatch beyond tolerance)
6. `update_watermark` (PythonOperator — advances `MUSAFIR_OPS.PIPELINE_CONTROL.WATERMARKS` **only after** successful load+validation)
7. `emit_run_metadata` (PythonOperator — writes to `MUSAFIR_OPS.PIPELINE_CONTROL.RUN_LOG`: rows loaded, duration, status)
8. `trigger_downstream_if_all_sources_ready` (TriggerDagRunOperator / dataset-based scheduling — see §6.4)

Task dependency: `check_source_availability >> get_last_watermark >> extract_incremental_to_stage >> copy_into_raw >> validate_row_counts >> update_watermark >> emit_run_metadata`

### 6.4 Cross-DAG Dependency: Waiting for All 3 Sources

The downstream `transform_curated_gold_v1` DAG must only run once **all 3 source ingestion DAGs** have successfully completed their 4-hour cycle. This is implemented using **Airflow Datasets** (data-aware scheduling): each ingestion DAG's final task marks a dataset (e.g., `Dataset("snowflake://MUSAFIR_RAW/OTA_RAW")`) as updated; `transform_curated_gold_v1` is scheduled on `[ota_dataset, tmc_dataset, retail_dataset]`, so Airflow automatically triggers the transform DAG only when all three have refreshed — no manual `ExternalTaskSensor` polling required, and no risk of transforming on partial data.

### 6.5 Transformation DAG (`transform_curated_gold_v1`)

1. `dbt_source_freshness_check` — verifies Raw tables were actually updated within SLA before proceeding
2. `dbt_run_staging` (`dbt run --select staging.*`, via `BashOperator`/`Cosmos` `DbtTaskGroup`)
3. `dbt_test_staging` (`dbt test --select staging.*` — fails fast before propagating bad data downstream)
4. `dbt_run_curated` (`dbt run --select curated.*`) — includes identity resolution models
5. `dbt_test_curated`
6. `dbt_run_mart` (`dbt run --select mart.*`)
7. `dbt_test_mart`
8. `refresh_dynamic_tables_check` (validate Dynamic Table `TARGET_LAG` freshness met)
9. `run_reconciliation_checks` (invokes `dq_monitoring` logic for finance reconciliation — §7.4)
10. `publish_run_complete_notification` (Slack/Teams/email — "Gold layer refreshed as of {batch_end_ts}, freshness = X hrs")

### 6.6 Incremental Load Logic (Detail)

- **Watermark table:** `MUSAFIR_OPS.PIPELINE_CONTROL.WATERMARKS (source_system, entity_name, last_watermark_value, last_watermark_type, last_successful_run_id, updated_at)`.
- **Extraction query pattern** (parameterized per entity in `source_*.yml` config):
  ```sql
  SELECT * FROM {entity}
  WHERE {change_column} > '{last_watermark}'
    AND {change_column} <= '{batch_end_ts}'
  ORDER BY {change_column}
  ```
- The **upper bound** (`batch_end_ts`, the Airflow scheduled `data_interval_end`) is fixed at DAG-run time — this guarantees deterministic, replayable extraction windows (re-running the same scheduled run always pulls the exact same row set), which is essential for idempotency.
- **Watermark advances only after successful validated load** (task #6 in §6.3) — if a run fails at any step, the next scheduled run naturally retries the same (or an extended) window; no data is skipped.
- **Late-arriving data:** each entity config defines a `lookback_buffer` (e.g., Retail POS uses a 24-hour lookback per §4.3's EOD reconciliation need) — the extraction window's lower bound is `last_watermark - lookback_buffer` for sources prone to late corrections, with de-duplication at the Staging layer (§5.3) handling any re-delivered rows safely.

### 6.7 Backfill Design (Detail)

Backfill is a **first-class, explicitly designed capability**, not an afterthought — required for: onboarding historical data (12 months, per Business Doc §11 assumption), correcting a source-system defect, and period-end restatements.

- **Dedicated DAG:** `backfill_source_v1`, manually triggered via Airflow UI/CLI/API with **DAG run configuration parameters**:
  ```json
  {
    "source_system": "retail",
    "entity_name": "retail_transactions",
    "backfill_start_date": "2025-01-01",
    "backfill_end_date": "2025-12-31",
    "chunk_size_days": 7,
    "target_layer": "raw_through_mart"
  }
  ```
- **Chunking:** large historical ranges are split into configurable date chunks (default 7 days) to keep individual extract/load operations small, resumable, and easy to monitor — implemented as a **dynamic task mapping** (`expand()`) over the date-chunk list, so Airflow parallelizes chunks up to a configured concurrency limit.
- **Isolation from incremental runs:** backfill tasks write to the **same Raw tables** but are tagged `_is_backfill = TRUE` and use a **separate Snowflake warehouse** (`WH_INGEST_XS` with a backfill-specific resource tag) so a large historical reload never starves or slows the live 4-hour incremental pipeline.
- **Idempotent replace, not append-duplicate:** backfill loads use `MERGE`/`DELETE+INSERT` on the natural business key + change-tracking timestamp (not blind append), so re-running a backfill for the same range is safe and produces no duplicates.
- **Downstream cascade:** after a Raw backfill completes and validates, the same `transform_curated_gold_v1` DAG logic (§6.5) is re-invoked scoped to the affected date partitions (dbt's `--vars` date-range filtering), rather than reprocessing the entire curated/mart history — keeping backfills fast.
- **Backfill registry & audit:** every backfill run is logged in `MUSAFIR_OPS.PIPELINE_CONTROL.BACKFILL_REGISTRY` (who triggered it, why — a mandatory `reason` parameter, date range, rows affected, before/after row-count deltas) — directly supporting the audit requirement in Business Doc §9.2.

### 6.8 Scheduling Summary

| DAG | Schedule | Trigger Type |
|---|---|---|
| `ingest_ota_v1` | `0 */4 * * *` (every 4h) | Time-based |
| `ingest_tmc_v1` | `0 */4 * * *` (every 4h) | Time-based |
| `ingest_retail_v1` | `0 */4 * * *` (every 4h) | Time-based |
| `transform_curated_gold_v1` | Dataset-triggered | Fires when all 3 ingestion datasets updated |
| `dq_monitoring_v1` | `15 */4 * * *` (15 min after transform expected) | Time-based, with dataset dependency check |
| `backfill_source_v1` | None (manual) | Manually triggered (UI/CLI/API), parameterized |
| `platform_maintenance_v1` | `0 2 * * 0` (weekly, Sunday 02:00 UTC) | Time-based |

### 6.9 Failure Handling, Retries & Alerting

- Standard task-level `retries=3`, `retry_delay=10 minutes`, `retry_exponential_backoff=True`.
- `sla` set on critical path tasks (e.g., `copy_into_raw` SLA = 90 minutes from `data_interval_start`) → SLA miss triggers a dedicated Slack/PagerDuty alert distinct from a hard failure.
- `on_failure_callback` posts structured failure detail (DAG, task, exception, last successful watermark) to a Slack channel monitored by the Data & Analytics team (Business Doc §4.1).
- Circuit-breaker pattern: if a source's `check_source_availability` sensor fails **3 consecutive scheduled runs**, the DAG auto-pauses and pages the on-call engineer rather than silently accumulating a growing backlog window.

---

## 7. Data Quality Framework

### 7.1 Layers of Data Quality Enforcement

| Layer | Checks | Tooling |
|---|---|---|
| **Extraction (Airflow)** | Source row count vs. staged file row count reconciliation; source connectivity/availability | Custom PythonOperator checks |
| **Raw → Staging (dbt)** | Not-null on primary/business keys, uniqueness (post-dedup), accepted value sets (e.g., `booking_status IN (...)`), referential integrity to conformed dimensions | `dbt test` (schema tests) + `dbt-utils`/`dbt-expectations` packages |
| **Staging → Curated (dbt)** | Identity-resolution confidence thresholds, SCD Type 2 correctness (no overlapping validity windows), business-rule assertions (e.g., `booking_amount >= 0`) | Custom dbt singular tests |
| **Curated → Mart (dbt + SQL)** | Aggregate reconciliation: sum of fact table revenue == sum of curated-layer source revenue (row-count and amount-level tie-outs) | Custom SQL reconciliation models + `dq_monitoring_v1` DAG |
| **Cross-source reconciliation** | Bookings ↔ Payment gateway settlement ↔ BSP/GDS airline settlement ↔ corporate invoicing variance detection | Dedicated `FINANCE_MART.reconciliation_variance` model, alerted daily |

### 7.2 Data Quality Result Storage & Alerting

- All `dbt test` results are stored (via `dbt artifacts` upload) into `MUSAFIR_OPS.DATA_QUALITY.TEST_RESULTS` for trend analysis (is a particular test flaky, degrading over time?).
- A **DQ scorecard** (pass rate % per run, per layer) is itself a Gold-layer mart (`EXEC_SCORECARD.pipeline_health`), surfaced to the Data & Analytics team and referenced in Business Doc §8.2 success criteria (≥99.5% pass rate).
- **Severity tiers:** `BLOCKING` (fails the pipeline run, e.g., primary key null) vs. `WARNING` (logs and alerts but does not block, e.g., a minor referential integrity gap pending upstream fix) — configured per test.

### 7.3 Schema Drift Detection

- Each ingestion DAG's `extract_incremental_to_stage` task compares the extracted column set against the last-known schema recorded in `MUSAFIR_OPS.PIPELINE_CONTROL.SOURCE_SCHEMA_REGISTRY`.
- **New column detected** → warning alert, column auto-added to Raw table (Snowflake `ALTER TABLE ... ADD COLUMN`, safe/additive), Staging layer flags it as `_unmapped_new_column` until a dbt model change consciously maps it.
- **Column removed/renamed/type-changed** → **blocking alert**, pipeline pauses for that entity pending manual review (prevents silent data loss or type-cast failures).

### 7.4 Finance Reconciliation Detail (Directly Serves Business Doc §7.5 Use Case 15)

`FINANCE_MART.reconciliation_variance` compares, per booking/PNR:
1. Booking amount recorded in source (OTA/TMC/Retail curated booking record)
2. Payment amount confirmed by payment gateway/POS settlement record
3. Airline/hotel supplier settlement amount (BSP/GDS report, ingested as part of the relevant source or a 4th lightweight feed if separately available)
4. Corporate invoice amount (TMC segment only)

Any variance beyond a configurable tolerance (e.g., currency-rounding threshold) is flagged into a `reconciliation_exceptions` table, feeding the Finance Reconciliation Dashboard and reducing manual month-end effort (Business Doc §8.2).

---

## 8. Data Model (Gold Layer — Star Schema Design)

### 8.1 Conformed Dimensions (Shared Across All Marts)

| Dimension | Grain | Key Attributes | SCD Type |
|---|---|---|---|
| `dim_date` | 1 row per calendar day | date, fiscal week/month/quarter/year, is_holiday (per country), day_of_week | Type 0 (static) |
| `dim_customer` | 1 row per resolved customer identity (+ history) | customer_master_key, name, contact (masked), segment_ever_used (OTA/Corporate/Retail flags), first_seen_date, ltv_tier | Type 2 |
| `dim_corporate_account` | 1 row per corporate client (+ history) | account_id, account_name, industry, region, credit_terms, account_manager_key | Type 2 |
| `dim_agent` | 1 row per retail agent / corporate account manager | agent_id, name, branch_key, role, hire_date | Type 2 |
| `dim_branch` | 1 row per retail branch/lounge | branch_id, branch_name, country, city, region, opened_date | Type 1 |
| `dim_supplier` | 1 row per airline/hotel/supplier | supplier_id, supplier_name, supplier_type (airline/hotel/DMC), IATA/chain code | Type 1 |
| `dim_product` | 1 row per product/category | product_id, product_category (Flight/Hotel/Holiday/Visa/MICE), sub_category | Type 1 |
| `dim_geography` | 1 row per region/country/city | geo_id, country, region, city, timezone | Type 1 |

### 8.2 Fact Tables

| Fact Table | Grain | Segment(s) | Key Measures |
|---|---|---|---|
| `fact_booking` | 1 row per booking line item | All (OTA, Corporate, Retail — via `segment` attribute) | gross_amount, net_revenue, cost_of_sale, margin, quantity |
| `fact_payment` | 1 row per payment/settlement transaction | All | payment_amount, payment_method, gateway_fee |
| `fact_cancellation_refund` | 1 row per cancellation/refund event | All | refund_amount, cancellation_fee, days_before_departure |
| `fact_corporate_trip` | 1 row per corporate trip request lifecycle | Corporate (TMC) | requested_amount, approved_amount, is_policy_compliant, approval_turnaround_hours |
| `fact_mice_event` | 1 row per MICE event | Corporate (TMC) | event_revenue, event_cost, attendee_count |
| `fact_retail_transaction` | 1 row per branch POS transaction | Retail | transaction_amount, settlement_cash, settlement_card, agent_key |
| `fact_footfall` | 1 row per branch-day footfall count | Retail | footfall_count, appointments_booked |
| `fact_reconciliation_variance` | 1 row per booking with a detected variance | All (Finance cross-cutting) | booking_amount, payment_amount, settlement_amount, variance_amount |

### 8.3 Example Star Schema (Unified Booking Mart)

```
                dim_date        dim_customer
                    \                /
                     \              /
   dim_supplier ---- fact_booking ---- dim_product
                     /              \
                    /                \
             dim_branch          dim_geography
             (Retail only, NULL for OTA)
```

`fact_booking.segment` (`OTA` / `CORPORATE` / `RETAIL`) allows single unified queries ("total revenue across all channels") while still supporting segment-filtered dashboards — this single conformed fact table is the technical backbone of Business Doc §7.1 Use Case 1 (Unified Revenue & Margin Dashboard).

---

## 9. Security & Access Control

### 9.1 Role Hierarchy (RBAC)

```
ACCOUNTADMIN
   └── SYSADMIN
         ├── MUSAFIR_PLATFORM_ENGINEER      (full DDL on RAW/STAGING/CURATED/MART, all warehouses)
         ├── MUSAFIR_DBT_SERVICE_ROLE       (used by Airflow/dbt service account; write to STAGING/CURATED/MART only)
         ├── MUSAFIR_ANALYST_GLOBAL         (read MART, masked PII, all segments)
         ├── MUSAFIR_ANALYST_OTA            (read MART.OTA_MART + EXEC_SCORECARD only)
         ├── MUSAFIR_ANALYST_TMC            (read MART.TMC_MART + EXEC_SCORECARD only)
         ├── MUSAFIR_ANALYST_RETAIL         (read MART.RETAIL_MART, row-access-restricted to own region)
         ├── MUSAFIR_FINANCE_COMPLIANCE     (read MART.FINANCE_MART, unmasked PII where legally justified & audited)
         └── MUSAFIR_EXEC_VIEWER            (read EXEC_SCORECARD only, masked PII)
```

- **Principle:** roles map to Business Doc §4.1 stakeholders 1:1, so access governance can be reasoned about in business terms, not just technical grants.
- Airflow/dbt connects using `MUSAFIR_DBT_SERVICE_ROLE` via **key-pair authentication** (RSA key pair, rotated per security policy) — never a personal user login.

### 9.2 Dynamic Data Masking Policies

```sql
CREATE OR REPLACE MASKING POLICY pii_email_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('MUSAFIR_FINANCE_COMPLIANCE','MUSAFIR_PLATFORM_ENGINEER') THEN val
    ELSE REGEXP_REPLACE(val, '^.+(@.+)$', '***\\1')
  END;

ALTER TABLE MUSAFIR_CURATED.CORE.DIM_CUSTOMER
  MODIFY COLUMN EMAIL SET MASKING POLICY pii_email_mask;
```

Applied to: email, phone, passport number, Emirates ID, payment card reference (last-4 only ever stored; never raw PAN — enforced at extraction, not just masking).

### 9.3 Row Access Policies (Regional Isolation)

```sql
CREATE OR REPLACE ROW ACCESS POLICY retail_region_policy AS (branch_region STRING) RETURNS BOOLEAN ->
  CURRENT_ROLE() IN ('MUSAFIR_PLATFORM_ENGINEER','MUSAFIR_ANALYST_GLOBAL','MUSAFIR_FINANCE_COMPLIANCE')
  OR branch_region = (SELECT allowed_region FROM MUSAFIR_OPS.AUDIT.ROLE_REGION_MAP WHERE role_name = CURRENT_ROLE());

ALTER TABLE MUSAFIR_MART.RETAIL_MART.FACT_RETAIL_TRANSACTION
  ADD ROW ACCESS POLICY retail_region_policy ON (branch_region);
```

### 9.4 Network & Authentication

- Snowflake **Network Policies** restrict connections to known Airflow environment IPs and ClientX corporate VPN ranges.
- **Key-pair auth** for all service accounts (Airflow, dbt Cloud/Core runner); **SSO/SAML** (via ClientX's IdP) for human analyst/BI access — no standalone passwords.
- All storage (cloud object storage landing zone) uses **server-side encryption at rest**; Snowflake data is encrypted at rest by default (AES-256) and in transit (TLS 1.2+).

### 9.5 Compliance Mapping

| Requirement (Business Doc §9.1) | Technical Control |
|---|---|
| UAE PDPL — data minimization, purpose limitation | Only fields needed for defined analytics use cases are extracted; data dictionary review at onboarding |
| PCI-DSS — no raw card data | Raw PAN never leaves source system; only tokenized reference + last-4 ingested |
| IATA/BSP auditability | `fact_reconciliation_variance` + immutable Raw layer with Time Travel provide full audit trail |
| Access auditability | Snowflake `ACCESS_HISTORY` + `MUSAFIR_OPS.AUDIT` schema capture all query/grant activity |

---

## 10. CI/CD & Environment Management

- **Version control:** All Airflow DAG code, dbt models, and Snowflake DDL (via a schema-as-code approach, e.g., `schemachange` or dbt's own DDL where possible) live in Git (monorepo: `/dags`, `/dbt`, `/snowflake_setup`).
- **CI pipeline** (on PR): dbt `compile` + `dbt test --select state:modified` (slim CI against a cloned dev schema via zero-copy clone), Airflow DAG `python -m pytest` for DAG-integrity tests (no import errors, no cycle, correct schedule), SQL linting (`sqlfluff`).
- **CD pipeline** (on merge to main): deploy DAGs to `staging` Airflow environment automatically; deploy to `prod` via manual approval gate (per Business Doc governance principle of change review).
- **Environment isolation:** `dev`/`staging` Snowflake databases are **zero-copy clones** of `prod` (refreshed weekly), so testing uses realistic data volume without duplicating storage cost or touching real prod credentials.

---

## 11. Cost Governance (FinOps)

- **Resource Monitors** on every warehouse (`WH_INGEST_XS`, `WH_TRANSFORM_S`, `WH_BI_M`, `WH_ADHOC_S`) with monthly credit quotas; `NOTIFY` at 75%, `SUSPEND` at 100% for non-critical warehouses (`WH_ADHOC_S`), `NOTIFY`-only with executive override for `WH_INGEST_XS`/`WH_TRANSFORM_S` to avoid silently breaking the core 4-hour SLA.
- **Auto-suspend (60s) / auto-resume** on all warehouses — no idle credit burn between the 4-hour batch runs.
- **Warehouse right-sizing reviews**: monthly review of `QUERY_HISTORY`/`WAREHOUSE_METERING_HISTORY` to confirm sizes remain appropriate as volume grows (directly supports Business Doc §9.3 cost-overrun risk mitigation).
- **Query tagging** (`ALTER SESSION SET QUERY_TAG = '{"dag_id":..., "task_id":..., "run_id":...}'`) enables per-pipeline-stage cost attribution.

---

## 12. Monitoring & Observability Summary

| Layer | Tool/Mechanism | What's Monitored |
|---|---|---|
| Orchestration | Airflow UI + Slack/PagerDuty alerts | DAG/task success-failure, SLA misses, retry counts |
| Data Platform | Snowflake `QUERY_HISTORY`, `TASK_HISTORY`, `WAREHOUSE_METERING_HISTORY` | Query performance, Task success, credit consumption |
| Data Quality | `MUSAFIR_OPS.DATA_QUALITY` schema + `dq_monitoring_v1` DAG | Test pass rate, schema drift, reconciliation variance |
| Pipeline Freshness | `MUSAFIR_OPS.PIPELINE_CONTROL.RUN_LOG` + Gold-layer `pipeline_health` mart | End-to-end freshness vs. 4-hour SLA (Business Doc §6, §8.2) |
| Cost | Resource Monitors + weekly FinOps report | Credit burn per warehouse/workload |

---

## 13. Appendix A — Sample DDL (Raw Layer)

```sql
CREATE TABLE IF NOT EXISTS MUSAFIR_RAW.OTA_RAW.BOOKINGS (
    booking_id            STRING,
    customer_id           STRING,
    booking_status        STRING,
    product_category      STRING,
    gross_amount          NUMBER(18,2),
    currency              STRING,
    created_at            TIMESTAMP_NTZ,
    updated_at            TIMESTAMP_NTZ,
    _raw_payload          VARIANT,
    _load_id              STRING,
    _batch_run_ts         TIMESTAMP_NTZ,
    _source_extracted_at  TIMESTAMP_NTZ,
    _file_name            STRING,
    _is_backfill          BOOLEAN DEFAULT FALSE
)
DATA_RETENTION_TIME_IN_DAYS = 90;
```

## 14. Appendix B — Sample Airflow Ingestion Task (Illustrative)

```python
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

copy_into_raw = SnowflakeOperator(
    task_id="copy_into_raw",
    snowflake_conn_id="snowflake_musafir_prod",
    sql="""
        COPY INTO MUSAFIR_RAW.{{ params.source_schema }}.{{ params.entity }}
        FROM @musafir_raw_stage/{{ params.source }}/{{ params.entity }}/
             {{ data_interval_end.strftime('%Y/%m/%d') }}/{{ run_id }}/
        FILE_FORMAT = (FORMAT_NAME = 'MUSAFIR_RAW.PARQUET_FORMAT')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'ABORT_STATEMENT';
    """,
    params={"source": "ota", "source_schema": "OTA_RAW", "entity": "bookings"},
)
```

## 15. Appendix C — Sample dbt Incremental Model (Staging Layer)

```sql
-- models/staging/ota/stg_ota__bookings.sql
{{ config(materialized='incremental', unique_key='booking_id', incremental_strategy='merge') }}

with source as (
    select * from {{ source('ota_raw', 'bookings') }}
    {% if is_incremental() %}
    where _batch_run_ts > (select max(_batch_run_ts) from {{ this }})
    {% endif %}
),
deduped as (
    select *,
        row_number() over (partition by booking_id order by updated_at desc) as rn
    from source
)
select
    booking_id,
    customer_id,
    lower(trim(booking_status)) as booking_status,
    product_category,
    gross_amount,
    currency,
    created_at,
    updated_at,
    'OTA' as segment
from deduped
where rn = 1
```

---

*End of Technical Architecture Document. See `03_ETL_Dataflow_Architecture.md` for the full set of dataflow/ETL/DAG diagrams.*
