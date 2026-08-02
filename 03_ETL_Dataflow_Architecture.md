# ClientX.com — Analytics Enablement Program
## Detailed ETL / Dataflow Architecture (Diagrams)

| | |
|---|---|
| **Program Name** | ClientX Unified Analytics Platform (MUAP) |
| **Document Type** | Dataflow & ETL Architecture Diagrams |
| **Version** | 1.0 |
| **Date** | 2025-07-22 |
| **Companion Document** | `01_Business_and_Core_Document.md`, `02_Technical_Architecture_Document.md` |

> All diagrams are authored in **Mermaid** syntax. They render natively in VS Code (Mermaid preview extension), GitHub/GitLab markdown, Obsidian, and any Mermaid-compatible viewer. This document is the visual companion to the Technical Architecture Document (`02_...`) — every diagram here corresponds to a section number there.

---

## 1. System Context Diagram (Who Talks to What)

```mermaid
flowchart LR
    subgraph SRC["Source Systems (Structured, Batch)"]
        S1[("Source 1\nOTA Booking DB\n(ClientX.com)")]
        S2[("Source 2\nMusafir Business /\nTMC DB")]
        S3[("Source 3\nRetail / Branch\nPOS DB")]
    end

    subgraph LAKE["Cloud Object Storage (Landing Zone)"]
        L1["s3://ClientX-lake/raw/ota/"]
        L2["s3://ClientX-lake/raw/tmc/"]
        L3["s3://ClientX-lake/raw/retail/"]
    end

    subgraph SNOW["Snowflake — MUSAFIR_ANALYTICS Account"]
        RAW[(RAW Layer\nMUSAFIR_RAW)]
        STG[(STAGING / Bronze\nMUSAFIR_STAGING)]
        CUR[(CURATED / Silver\nMUSAFIR_CURATED)]
        MART[(MART / Gold\nMUSAFIR_MART)]
        OPS[(OPS / Control\nMUSAFIR_OPS)]
    end

    AF["Apache Airflow\n(Orchestration Layer)"]
    BI["BI / Consumption\n(Dashboards, Snowsight)"]
    STAKE["Business Stakeholders\n(Exec, Finance, Segment Heads,\nRegional GMs, Compliance)"]

    S1 -->|JDBC batch extract, every 4h| L1
    S2 -->|JDBC batch extract, every 4h| L2
    S3 -->|JDBC batch extract, every 4h| L3

    L1 -->|COPY INTO| RAW
    L2 -->|COPY INTO| RAW
    L3 -->|COPY INTO| RAW

    RAW --> STG --> CUR --> MART --> BI --> STAKE

    AF -.orchestrates & schedules.-> S1
    AF -.orchestrates & schedules.-> S2
    AF -.orchestrates & schedules.-> S3
    AF -.orchestrates.-> RAW
    AF -.orchestrates dbt.-> STG
    AF -.orchestrates dbt.-> CUR
    AF -.orchestrates dbt.-> MART
    AF -.writes watermarks/logs.-> OPS
    OPS -.DQ + reconciliation results.-> BI
```

---

## 2. End-to-End Detailed Dataflow (Medallion Architecture)

```mermaid
flowchart TB
    A1["OTA Booking DB\n(customers, bookings, payments,\ncancellations, promotions)"]
    A2["ClientX Business DB\n(corporate accounts, trips,\napprovals, MICE, invoices)"]
    A3["Retail POS DB\n(branches, agents,\ntransactions, settlements)"]

    A1 --> E1["Extractor Task:\nWHERE updated_at > watermark\nAND updated_at <= batch_end_ts"]
    A2 --> E2["Extractor Task:\nWHERE last_modified_ts > watermark"]
    A3 --> E3["Extractor Task:\nWHERE txn_updated_at > watermark\n(+24h lookback buffer)"]

    E1 --> F1["Parquet files\npartitioned by entity/date/batch_id"]
    E2 --> F2["Parquet files\npartitioned by entity/date/batch_id"]
    E3 --> F3["Parquet files\npartitioned by entity/date/batch_id"]

    F1 --> C1["COPY INTO\nMUSAFIR_RAW.OTA_RAW.*"]
    F2 --> C2["COPY INTO\nMUSAFIR_RAW.TMC_RAW.*"]
    F3 --> C3["COPY INTO\nMUSAFIR_RAW.RETAIL_RAW.*"]

    C1 --> V1{"Row count\nreconciliation\npass?"}
    C2 --> V2{"Row count\nreconciliation\npass?"}
    C3 --> V3{"Row count\nreconciliation\npass?"}

    V1 -->|Yes| W1["Advance watermark\n(OTA)"]
    V2 -->|Yes| W2["Advance watermark\n(TMC)"]
    V3 -->|Yes| W3["Advance watermark\n(Retail)"]
    V1 -->|No| X1["Fail task, alert,\nretain prior watermark"]
    V2 -->|No| X2["Fail task, alert,\nretain prior watermark"]
    V3 -->|No| X3["Fail task, alert,\nretain prior watermark"]

    W1 --> D["Dataset marker:\nota_raw_updated"]
    W2 --> D2["Dataset marker:\ntmc_raw_updated"]
    W3 --> D3["Dataset marker:\nretail_raw_updated"]

    D & D2 & D3 --> T0["transform_curated_gold_v1\nDAG triggers\n(all 3 datasets ready)"]

    T0 --> T1["dbt run: STAGING\n(dedupe, type-cast,\nstandardize per source)"]
    T1 --> T1T["dbt test: STAGING"]
    T1T --> T2["dbt run: CURATED\n(identity resolution,\nSCD Type 2, business rules,\nconformed dimensions)"]
    T2 --> T2T["dbt test: CURATED"]
    T2T --> T3["dbt run: MART\n(star schemas: fact_booking,\nfact_payment, fact_retail_transaction,\nfact_corporate_trip, fact_mice_event, ...)"]
    T3 --> T3T["dbt test: MART"]
    T3T --> R["Reconciliation checks\n(bookings vs payments vs\nBSP/GDS settlement vs invoices)"]
    R --> N["Notify: Gold layer refreshed\n(Slack/Teams) + freshness metric\nwritten to pipeline_health mart"]
    N --> BI["BI Tools / Snowsight\nExecutive & Segment Dashboards"]
```

---

## 3. Airflow DAG Dependency Graph (Orchestration View)

```mermaid
flowchart LR
    subgraph DAG1["ingest_ota_v1  (schedule: 0 */4 * * *)"]
        a1["check_source_availability"] --> a2["get_last_watermark"] --> a3["extract_incremental_to_stage"] --> a4["copy_into_raw"] --> a5["validate_row_counts"] --> a6["update_watermark"] --> a7["emit_run_metadata"]
    end

    subgraph DAG2["ingest_tmc_v1  (schedule: 0 */4 * * *)"]
        b1["check_source_availability"] --> b2["get_last_watermark"] --> b3["extract_incremental_to_stage"] --> b4["copy_into_raw"] --> b5["validate_row_counts"] --> b6["update_watermark"] --> b7["emit_run_metadata"]
    end

    subgraph DAG3["ingest_retail_v1  (schedule: 0 */4 * * *)"]
        c1["check_source_availability"] --> c2["get_last_watermark"] --> c3["extract_incremental_to_stage"] --> c4["copy_into_raw"] --> c5["validate_row_counts"] --> c6["update_watermark"] --> c7["emit_run_metadata"]
    end

    a7 -.marks dataset.-> DS1(["Dataset:\nota_raw_updated"])
    b7 -.marks dataset.-> DS2(["Dataset:\ntmc_raw_updated"])
    c7 -.marks dataset.-> DS3(["Dataset:\nretail_raw_updated"])

    DS1 & DS2 & DS3 --> DAG4

    subgraph DAG4["transform_curated_gold_v1  (dataset-triggered)"]
        d1["dbt_source_freshness_check"] --> d2["dbt_run_staging"] --> d3["dbt_test_staging"] --> d4["dbt_run_curated"] --> d5["dbt_test_curated"] --> d6["dbt_run_mart"] --> d7["dbt_test_mart"] --> d8["refresh_dynamic_tables_check"] --> d9["run_reconciliation_checks"] --> d10["publish_run_complete_notification"]
    end

    DAG4 --> DAG5["dq_monitoring_v1\n(schedule: 15 */4 * * *)"]

    DAGB["backfill_source_v1\n(manual trigger, parameterized:\nsource, entity, date range, reason)"] -.shares task logic with.-> DAG1
    DAGB -.shares task logic with.-> DAG2
    DAGB -.shares task logic with.-> DAG3
    DAGB -.re-invokes scoped run of.-> DAG4

    DAG6["platform_maintenance_v1\n(schedule: weekly, Sun 02:00 UTC)"]
```

---

## 4. Incremental Load — Sequence Diagram

```mermaid
sequenceDiagram
    participant AF as Airflow Scheduler
    participant SRC as Source System (e.g., OTA DB)
    participant STG as Cloud Storage (Landing)
    participant SF as Snowflake (RAW)
    participant OPS as MUSAFIR_OPS (Watermarks/Logs)

    AF->>OPS: get_last_watermark(source, entity)
    OPS-->>AF: last_watermark_value = T1
    AF->>SRC: SELECT * WHERE change_col > T1 AND change_col <= batch_end_ts(T2)
    SRC-->>AF: rows (delta since T1)
    AF->>STG: write Parquet file(s) to /raw/{source}/{entity}/{date}/{batch_id}/
    AF->>SF: COPY INTO RAW.<source>.<entity> FROM stage
    SF-->>AF: rows_loaded count
    AF->>AF: validate_row_counts(source_count, rows_loaded)
    alt counts match (within tolerance)
        AF->>OPS: update_watermark(source, entity, T2)
        AF->>OPS: emit_run_metadata(status=SUCCESS, rows=rows_loaded)
        AF->>AF: mark dataset updated -> triggers downstream transform DAG
    else mismatch
        AF->>OPS: emit_run_metadata(status=FAILED, reason=count_mismatch)
        AF->>AF: alert on-call (Slack/PagerDuty), retain watermark = T1
    end
```

---

## 5. Backfill Flow — Sequence Diagram

```mermaid
sequenceDiagram
    participant ENG as Data Engineer
    participant AF as Airflow (backfill_source_v1)
    participant SRC as Source System
    participant SF_RAW as Snowflake RAW
    participant DBT as dbt (Curated + Mart)
    participant REG as Backfill Registry (MUSAFIR_OPS)

    ENG->>AF: Trigger backfill_source_v1\n(source, entity, start_date, end_date, reason, chunk_size_days=7)
    AF->>REG: log backfill request (who, why, range)
    AF->>AF: split range into chunks (dynamic task mapping)
    loop for each date chunk (parallel, bounded concurrency)
        AF->>SRC: SELECT * WHERE change_col BETWEEN chunk_start AND chunk_end
        SRC-->>AF: historical rows for chunk
        AF->>SF_RAW: MERGE/replace into RAW tables\n(tag _is_backfill = TRUE)
        AF->>REG: log chunk result (rows affected, before/after counts)
    end
    AF->>DBT: re-invoke transform_curated_gold_v1\nscoped to affected date partitions only
    DBT-->>AF: Curated + Mart refreshed for affected range
    AF->>REG: mark backfill COMPLETE, final row-count delta
    AF-->>ENG: Notification: backfill complete, summary stats
```

---

## 6. Incremental vs. Backfill — Decision & Isolation Logic

```mermaid
flowchart TD
    Start(["Pipeline event"]) --> Q1{"Scheduled\n(every 4h) or\nmanually triggered?"}
    Q1 -->|Scheduled| INC["Incremental Path"]
    Q1 -->|Manual, parameterized| BACK["Backfill Path"]

    INC --> INC1["Uses WH_INGEST_XS (live warehouse)"]
    INC1 --> INC2["Extraction window:\nlast_watermark -> data_interval_end"]
    INC2 --> INC3["Append/Merge into RAW\n(_is_backfill = FALSE)"]
    INC3 --> INC4["Watermark advances\non success only"]
    INC4 --> INC5["Downstream transform:\nfull incremental dbt run\n(Streams-based, new rows only)"]

    BACK --> BACK1["Uses WH_INGEST_XS with\nbackfill resource tag\n(isolated from live SLA)"]
    BACK1 --> BACK2["Extraction window:\nuser-specified start_date -> end_date,\nchunked (default 7 days)"]
    BACK2 --> BACK3["MERGE/replace into RAW\n(_is_backfill = TRUE, idempotent)"]
    BACK3 --> BACK4["Watermark NOT affected\n(backfill is out-of-band vs. live watermark)"]
    BACK4 --> BACK5["Downstream transform:\ndbt run scoped via --vars\nto affected date partitions only"]

    INC5 --> Done(["Gold layer reflects new data\nwithin 4-6h SLA"])
    BACK5 --> Done2(["Gold layer reflects corrected\nhistorical data, live SLA unaffected"])
```

---

## 7. Data Model — Conformed Star Schema (Entity Relationship Diagram)

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_BOOKING : "booking_date_key"
    DIM_CUSTOMER ||--o{ FACT_BOOKING : "customer_key"
    DIM_SUPPLIER ||--o{ FACT_BOOKING : "supplier_key"
    DIM_PRODUCT ||--o{ FACT_BOOKING : "product_key"
    DIM_BRANCH ||--o{ FACT_BOOKING : "branch_key (nullable, Retail only)"
    DIM_GEOGRAPHY ||--o{ FACT_BOOKING : "geo_key"

    FACT_BOOKING ||--o{ FACT_PAYMENT : "booking_key"
    FACT_BOOKING ||--o{ FACT_CANCELLATION_REFUND : "booking_key"

    DIM_CORPORATE_ACCOUNT ||--o{ FACT_CORPORATE_TRIP : "account_key"
    DIM_AGENT ||--o{ FACT_CORPORATE_TRIP : "account_manager_key"
    FACT_CORPORATE_TRIP ||--o{ FACT_MICE_EVENT : "trip_key (optional)"

    DIM_BRANCH ||--o{ FACT_RETAIL_TRANSACTION : "branch_key"
    DIM_AGENT ||--o{ FACT_RETAIL_TRANSACTION : "agent_key"
    DIM_BRANCH ||--o{ FACT_FOOTFALL : "branch_key"

    FACT_BOOKING ||--o{ FACT_RECONCILIATION_VARIANCE : "booking_key"

    DIM_CUSTOMER {
        string customer_master_key PK
        string name
        string email_masked
        string phone_masked
        string segment_flags
        date first_seen_date
        string ltv_tier
    }
    DIM_CORPORATE_ACCOUNT {
        string account_key PK
        string account_name
        string industry
        string region
        string credit_terms
    }
    DIM_AGENT {
        string agent_key PK
        string name
        string role
        string branch_key FK
    }
    DIM_BRANCH {
        string branch_key PK
        string branch_name
        string country
        string region
    }
    DIM_SUPPLIER {
        string supplier_key PK
        string supplier_name
        string supplier_type
        string iata_code
    }
    DIM_PRODUCT {
        string product_key PK
        string product_category
        string sub_category
    }
    DIM_GEOGRAPHY {
        string geo_key PK
        string country
        string city
        string timezone
    }
    DIM_DATE {
        date date_key PK
        int fiscal_week
        int fiscal_month
        int fiscal_year
    }
    FACT_BOOKING {
        string booking_key PK
        string segment
        string customer_key FK
        string product_key FK
        string supplier_key FK
        string branch_key FK
        date booking_date_key FK
        decimal gross_amount
        decimal net_revenue
        decimal cost_of_sale
        decimal margin
    }
    FACT_PAYMENT {
        string payment_key PK
        string booking_key FK
        decimal payment_amount
        string payment_method
    }
    FACT_CANCELLATION_REFUND {
        string cancel_key PK
        string booking_key FK
        decimal refund_amount
        int days_before_departure
    }
    FACT_CORPORATE_TRIP {
        string trip_key PK
        string account_key FK
        string account_manager_key FK
        decimal requested_amount
        decimal approved_amount
        boolean is_policy_compliant
    }
    FACT_MICE_EVENT {
        string event_key PK
        string trip_key FK
        decimal event_revenue
        decimal event_cost
        int attendee_count
    }
    FACT_RETAIL_TRANSACTION {
        string transaction_key PK
        string branch_key FK
        string agent_key FK
        decimal transaction_amount
        decimal settlement_cash
        decimal settlement_card
    }
    FACT_FOOTFALL {
        string footfall_key PK
        string branch_key FK
        date date_key FK
        int footfall_count
    }
    FACT_RECONCILIATION_VARIANCE {
        string variance_key PK
        string booking_key FK
        decimal booking_amount
        decimal payment_amount
        decimal settlement_amount
        decimal variance_amount
    }
```

---

## 8. Identity Resolution (Customer 360) Flow

```mermaid
flowchart TD
    O["OTA customer record\n(email, phone)"] --> M["Matching Engine"]
    T["ClientX Business traveler record\n(email, corporate ID)"] --> M
    R["Retail walk-in record\n(name, phone, Emirates ID/passport)"] --> M

    M --> D1{"Deterministic match?\n(email / phone / gov-ID exact)"}
    D1 -->|Yes, high confidence| MERGE["Merge into single\ncustomer_master_key\n(apply survivorship rules)"]
    D1 -->|No exact match| D2{"Fuzzy match?\n(name + DOB, etc.)"}
    D2 -->|Medium confidence, below\nauto-merge threshold| QUEUE["Route to\nIDENTITY_REVIEW_QUEUE\n(manual analyst review)"]
    D2 -->|No plausible match| NEW["Create new\ncustomer_master_key"]

    MERGE --> DIMC["dim_customer (SCD Type 2)"]
    NEW --> DIMC
    QUEUE -->|Analyst confirms/rejects| DIMC
```

---

## 9. Security Enforcement Flow (Query-Time)

```mermaid
flowchart LR
    U["User / BI Tool\n(e.g., Retail Analyst, Qatar region)"] -->|Query MUSAFIR_MART| RBAC{"Role has\nGRANT on schema/object?"}
    RBAC -->|No| DENY["Access Denied"]
    RBAC -->|Yes| ROW{"Row Access Policy\nevaluates branch_region\nagainst role's allowed region"}
    ROW -->|Fails| EMPTY["Rows filtered out\n(no error, zero rows for\nout-of-scope region)"]
    ROW -->|Passes| MASK{"Dynamic Masking Policy\non PII columns"}
    MASK -->|Role lacks PII grant| MASKED["Returns masked values\n(e.g., j***@domain.com)"]
    MASK -->|Role has PII grant\n(Finance/Compliance)| CLEAR["Returns unmasked values\n(fully audited via ACCESS_HISTORY)"]
```

---

## 10. Batch Schedule Timeline (Business Day View)

```mermaid
gantt
    title 4-Hour Batch Cycle (UTC) — Illustrative Single Day
    dateFormat  HH:mm
    axisFormat  %H:%M
    section Ingestion (Sources -> RAW)
    ingest_ota_v1        :done, i1, 00:00, 20m
    ingest_tmc_v1        :done, i2, 00:00, 15m
    ingest_retail_v1     :done, i3, 00:00, 10m
    section Transformation (RAW -> Gold)
    transform_curated_gold_v1 :active, t1, 00:20, 30m
    dq_monitoring_v1          :t2, 00:50, 15m
    section Next Cycle
    ingest_ota_v1_next   :i4, 04:00, 20m
    ingest_tmc_v1_next   :i5, 04:00, 15m
    ingest_retail_v1_next:i6, 04:00, 10m
```

*(Cycle repeats at 04:00, 08:00, 12:00, 16:00, 20:00 UTC — 6 cycles/day, meeting the 4–6 hour freshness SLA defined in Business Doc §6.)*

---

## 11. Diagram-to-Document Cross-Reference

| Diagram | Corresponding Technical Doc Section | Corresponding Business Doc Section |
|---|---|---|
| §1 System Context | §3 High-Level Architecture | §5 Data Sources |
| §2 Detailed Dataflow | §5 Snowflake Platform Design | §6 Ingestion & Refresh Cadence |
| §3 DAG Dependency Graph | §6.3–§6.5 Orchestration | §6 Ingestion & Refresh Cadence |
| §4 Incremental Sequence | §6.6 Incremental Load Logic | §6 Ingestion & Refresh Cadence |
| §5 Backfill Sequence | §6.7 Backfill Design | §6 Ingestion & Refresh Cadence, §9.3 Risk |
| §6 Incremental vs Backfill | §6.6–§6.7 | §6 |
| §7 Star Schema ERD | §8 Data Model | §7 Business Use Cases |
| §8 Identity Resolution | §5.4.1 Customer 360 Engine | §5.4 Cross-Source Business Keys |
| §9 Security Enforcement | §9 Security & Access Control | §9 Governance, Compliance & Risk |
| §10 Batch Timeline | §6.8 Scheduling Summary | §6 Ingestion & Refresh Cadence |

---

*End of ETL Dataflow Architecture Document. This is the third and final document of the ClientX Unified Analytics Platform documentation set, alongside `01_Business_and_Core_Document.md` and `02_Technical_Architecture_Document.md`.*
