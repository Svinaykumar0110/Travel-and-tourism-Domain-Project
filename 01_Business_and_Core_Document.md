# ClientX.com — Analytics Enablement Program
## Business & Core Document

| | |
|---|---|
| **Client** | ClientX.com (ClientX Travel & Tourism, ClientX Business, ClientX Retail) |
| **Program Name** | ClientX Unified Analytics Platform (MUAP) |
| **Document Type** | Business & Core Requirements Document |
| **Prepared For** | ClientX.com Leadership, Business Stakeholders |
| **Prepared By** | Analytics & Data Engineering Partner (We_X.ai) |
| **Version** | 1.0 |
| **Date** | 2025-07-22 |
| **Status** | Draft for Stakeholder Sign-off |
| **Companion Document** | `02_Technical_Architecture_Document.md`, `03_ETL_Dataflow_Architecture.md` |

---

## 1. Document Purpose

This document is the **business and core reference** for the ClientX.com Analytics Enablement Program. It exists to answer, in one place, every foundational business question a stakeholder — new or existing, technical or non-technical — could ask about this engagement:

- **WHO** is ClientX, who are the stakeholders, who owns what, who consumes the output.
- **WHAT** are we building, what problem does it solve, what is in and out of scope.
- **WHEN** does data arrive, when do stakeholders get insights, when does the program deliver value (roadmap).
- **WHERE** does the data originate, where does it end up, where is it governed.
- **WHY** is this program being undertaken, why now, why this architecture.
- **HOW** will success be measured, how will the business change its decisions as a result.

The companion **Technical Architecture Document** answers the corresponding engineering questions (how the pipelines, Snowflake objects, and Airflow DAGs are actually built). This document intentionally stays implementation-agnostic so it remains readable by CXOs, business unit heads, finance, and compliance stakeholders.

---

## 2. About ClientX.com (Business Context)

### 2.1 Company Overview

ClientX.com is a Dubai (DIFC)-headquartered travel and tourism company, originally founded as a retail travel agency in **Sharjah in 2005**, and formally incorporated as ClientX.com in **August 2007** by Sheikh Mohammed bin Abdulla Al Thani and Sachin Gadoya. In 2008, ClientX launched what is recognized as the **first online travel portal in the UAE**, evolving the company into a hybrid ("clicks-and-mortar") travel business.

Today ClientX operates across **UAE, Qatar, India, and Saudi Arabia**, has served **over 1 million customers**, runs **9+ physical branches/lounges**, and employs roughly **245+ staff**, with a Technology & Innovation Centre in Pune, India supporting global operations.

### 2.2 Business Segments (Why There Are 3 Data Sources)

ClientX's commercial model is built on **three distinct, independently-operated business lines** — this is the direct business reason the analytics platform must integrate three structured source systems:

| # | Business Segment | Customer Type | Channel | Core Products |
|---|---|---|---|---|
| 1 | **ClientX.com (OTA / D2C)** | Individual leisure travellers | Website + mobile app, self-serve booking engine | Flights (270+ airlines), hotels (1M+ properties), holiday packages, UAE tourist visas, global visa assistance |
| 2 | **ClientX Business (B2B / TMC)** | Corporates, SMEs, travel agents (2,000+ organizations) | Dedicated B2B portal + relationship managers | Corporate travel management, policy-compliant booking, MICE (Meetings, Incentives, Conferences, Exhibitions), expense/invoice management |
| 3 | **ClientX Retail** | Walk-in customers | Physical branches & lounges (UAE, Qatar, India) | Full-service in-person travel counselling, flights, hotels, holidays, visas, forex-adjacent services |

Each segment runs on its **own transactional system of record**, has its own customer base, pricing/commercial model, and operational KPIs — yet ClientX's leadership needs a **single, unified, trustworthy view** across all three to run the business. That fragmentation is precisely the business problem this program solves.

### 2.3 Why ClientX Needs This Program (Problem Statement)

ClientX engaged us because, in its current state:

1. **No single source of truth.** OTA, ClientX Business, and Retail data live in separate operational systems, each optimized for transactions, not analysis. Leadership cannot answer "what is our true blended revenue and margin this month across all three businesses?" without manual, error-prone spreadsheet reconciliation.
2. **Decisions are lagging, not real-time-informed.** Regional GMs (UAE, Qatar, India, KSA) and category heads (Flights, Hotels, Holidays, Visa) rely on delayed, manually-built reports rather than governed dashboards.
3. **No cross-segment customer view.** A traveller who books leisure through the OTA and later becomes a corporate account (or vice versa) is invisible as the "same" customer — losing cross-sell/up-sell opportunity and lifetime-value insight.
4. **Supplier and channel performance is opaque.** Airline/hotel supplier profitability, agent/branch productivity, and marketing channel ROI are not consistently measurable across segments.
5. **Compliance & reconciliation risk.** Finance needs to reconcile GDS/BSP airline settlements, payment gateway settlements, and corporate invoicing against actual bookings — currently a manual, high-risk monthly close process.
6. **No scalable foundation for growth.** As ClientX expands retail footprint (Qatar, more UAE stores) and grows ClientX Business, ad-hoc reporting does not scale.

### 2.4 Why Now

- ClientX is in an active **expansion phase** (new retail stores in Qatar/UAE, corporate footprint growth in KSA/India) — expansion decisions (where to open, which market to prioritize) require data, not intuition.
- Competitive pressure from other GCC OTAs (regional and global) makes **speed of insight** a competitive differentiator.
- Increasing regulatory scrutiny (UAE PDPL, payment card data handling, IATA/BSP settlement audit requirements) requires **governed, auditable data**, not spreadsheets.

---

## 3. Program Objectives

### 3.1 Primary Business Objectives

| Objective | Description | Primary Owner |
|---|---|---|
| O1 — Unified Revenue & Margin Visibility | Single, trusted, near-real-time (4-hour refresh) view of revenue, cost, and margin across OTA, Corporate, and Retail | CFO / Finance |
| O2 — 360° Customer View | Unify customer identities across all three segments to enable cross-sell, retention, and LTV analysis | CMO / Growth |
| O3 — Operational Performance Management | Branch, agent, and corporate account manager productivity dashboards | COO / Regional GMs |
| O4 — Supplier & Channel Performance | Airline/hotel supplier profitability, marketing channel attribution and ROI | Category Heads / Marketing |
| O5 — Corporate (TMC) Program Analytics | Policy compliance, cost savings, MICE event profitability for ClientX Business clients | Head of ClientX Business |
| O6 — Finance Reconciliation & Compliance | Automated reconciliation of bookings ↔ payments ↔ supplier settlement (BSP/GDS) ↔ invoicing | Finance / Compliance |
| O7 — Scalable Data Foundation | A governed, reusable Snowflake-based analytics platform that scales with new markets, products, and segments without re-architecture | CTO / Data & Analytics |

### 3.2 Explicit Non-Objectives (Out of Scope — Phase 1)

To keep scope honest and prevent scope creep, the following are **not** part of this phase:

- Real-time / sub-minute streaming analytics (the business requirement is a 4-hour batch cadence; this is **not** a real-time system).
- Unstructured data sources (call center recordings, chat transcripts, social media, review scraping) — all 3 sources are **structured** data only.
- Building a new customer-facing product or booking engine feature.
- Replacing existing operational systems (OTA booking engine, TMC platform, POS/branch system) — this program is analytics-only and reads from these systems; it does not modify them.
- Machine learning / predictive personalization (recommendation engines, dynamic pricing models) — explicitly flagged as a **Phase 2+** candidate once the curated data foundation (Gold layer) is stable.

---

## 4. Stakeholders — WHO Is Involved

### 4.1 Stakeholder Map

| Stakeholder | Role | Interest / What They Need |
|---|---|---|
| **CEO / Founders** | Executive sponsor | Company-wide revenue, growth, and market expansion visibility |
| **CFO & Finance Team** | Business owner (Finance) | Revenue/margin accuracy, reconciliation, audit trails, BSP/GDS settlement matching |
| **COO** | Business owner (Operations) | Cross-segment operational efficiency, branch & agent performance |
| **CMO / Marketing** | Business owner (Growth) | Customer 360, campaign attribution, channel ROI, retention/LTV |
| **Head of ClientX.com (OTA)** | Segment owner | Website/app booking funnel, conversion, ancillary attach rate |
| **Head of ClientX Business (TMC)** | Segment owner | Corporate account health, policy compliance, MICE profitability |
| **Head of Retail** | Segment owner | Branch/lounge footfall-to-booking conversion, agent productivity |
| **Regional GMs (UAE, Qatar, India, KSA)** | Regional owners | Region-level performance, expansion decision support |
| **Category Heads (Flights, Hotels, Holidays, Visa)** | Product owners | Product-line margin, supplier performance |
| **Compliance & Legal** | Governance | UAE PDPL, GDPR (India/EU travelers), PCI-DSS (payment data), IATA/BSP audit compliance |
| **CTO / Data & Engineering Leadership** | Technical sponsor | Platform scalability, cost governance, security |
| **Data & Analytics Team (ClientX)** | Platform operators (post-handover) | Ownership of dashboards, self-serve analytics, ongoing pipeline operations |
| **We_X.ai (Delivery Partner)** | Build & advisory | Design, build, document, and hand over the platform |

### 4.2 RACI — Program-Level

| Activity | CEO/CFO/COO | Segment Heads | Data/Analytics Team | We_X.ai (Delivery) | Compliance |
|---|---|---|---|---|---|
| Define business KPIs & priorities | A | R | C | C | I |
| Approve data governance & access policy | A | C | R | C | R |
| Source system access & credentials | I | A | R | R | I |
| Pipeline design & Snowflake architecture | I | I | A | R | C |
| Data quality sign-off per source | I | C | A | R | I |
| Dashboard/report design & sign-off | C | A | R | R | I |
| Production go-live approval | A | C | C | R | C |
| Ongoing operations (post-handover) | I | I | A/R | C (support) | I |

*(R = Responsible, A = Accountable, C = Consulted, I = Informed)*

---

## 5. The Three Data Sources — WHERE Data Comes From

All three sources are **structured, relational, transactional operational systems** — no unstructured or semi-structured (logs, JSON blobs, images, free text mining) data is in scope for Phase 1.

### 5.1 Source 1 — OTA / D2C Booking Platform ("ClientX.com")

- **System type:** Web & mobile booking engine backend (transactional relational database, e.g., PostgreSQL/MySQL-class OLTP system, or a booking-engine SaaS with a reporting DB replica).
- **Business owner:** Head of ClientX.com (OTA)
- **What it captures:** Customer accounts, search-to-booking funnel events (structured, not clickstream logs), flight/hotel/holiday/visa bookings, itineraries, passengers, add-ons/ancillaries, payments, cancellations, refunds, promo/coupon usage, customer support tickets tied to bookings.
- **Update pattern:** High transaction volume, continuous inserts/updates (booking status changes, payment confirmations, cancellations) throughout the day.
- **Why it matters:** This is ClientX's largest transaction-volume segment and primary driver of digital revenue and customer acquisition.

### 5.2 Source 2 — ClientX Business Platform ("Corporate / TMC")

- **System type:** B2B corporate travel management platform (relational OLTP database).
- **Business owner:** Head of ClientX Business
- **What it captures:** Corporate account master data, employee/traveler profiles, travel policy rules, trip requests & approvals, corporate bookings (flights/hotels/holidays), MICE event bookings, corporate invoicing, credit terms, account manager assignments, cost-center/expense tagging.
- **Update pattern:** Lower volume than OTA but higher value per transaction; approval workflows mean records are updated across multiple states (requested → approved → booked → invoiced).
- **Why it matters:** Represents ClientX's highest-margin, most strategic relationship-based revenue (2,000+ corporate clients); policy compliance and account health analytics are unique to this segment.

### 5.3 Source 3 — Retail / Branch POS System

- **System type:** Point-of-sale and branch operations system across 9+ physical branches/lounges (UAE, Qatar, India).
- **Business owner:** Head of Retail
- **What it captures:** Walk-in customer registrations, in-branch bookings (flights/hotels/holidays/visas), agent/staff assignment per transaction, cash/card/split settlement, branch-level daily till reconciliation, footfall/appointment logs (structured counters, not video).
- **Update pattern:** Business-hours-driven, branch-local time zones, end-of-day settlement batch.
- **Why it matters:** Only channel with direct human-to-human sales interaction; agent productivity and in-person conversion are unique, high-value operational metrics.

### 5.4 Cross-Source Business Keys (the basis of the 360° view)

| Shared Concept | OTA | ClientX Business | Retail | Unification Approach |
|---|---|---|---|---|
| Customer/Traveler identity | Customer email/phone/ID | Traveler profile linked to corporate account | Walk-in customer (name/phone/Emirates ID/passport) | Identity resolution via email/phone/document-ID matching + fuzzy match fallback, producing a `customer_master_key` |
| Booking / PNR | Booking ID / PNR | Trip ID / PNR | Branch transaction ID / PNR | GDS PNR (where present) used as a natural cross-system join key |
| Supplier (Airline/Hotel) | Supplier code | Supplier code | Supplier code | Conformed `dim_supplier` using IATA codes / hotel chain codes |
| Branch/Region | N/A (digital) | Servicing office | Branch code | Conformed `dim_geography` |
| Date/Time | Booking timestamp | Request/approval/booking timestamps | Transaction timestamp | Conformed `dim_date` / `dim_time`, all normalized to UTC + local business timezone attribute |

---

## 6. How the Business Will Consume Data — Ingestion & Refresh Cadence (Business View)

> Full technical detail is in the companion Technical Document. This section describes the **business-visible behavior**.

- **Ingestion mode:** Batch (not streaming/real-time). This was a deliberate, business-approved decision: none of the three source systems require sub-hourly analytics for the identified use cases (finance close, executive dashboards, operational reviews), and batch keeps cost and operational complexity proportionate to actual need.
- **Frequency:** Every **4 hours** (6 runs/day: e.g., 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC, aligned to ClientX's operating regions).
- **Incremental loads:** Each 4-hour run pulls only new/changed records since the last successful run (via change-tracking columns/timestamps) — this keeps runs fast and cheap.
- **Backfill capability:** The platform can always reprocess a historical date range (e.g., "reload all of Q1 2026 Retail data because the source system had a defect") without disrupting ongoing incremental loads — a business necessity for period-end restatements, source system bug corrections, and onboarding new historical data.
- **Data latency SLA (business commitment):** Any transaction posted in a source system will be reflected in dashboards **within 4–6 hours**, communicated clearly to stakeholders so expectations (e.g., "is this today's revenue number final?") are set correctly.
- **Why 4 hours, not real-time:** Confirmed with stakeholders — operational and financial decisions at ClientX are made on a daily/weekly cadence, not minute-by-minute. A 4-hour cadence gives near-fresh data at a fraction of the cost/complexity of streaming, and leaves headroom to move to intraday-hourly or event-driven streaming later if a specific use case justifies it (see Roadmap Phase 3).

---

## 7. Key Business Use Cases & Analytics Deliverables

### 7.1 Executive / Cross-Segment (CEO, CFO, COO)

1. **Unified Revenue & Margin Dashboard** — blended and segment-level revenue, cost of sale, gross margin, by product line (flights/hotels/holidays/visa), by region, by day/week/month, with prior-period and YoY comparisons.
2. **Customer 360 & Lifetime Value** — cross-segment customer view: has this customer booked via OTA, Retail, and/or is their employer a ClientX Business account; LTV, repeat-booking rate, churn risk flags.
3. **Company-Wide KPI Scorecard** — bookings, GMV (Gross Merchandise Value), net revenue, take rate, refund/cancellation rate, NPS (where available), all segments side by side.

### 7.2 ClientX.com (OTA) Analytics

4. **Booking Funnel & Conversion Analytics** — search → quote → booking → payment conversion, by product/device/region.
5. **Ancillary & Cross-sell Attach Rate** — % of flight bookings with hotel/holiday/visa attach; promo/coupon effectiveness.
6. **Cancellation & Refund Analytics** — rate, reasons (where captured), financial impact, supplier-level cancellation patterns.

### 7.3 ClientX Business (Corporate/TMC) Analytics

7. **Corporate Account Health Dashboard** — booking volume/value trend per corporate account, policy compliance rate, potential savings leakage (off-policy bookings), invoice aging.
8. **MICE / Event Profitability** — revenue vs. cost per event, by client, by region.
9. **Travel Manager / Account Manager Productivity** — bookings serviced, turnaround time on approvals, client satisfaction proxy metrics.

### 7.4 Retail Analytics

10. **Branch Performance Dashboard** — footfall-to-booking conversion, revenue per branch, revenue per agent, product mix by branch.
11. **Agent Productivity & Incentive Support** — bookings/revenue per agent, ranked leaderboards, commission-relevant metrics.
12. **Branch Expansion Decision Support** — regional demand density, product-mix trends to inform new branch site selection (directly supports ClientX's active Qatar/UAE retail expansion).

### 7.5 Supplier, Marketing & Finance (Cross-Cutting)

13. **Supplier (Airline/Hotel) Performance & Profitability** — volume, net rate/commission, cancellation impact, by supplier and by segment.
14. **Marketing Channel Attribution & ROI** (OTA-driven, informs whole-funnel view).
15. **Finance Reconciliation Dashboard** — bookings vs. payment gateway settlement vs. BSP/GDS airline settlement vs. corporate invoicing; exception/variance reporting to reduce manual month-end close effort.

---

## 8. Success Metrics — HOW We Measure Program Success

### 8.1 Business KPIs the Platform Must Report On (Gold-layer outputs)

| KPI Category | Example Metrics |
|---|---|
| Revenue & Growth | GMV, Net Revenue, Take Rate, Revenue Growth % (MoM/YoY), Revenue by Segment/Region/Product |
| Profitability | Gross Margin %, Cost of Sale, Supplier Commission/Incentive Realized |
| Customer | Active Customers, New vs. Repeat, LTV, Cross-Segment Penetration %, Churn Rate |
| Operational | Booking Volume, Average Booking Value, Cancellation Rate, Refund Turnaround Time |
| Corporate/TMC | Policy Compliance %, Corporate Account Growth, Off-Policy Spend Leakage, Invoice Aging (DSO) |
| Retail | Footfall-to-Booking Conversion %, Revenue per Branch, Revenue per Agent |
| Data Platform Health | Pipeline SLA adherence %, Data Freshness (hrs), Data Quality Pass Rate %, Reconciliation Variance % |

### 8.2 Program Delivery Success Criteria

- All 3 sources reliably ingested on the 4-hour batch cadence with **≥ 99% SLA adherence** (measured monthly).
- Data quality checks (completeness, uniqueness, referential integrity, reconciliation-to-source row counts) passing at **≥ 99.5%** per run, with automated alerting on failure.
- Finance able to reduce manual reconciliation effort by a target **≥ 50%** within 2 quarters of go-live.
- All defined executive and segment dashboards signed off by respective business owners (Section 4.1) and in active weekly use.
- Full backfill of at least 12 months of historical data across all 3 sources, validated against source-of-truth totals (row counts & revenue totals reconciled).
- Documented, governed access model in place (no direct raw-PII access without approved role).

---

## 9. Governance, Compliance & Risk — WHY Trust Matters

### 9.1 Data Sensitivity & Compliance Drivers

- **Personally Identifiable Information (PII):** Customer names, passport numbers, Emirates ID, phone, email, payment card metadata (tokenized, never raw PAN) — governed under **UAE PDPL (Federal Decree Law No. 45 of 2021)**, and where EU/India travelers are involved, mindful of **GDPR**-equivalent principles and **India DPDP Act** considerations.
- **Payment Data:** Payment references must follow **PCI-DSS** principles — the analytics platform never stores raw card PAN/CVV; only tokenized references and settlement metadata are ingested.
- **Travel Industry Settlement:** Airline ticketing data must remain reconcilable against **IATA BSP (Billing and Settlement Plan)** reports for audit purposes.

### 9.2 Data Governance Principles

1. **Least-privilege access** — role-based access control (RBAC) means no stakeholder sees more than their function requires (e.g., Retail branch managers see their branch/region, not global corporate account financials).
2. **PII masking by default** — analysts and dashboard consumers see masked/tokenized PII unless explicitly granted an approved, audited "PII access" role (e.g., Compliance, senior Finance).
3. **Single accountable data owner per source** — each segment head (Section 4.1) is the business owner of their source's data quality and correctness.
4. **Auditability** — every pipeline run, every schema change, every access grant is logged; every number in a dashboard must be traceable back to a source record.
5. **Change management** — schema or KPI-definition changes go through a lightweight governance review (Data & Analytics Team + affected business owner) before deployment.

### 9.3 Key Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Source system schema changes without notice | Broken pipelines, wrong numbers | Contract-based schema validation at ingestion; alerting on schema drift (detailed in Technical Doc) |
| Identity resolution errors across 3 sources (wrong customer merge) | Incorrect 360°/LTV metrics | Conservative, auditable matching rules; manual review queue for low-confidence matches |
| Late or missing source data | Delayed/incorrect KPI numbers within a cycle | SLA monitoring, automated backfill runbook, "data freshness" flag surfaced on dashboards |
| Access control misconfiguration exposing PII | Compliance breach | Automated masking policies applied at the Snowflake object level (not dashboard level), periodic access review |
| Reconciliation mismatch (bookings vs. payments vs. settlement) | Financial misstatement risk | Dedicated reconciliation Gold-layer models with automated variance alerting |
| Cost overrun (compute) | Budget risk | Warehouse-level resource monitors, workload-appropriate warehouse sizing (Technical Doc §Cost Governance) |

---

## 10. Program Roadmap

| Phase | Timeline (indicative) | Scope |
|---|---|---|
| **Phase 0 — Discovery & Design** | Weeks 1–3 | Source system profiling, access provisioning, data contracts, KPI/metric sign-off, architecture design (this document + Technical Doc) |
| **Phase 1 — Foundation Build** | Weeks 4–10 | Snowflake environment setup, Airflow orchestration, ingestion of all 3 sources (Raw → Curated), historical backfill (12 months), core data quality framework |
| **Phase 2 — Analytics & Dashboards (MVP)** | Weeks 8–14 (overlapping) | Gold-layer marts, executive scorecard, segment dashboards (Section 7), reconciliation dashboard, UAT & sign-off |
| **Phase 3 — Hardening & Handover** | Weeks 14–16 | Performance/cost tuning, governance sign-off, documentation, knowledge transfer to ClientX's Data & Analytics team |
| **Phase 4 — Advanced Analytics (Future)** | Post-handover | Candidate for ML-based demand forecasting, dynamic pricing support, next-best-offer recommendations, and — if a business case emerges — a move toward intraday/streaming ingestion for specific high-urgency use cases |

---

## 11. Assumptions

1. All three source systems expose (or can be configured to expose) a mechanism for **incremental extraction** (e.g., a last-modified timestamp column, change log table, or CDC-capable connector).
2. ClientX will provide read-only access credentials to each source system (or its reporting replica) — the platform never writes back to operational systems.
3. ClientX's Finance team will provide the current manual reconciliation logic (BSP/GDS ↔ payments ↔ bookings) so it can be codified into automated Gold-layer models.
4. Historical data of at least 12 months is available and extractable from all three sources for backfill.
5. ClientX's Compliance/Legal function will formally review and approve the PII handling and masking policy prior to production go-live.
6. Snowflake is the pre-approved, standardized data platform for this program (per ClientX's technology decision), and Apache Airflow is the pre-approved orchestration tool.

---

## 12. Glossary (Business Terms)

| Term | Meaning |
|---|---|
| OTA | Online Travel Agency — ClientX.com's direct-to-consumer digital channel |
| TMC | Travel Management Company — the category ClientX Business operates in (corporate travel) |
| MICE | Meetings, Incentives, Conferences, Exhibitions — corporate event travel category |
| GMV | Gross Merchandise Value — total value of bookings transacted, before deducting costs |
| Take Rate | Net revenue as a % of GMV |
| PNR | Passenger Name Record — the booking reference used across airline/GDS systems |
| BSP | Billing and Settlement Plan (IATA) — the airline industry's ticket settlement/reconciliation system |
| GDS | Global Distribution System — the reservation systems (e.g., Amadeus, Sabre, Travelport) used to book flights/hotels |
| LTV | Customer Lifetime Value |
| SLA | Service Level Agreement — here, the committed data freshness/availability target (4–6 hours) |
| PDPL | UAE Personal Data Protection Law |
| RBAC | Role-Based Access Control |

---

## 13. Sign-off

| Stakeholder | Name | Role | Signature/Approval | Date |
|---|---|---|---|---|
| Executive Sponsor | | CEO/CFO | | |
| Head of ClientX.com (OTA) | | Segment Owner | | |
| Head of ClientX Business | | Segment Owner | | |
| Head of Retail | | Segment Owner | | |
| Compliance/Legal | | Governance | | |
| CTO / Data Leadership | | Technical Sponsor | | |

---

*End of Business & Core Document. See `02_Technical_Architecture_Document.md` for full technical design and `03_ETL_Dataflow_Architecture.md` for the detailed dataflow/ETL architecture diagrams.*
