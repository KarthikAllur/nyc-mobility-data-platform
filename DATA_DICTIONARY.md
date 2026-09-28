# Data Dictionary

## NYC Mobility Analytics Platform

This file defines every field in every table across Bronze, Silver, and Gold layers.
Updated as new tables are created.

---

## Status: Phase 0 - Setup

Tables will be defined as each layer is built.

| Layer | Table | Status |
|-------|-------|--------|
| Bronze | tlc_trips_raw | Defined in Phase 3 |
| Bronze | zones_raw | Defined in Phase 3 |
| Bronze | weather_raw | Defined in Phase 3 |
| Bronze | azure_sql_raw | Defined in Phase 3 |
| Silver | trips | Defined in Phase 6 |
| Silver | zones | Defined in Phase 6 |
| Silver | weather | Defined in Phase 6 |
| Gold | fact_trips | Defined in Phase 7 |
| Gold | dim_zone | Defined in Phase 7 |
| Gold | dim_date | Defined in Phase 7 |
| Gold | dim_weather | Defined in Phase 7 |

---

## Naming Conventions

| Convention | Rule | Example |
|------------|------|---------|
| Table names | snake_case, lowercase | fact_trips |
| Column names | snake_case, lowercase | pickup_datetime |
| Date columns | _date suffix | trip_date |
| Timestamp columns | _datetime suffix | pickup_datetime |
| ID columns | _id suffix | zone_id |
| Surrogate keys | sk_ prefix | sk_zone |
| Flags | is_ prefix | is_valid |
| Audit columns | _ingested_at, _updated_at | ingested_at |

