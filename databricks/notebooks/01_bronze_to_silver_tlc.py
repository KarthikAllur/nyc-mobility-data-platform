# Databricks notebook source
# ── Pipeline parameters — passed by ADF (pl_master_end_to_end) ──
import re
from datetime import date

dbutils.widgets.text("YearMonth", "2024-01", "YearMonth (yyyy-MM)")
year_month = dbutils.widgets.get("YearMonth").strip()

# Fail fast on a malformed parameter
if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", year_month):
    raise ValueError(f"Invalid YearMonth '{year_month}'. Expected yyyy-MM, e.g. 2024-02")

year, month  = map(int, year_month.split("-"))
period_start = date(year, month, 1).isoformat()                                   # inclusive
period_end   = date(year + (month == 12), month % 12 + 1, 1).isoformat()          # exclusive (1st of next month)

# ── Centralised paths (single source of truth for every cell below) ──
storage_root = "abfss://mobility@stnycmobilitydevka.dfs.core.windows.net"

# Bronze inputs
bronze_path  = f"{storage_root}/bronze/tlc/yellow_tripdata_{year_month}.parquet"
weather_path = f"{storage_root}/bronze/weather/weather_{year_month}.json"
zone_path    = f"{storage_root}/bronze/zones/taxi_zone_lookup.csv"           # static dimension

# Silver outputs
silver_path             = f"{storage_root}/silver/tlc/yellow_tripdata_{year_month}"
quarantine_path         = f"{storage_root}/silver_quarantine/tlc/yellow_tripdata_{year_month}"
weather_silver_path     = f"{storage_root}/silver/weather/hourly_nyc_{year_month}"
weather_quarantine_path = f"{storage_root}/silver_quarantine/weather/hourly_nyc_{year_month}"
zone_silver_path        = f"{storage_root}/silver/zones/taxi_zone_lookup"
zone_quarantine_path    = f"{storage_root}/silver_quarantine/zones/taxi_zone_lookup"

# Gold outputs
gold_trips_path  = f"{storage_root}/gold/tlc/enriched_trips_{year_month}"
gold_daily_path  = f"{storage_root}/gold/tlc/daily_borough_summary_{year_month}"
gold_hourly_path = f"{storage_root}/gold/tlc/hourly_demand_weather_{year_month}"

# ── Fail fast if a Bronze input for this month has not landed yet ──
missing = []
for p in (bronze_path, weather_path, zone_path):
    try:
        dbutils.fs.ls(p)
    except Exception as e:
        missing.append(f"{p}  ({type(e).__name__}: {str(e)[:150]})")
if missing:
    raise FileNotFoundError("Bronze input(s) missing for " + year_month + ":\n  " + "\n  ".join(missing))

print(f"YearMonth : {year_month}")
print(f"Period    : {period_start} (inclusive) → {period_end} (exclusive)")
print(f"Bronze    : trips, weather, zones found ✓")

# COMMAND ----------

# 1. Read the raw Bronze Parquet file for the requested YearMonth (bronze_path is set in the Parameters cell)
df_raw = spark.read.parquet(bronze_path)

# 2. View the first 10 rows
display(df_raw.limit(10))

# 3. Print the total row count
print(f"Total raw trips for {year_month}: {df_raw.count():,}")


# COMMAND ----------

# Check for dirty data in raw Bronze
print("Checking data quality issues in Bronze:")

# 1. Trips with 0 passengers
zero_passengers = df_raw.filter(df_raw.passenger_count == 0).count()
print(f"• Trips with 0 passengers: {zero_passengers:,}")

# 2. Trips with negative or zero fare amount
negative_fares = df_raw.filter(df_raw.fare_amount <= 0).count()
print(f"• Trips with zero/negative fare: {negative_fares:,}")

# 3. Trips with zero distance
zero_distance = df_raw.filter(df_raw.trip_distance <= 0).count()
print(f"• Trips with 0 distance: {zero_distance:,}")

# 4. Outlier trips (distance > 100 miles)
outlier_distance = df_raw.filter(df_raw.trip_distance > 100).count()
print(f"• Outlier trips (> 100 miles): {outlier_distance:,}")


# COMMAND ----------

from pyspark.sql import functions as F

# ── 1. Date-range filter: keep only pickups in the requested YearMonth ──
# period_start / period_end (exclusive) are set in the Parameters cell
df_dated = df_raw.filter(
    (F.col("tpep_pickup_datetime") >= period_start) &
    (F.col("tpep_pickup_datetime") <  period_end)
)

# ── 2. Rename columns to snake_case (early, for downstream consistency) ──
df_renamed = (
    df_dated
    .withColumnRenamed("VendorID",     "vendor_id")
    .withColumnRenamed("RatecodeID",   "ratecode_id")
    .withColumnRenamed("PULocationID", "pu_location_id")
    .withColumnRenamed("DOLocationID", "do_location_id")
    .withColumnRenamed("Airport_fee",  "airport_fee")
)

# ── 3. Derive columns + lineage metadata ──
df_enriched = (
    df_renamed
    .withColumn(
        "trip_duration_minutes",
        F.round(
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 60, 2
        )
    )
    .withColumn(
        "average_speed_mph",
        F.round(
            F.when(F.col("trip_duration_minutes") > 0,
                   F.col("trip_distance") / (F.col("trip_duration_minutes") / 60))
            .otherwise(None), 2
        )
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
)

# ── 4. Compute expected total for cross-check ──
fee_cols = ["fare_amount", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "improvement_surcharge", "congestion_surcharge", "airport_fee"]
expected_total = sum(F.coalesce(F.col(c), F.lit(0.0)) for c in fee_cols)

# ── 5. Tag rejection reasons (quarantine pattern) ──
df_flagged = df_enriched.withColumn(
    "_reject_reasons",
    F.array_compact(F.array(
        F.when(F.col("fare_amount") <= 0,           F.lit("non_positive_fare")),
        F.when(F.col("trip_distance") <= 0,          F.lit("zero_distance")),
        F.when(F.col("trip_distance") > 100,         F.lit("outlier_distance_gt_100mi")),
        F.when(F.col("passenger_count") <= 0,        F.lit("zero_passengers")),
        F.when(F.col("trip_duration_minutes") <= 0,  F.lit("non_positive_duration")),
        F.when(F.col("average_speed_mph") > 80,      F.lit("speed_gt_80mph")),
        F.when(F.col("fare_amount") > 500,           F.lit("extreme_fare_gt_500")),
        F.when(F.abs(expected_total - F.col("total_amount")) > 1.0,
               F.lit("total_amount_mismatch")),
    ))
)

# ── 6. Split: clean Silver vs quarantine ──
df_silver     = df_flagged.filter(F.size("_reject_reasons") == 0).drop("_reject_reasons")
df_quarantine = df_flagged.filter(F.size("_reject_reasons") > 0)

# ── 7. Write Silver Delta (partitioned by pickup_date) ──
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("pickup_date").save(silver_path)

# ── 8. Write quarantine Delta ──
df_quarantine.write.format("delta").mode("overwrite").save(quarantine_path)

# ── Summary ──
silver_count = spark.read.format("delta").load(silver_path).count()
quarantine_count = spark.read.format("delta").load(quarantine_path).count()
print(f"✅ Silver:     {silver_count:,} rows → {silver_path}")
print(f"🔶 Quarantine: {quarantine_count:,} rows → {quarantine_path}")
display(df_silver.limit(100))

# COMMAND ----------

from pyspark.sql import functions as F

# Read back the Silver Delta to profile what's already written
df = spark.read.format("delta").load(silver_path)
total = df.count()
print(f"Silver rows: {total:,}\n")

# ── 1. Null audit across all columns ──
print("=== NULL COUNTS ===")
null_counts = df.select(
    *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
).collect()[0]
for c in df.columns:
    n = null_counts[c]
    if n and n > 0:
        print(f"  {c}: {n:,} nulls ({n/total*100:.2f}%)")

# ── 2. Negative / zero trip durations (dropoff <= pickup) ──
bad_duration = df.filter(F.col("trip_duration_minutes") <= 0).count()
print(f"\n=== DURATION ISSUES ===")
print(f"  Trips with duration <= 0 min: {bad_duration:,}")

# ── 3. Unreasonable speeds (> 80 mph avg in NYC) ──
high_speed = df.filter(F.col("average_speed_mph") > 80).count()
print(f"\n=== SPEED OUTLIERS ===")
print(f"  Trips with avg speed > 80 mph: {high_speed:,}")

# ── 4. Extreme fare outliers ──
fare_stats = df.select(
    F.percentile_approx("fare_amount", 0.99).alias("p99"),
    F.percentile_approx("fare_amount", 0.999).alias("p999"),
    F.max("fare_amount").alias("max_fare")
).collect()[0]
print(f"\n=== FARE DISTRIBUTION ===")
print(f"  P99: ${fare_stats['p99']:.2f}  |  P99.9: ${fare_stats['p999']:.2f}  |  Max: ${fare_stats['max_fare']:.2f}")
extreme_fare = df.filter(F.col("fare_amount") > 500).count()
print(f"  Trips with fare > $500: {extreme_fare:,}")

# ── 5. Duplicate rows (exact duplicates on business key) ──
biz_keys = ["vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "pu_location_id", "do_location_id", "fare_amount", "trip_distance"]
total_bk = df.count()
distinct_bk = df.dropDuplicates(biz_keys).count()
print(f"\n=== DUPLICATE CHECK (on business key) ===")
print(f"  Total: {total_bk:,}  |  Distinct: {distinct_bk:,}  |  Dupes: {total_bk - distinct_bk:,}")

# ── 6. Domain-value validation ──
print(f"\n=== DOMAIN VALUE CHECKS ===")
invalid_rate = df.filter(~F.col("ratecode_id").isin(1,2,3,4,5,6,99)).count()
print(f"  Invalid ratecode_id (not 1-6/99): {invalid_rate:,}")
invalid_pay = df.filter(~F.col("payment_type").isin(0,1,2,3,4,5,6)).count()
print(f"  Invalid payment_type (not 0-6): {invalid_pay:,}")
invalid_flag = df.filter(~F.col("store_and_fwd_flag").isin("Y","N")).count()
print(f"  Invalid store_and_fwd_flag (not Y/N): {invalid_flag:,}")

# ── 7. Tip > total (logical impossibility) ──
tip_gt_total = df.filter(F.col("tip_amount") > F.col("total_amount")).count()
print(f"  Tip > total_amount: {tip_gt_total:,}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# ── 1. Read nested Bronze JSON ──
df_w_raw = spark.read.option("multiLine", "true").json(weather_path)

# ── 2. Flatten: zip parallel arrays → explode into one row per hour ──
df_flat = (
    df_w_raw
    .select(
        F.col("latitude"), F.col("longitude"), F.col("elevation"),
        F.col("timezone"),
        F.posexplode(
            F.arrays_zip(
                F.col("hourly.time"),
                F.col("hourly.temperature_2m"),
                F.col("hourly.precipitation"),
                F.col("hourly.wind_speed_10m")
            )
        ).alias("pos", "hourly_row")
    )
    .select(
        "latitude", "longitude", "elevation", "timezone",
        F.col("hourly_row.time").alias("observation_time_str"),
        F.col("hourly_row.temperature_2m").alias("temperature_c"),
        F.col("hourly_row.precipitation").alias("precipitation_mm"),
        F.col("hourly_row.wind_speed_10m").alias("wind_speed_kmh"),
    )
)

# ── 3. Parse timestamp + derive columns ──
df_enriched = (
    df_flat
    .withColumn("observation_time", F.to_timestamp("observation_time_str", "yyyy-MM-dd'T'HH:mm"))
    .drop("observation_time_str")
    .withColumn("observation_date", F.to_date("observation_time"))
    .withColumn("hour_of_day", F.hour("observation_time"))
    # Convert Celsius → Fahrenheit
    .withColumn("temperature_f", F.round(F.col("temperature_c") * 9 / 5 + 32, 1))
    # Convert km/h → mph
    .withColumn("wind_speed_mph", F.round(F.col("wind_speed_kmh") * 0.621371, 1))
    # Precipitation category
    .withColumn("precipitation_category",
        F.when(F.col("precipitation_mm") == 0, "none")
         .when(F.col("precipitation_mm") <= 2.5, "light")
         .when(F.col("precipitation_mm") <= 7.5, "moderate")
         .otherwise("heavy")
    )
    .withColumn("_ingested_at", F.current_timestamp())
)

# ── 4. Date-range validation (requested YearMonth only) ──
df_period = df_enriched.filter(
    (F.col("observation_date") >= period_start) &
    (F.col("observation_date") <  period_end)
)

# ── 5. Quarantine: null measurements ──
df_flagged = df_period.withColumn(
    "_reject_reasons",
    F.array_compact(F.array(
        F.when(F.col("observation_time").isNull(), F.lit("null_observation_time")),
        F.when(F.col("temperature_c").isNull(),    F.lit("null_temperature")),
        F.when(F.col("precipitation_mm").isNull(),  F.lit("null_precipitation")),
        F.when(F.col("wind_speed_kmh").isNull(),    F.lit("null_wind_speed")),
        F.when(F.col("temperature_c") < -50,        F.lit("extreme_cold_below_neg50c")),
        F.when(F.col("temperature_c") > 60,         F.lit("extreme_heat_above_60c")),
        F.when(F.col("wind_speed_kmh") < 0,         F.lit("negative_wind_speed")),
    ))
)

df_weather_silver = df_flagged.filter(F.size("_reject_reasons") == 0).drop("_reject_reasons")
df_weather_quarantine = df_flagged.filter(F.size("_reject_reasons") > 0)

# ── 6. Write Silver + Quarantine ──
df_weather_silver.write.format("delta").mode("overwrite").partitionBy("observation_date").save(weather_silver_path)

if df_weather_quarantine.count() > 0:
    df_weather_quarantine.write.format("delta").mode("overwrite").save(weather_quarantine_path)
    q_count = df_weather_quarantine.count()
else:
    q_count = 0

silver_count = spark.read.format("delta").load(weather_silver_path).count()
print(f"✅ Weather Silver: {silver_count:,} hourly rows → {weather_silver_path}")
print(f"🔶 Weather Quarantine: {q_count:,} rows")
display(df_weather_silver.orderBy("observation_time").limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

# ── 1. Read Bronze CSV ──
df_z_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_path)

# ── 2. Rename to snake_case ──
df_renamed = (
    df_z_raw
    .withColumnRenamed("LocationID", "location_id")
    .withColumnRenamed("Borough",    "borough")
    .withColumnRenamed("Zone",       "zone_name")
)

# ── 3. Standardise sentinel values + trim whitespace ──
df_clean = (
    df_renamed
    .withColumn("borough",
        F.when(F.upper(F.trim("borough")).isin("N/A", "NA", "UNKNOWN", ""), F.lit("Unknown"))
         .otherwise(F.trim("borough"))
    )
    .withColumn("zone_name", F.trim("zone_name"))
    .withColumn("service_zone",
        F.when(F.upper(F.trim("service_zone")).isin("N/A", "NA", ""), F.lit("Unknown"))
         .otherwise(F.trim("service_zone"))
    )
)

# ── 4. Quarantine: missing primary key or zone name ──
df_flagged = df_clean.withColumn(
    "_reject_reasons",
    F.array_compact(F.array(
        F.when(F.col("location_id").isNull(), F.lit("null_location_id")),
        F.when(F.col("zone_name").isNull() | (F.col("zone_name") == ""), F.lit("empty_zone_name")),
    ))
)

df_zone_silver = df_flagged.filter(F.size("_reject_reasons") == 0).drop("_reject_reasons")
df_zone_quarantine = df_flagged.filter(F.size("_reject_reasons") > 0)

# ── 5. Validate uniqueness on location_id (primary key check) ──
total_rows = df_zone_silver.count()
distinct_ids = df_zone_silver.select("location_id").distinct().count()
assert total_rows == distinct_ids, f"Duplicate location_ids detected: {total_rows} rows vs {distinct_ids} distinct"

# ── 6. Add lineage metadata + write Silver Delta ──
df_zone_silver = df_zone_silver.withColumn("_ingested_at", F.current_timestamp())

df_zone_silver.write.format("delta").mode("overwrite").save(zone_silver_path)

if df_zone_quarantine.count() > 0:
    df_zone_quarantine.write.format("delta").mode("overwrite").save(zone_quarantine_path)
    zq_count = df_zone_quarantine.count()
else:
    zq_count = 0

zone_count = spark.read.format("delta").load(zone_silver_path).count()
print(f"✅ Zone Silver: {zone_count:,} zones → {zone_silver_path}")
print(f"🔶 Zone Quarantine: {zq_count:,} rows")
print(f"   PK uniqueness: ✓ ({distinct_ids} distinct location_ids)")
display(df_zone_silver.orderBy("location_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze → Silver Data Quality Summary
# MAGIC 
# MAGIC > **Pipeline**: `01_bronze_to_silver_tlc` &nbsp;|&nbsp; **Period**: parameter-driven via `YearMonth` widget (figures below are from the 2024-01 reference run) &nbsp;|&nbsp; **Storage**: ADLS Gen2 (`stnycmobilitydevka / mobility`)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 1. Row Counts
# MAGIC 
# MAGIC | Dataset | Bronze (Raw) | Silver (Clean) | Quarantined | Drop Rate |
# MAGIC |---|---:|---:|---:|---:|
# MAGIC | **Yellow Taxi Trips** | 2,964,624 | 2,133,221 | 831,385 | 28.0% |
# MAGIC | **Hourly Weather** | 1 JSON (744 hours) | 744 | 0 | 0.0% |
# MAGIC | **Taxi Zone Lookup** | 265 | 265 | 0 | 0.0% |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 2. Trip Quality Rules (Quarantine Reasons)
# MAGIC 
# MAGIC Each rejected trip carries an `_reject_reasons` array — a single row can trigger multiple rules.
# MAGIC 
# MAGIC | Rule | Condition | Rationale |
# MAGIC |---|---|---|
# MAGIC | `non_positive_fare` | `fare_amount <= 0` | Cancelled/voided or data entry error |
# MAGIC | `zero_distance` | `trip_distance <= 0` | Meter not engaged or GPS failure |
# MAGIC | `outlier_distance_gt_100mi` | `trip_distance > 100` | Physically implausible for NYC yellow cab |
# MAGIC | `zero_passengers` | `passenger_count <= 0` | Missing or corrupt passenger data |
# MAGIC | `non_positive_duration` | `trip_duration_minutes <= 0` | Dropoff timestamp before pickup |
# MAGIC | `speed_gt_80mph` | `average_speed_mph > 80` | Impossible average speed in NYC traffic |
# MAGIC | `extreme_fare_gt_500` | `fare_amount > 500` | Outlier beyond P99.9 ($135); likely erroneous |
# MAGIC | `total_amount_mismatch` | \|sum(fees) − total_amount\| > $1.00 | Fee components don't reconcile with reported total |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 3. Derived Columns
# MAGIC 
# MAGIC ### Trips
# MAGIC | Column | Formula | Purpose |
# MAGIC |---|---|---|
# MAGIC | `trip_duration_minutes` | (dropoff − pickup) / 60 | Core trip metric |
# MAGIC | `average_speed_mph` | distance / (duration in hours) | Quality filter + analytics |
# MAGIC | `pickup_date` | date(pickup_datetime) | Partition key for query performance |
# MAGIC | `_ingested_at` | current_timestamp() | Lineage tracking |
# MAGIC 
# MAGIC ### Weather
# MAGIC | Column | Formula | Purpose |
# MAGIC |---|---|---|
# MAGIC | `temperature_f` | °C × 9/5 + 32 | US-friendly unit |
# MAGIC | `wind_speed_mph` | km/h × 0.621371 | US-friendly unit |
# MAGIC | `precipitation_category` | none / light (≤2.5mm) / moderate (≤7.5mm) / heavy | Categorical bucketing for joins |
# MAGIC | `hour_of_day` | hour(observation_time) | Join key with trips |
# MAGIC | `observation_date` | date(observation_time) | Partition key |
# MAGIC 
# MAGIC ### Zones
# MAGIC | Transform | Detail |
# MAGIC |---|---|
# MAGIC | snake_case rename | `LocationID` → `location_id`, `Borough` → `borough`, `Zone` → `zone_name` |
# MAGIC | Sentinel standardisation | `N/A`, `NA`, `Unknown`, blank → `"Unknown"` |
# MAGIC | PK uniqueness assertion | 265 distinct `location_id` values verified |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 4. Silver Output Paths (Delta)
# MAGIC 
# MAGIC | Dataset | Path | Partitioned By |
# MAGIC |---|---|---|
# MAGIC | Trips | `.../silver/tlc/yellow_tripdata_{YearMonth}` | `pickup_date` |
# MAGIC | Weather | `.../silver/weather/hourly_nyc_{YearMonth}` | `observation_date` |
# MAGIC | Zones | `.../silver/zones/taxi_zone_lookup` | — (small dimension) |
# MAGIC | Trips Quarantine | `.../silver_quarantine/tlc/yellow_tripdata_{YearMonth}` | — |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 5. Gold-Layer Join Keys
# MAGIC 
# MAGIC | Join | Left Key | Right Key |
# MAGIC |---|---|---|
# MAGIC | Trips → Zones (pickup) | `pu_location_id` | `location_id` |
# MAGIC | Trips → Zones (dropoff) | `do_location_id` | `location_id` |
# MAGIC | Trips → Weather | `pickup_date` + `hour(tpep_pickup_datetime)` | `observation_date` + `hour_of_day` |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 6. Gold Layer Output
# MAGIC 
# MAGIC | Gold Table | Rows | Grain | Path |
# MAGIC |---|---:|---|---|
# MAGIC | **Enriched Trips** | 2,133,221 | 1 per trip | `.../gold/tlc/enriched_trips_{YearMonth}` |
# MAGIC | **Daily Borough Summary** | 201 | 1 per date × borough | `.../gold/tlc/daily_borough_summary_{YearMonth}` |
# MAGIC | **Hourly Demand & Weather** | 744 | 1 per date × hour | `.../gold/tlc/hourly_demand_weather_{YearMonth}` |
# MAGIC 
# MAGIC **Join integrity verified**: Enriched Trips (2,133,221) = Silver Trips (2,133,221) — no fan-out from zone or weather joins.
# MAGIC 
# MAGIC ### Gold Columns Added (Enriched Trips)
# MAGIC | Column | Source | Join Key |
# MAGIC |---|---|---|
# MAGIC | `pickup_borough`, `pickup_zone`, `pickup_service_zone` | Zones | `pu_location_id` = `location_id` |
# MAGIC | `dropoff_borough`, `dropoff_zone`, `dropoff_service_zone` | Zones | `do_location_id` = `location_id` |
# MAGIC | `temperature_f`, `precipitation_mm`, `precipitation_category`, `wind_speed_mph` | Weather | `pickup_date` + `pickup_hour` = `observation_date` + `hour_of_day` |
# MAGIC 
# MAGIC ### Daily Borough Summary KPIs
# MAGIC `trip_count`, `total_revenue`, `avg_fare`, `avg_duration_min`, `avg_distance_mi`, `avg_speed_mph`, `avg_tip`, `avg_temperature_f`, `avg_precipitation_mm`, `unique_pickup_zones`
# MAGIC 
# MAGIC ### Hourly Demand & Weather KPIs
# MAGIC `trip_count`, `total_revenue`, `avg_fare`, `avg_duration_min`, `avg_distance_mi`, `temperature_f`, `precipitation_mm`, `precipitation_category`, `wind_speed_mph`, `unique_pickup_zones`, `unique_dropoff_zones`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Gold Layer — Aggregated Analytics Tables
# MAGIC 
# MAGIC > Joins the three Silver datasets (trips, zones, weather) and produces denormalised, analytics-ready Gold tables.
# MAGIC 
# MAGIC | Gold Table | Grain | Purpose |
# MAGIC |---|---|---|
# MAGIC | **Enriched Trips** | 1 row per trip | Denormalised fact: trips + zone names + weather conditions |
# MAGIC | **Daily Borough Summary** | 1 row per date × borough | Daily KPIs by borough for dashboards |
# MAGIC | **Hourly Demand & Weather** | 1 row per date × hour | Demand patterns correlated with weather |

# COMMAND ----------

from pyspark.sql import functions as F

# ── 1. Load Silver tables ──
df_trips   = spark.read.format("delta").load(silver_path)
df_zones   = spark.read.format("delta").load(zone_silver_path)
df_weather = spark.read.format("delta").load(weather_silver_path)

# ── 2. Join trips → zones (pickup) ──
df_pu = (
    df_trips
    .join(
        df_zones.select(
            F.col("location_id").alias("pu_location_id"),
            F.col("borough").alias("pickup_borough"),
            F.col("zone_name").alias("pickup_zone"),
            F.col("service_zone").alias("pickup_service_zone")
        ),
        on="pu_location_id",
        how="left"
    )
)

# ── 3. Join trips → zones (dropoff) ──
df_zones_do = df_zones.select(
    F.col("location_id").alias("do_location_id"),
    F.col("borough").alias("dropoff_borough"),
    F.col("zone_name").alias("dropoff_zone"),
    F.col("service_zone").alias("dropoff_service_zone")
)
df_with_zones = df_pu.join(df_zones_do, on="do_location_id", how="left")

# ── 4. Join trips → weather (date + hour) ──
df_gold_trips = (
    df_with_zones
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .join(
        df_weather.select(
            F.col("observation_date"),
            F.col("hour_of_day"),
            F.col("temperature_f"),
            F.col("precipitation_mm"),
            F.col("precipitation_category"),
            F.col("wind_speed_mph")
        ),
        on=[
            df_with_zones["pickup_date"] == df_weather["observation_date"],
            F.hour(df_with_zones["tpep_pickup_datetime"]) == df_weather["hour_of_day"]
        ],
        how="left"
    )
    .drop("observation_date", "hour_of_day")
)

# ── 5. Validate no fan-out from joins ──
silver_count = df_trips.count()
gold_count   = df_gold_trips.count()
assert gold_count == silver_count, (
    f"Join fan-out detected! Silver: {silver_count:,} vs Gold: {gold_count:,}"
)

# ── 6. Write Gold enriched trips ──
df_gold_trips.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("pickup_date").save(gold_trips_path)

print(f"✅ Gold Enriched Trips: {gold_count:,} rows → {gold_trips_path}")
print(f"   Join integrity: Silver {silver_count:,} = Gold {gold_count:,} ✓")
display(df_gold_trips.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

# ── Aggregate: 1 row per date × pickup borough ──
df_daily_borough = (
    df_gold_trips
    .groupBy("pickup_date", "pickup_borough")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
        F.round(F.avg("average_speed_mph"), 2).alias("avg_speed_mph"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.avg("temperature_f"), 1).alias("avg_temperature_f"),
        F.round(F.avg("precipitation_mm"), 2).alias("avg_precipitation_mm"),
        F.countDistinct("pu_location_id").alias("unique_pickup_zones")
    )
    .orderBy("pickup_date", "pickup_borough")
)

# ── Write ──
df_daily_borough.write.format("delta").mode("overwrite").save(gold_daily_path)

row_count = spark.read.format("delta").load(gold_daily_path).count()
print(f"✅ Gold Daily Borough Summary: {row_count:,} rows → {gold_daily_path}")
display(df_daily_borough)

# COMMAND ----------

from pyspark.sql import functions as F

# ── Aggregate: 1 row per date × hour ──
df_hourly_demand = (
    df_gold_trips
    .groupBy("pickup_date", "pickup_hour")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
        # Weather (same for all trips in the same hour)
        F.first("temperature_f").alias("temperature_f"),
        F.first("precipitation_mm").alias("precipitation_mm"),
        F.first("precipitation_category").alias("precipitation_category"),
        F.first("wind_speed_mph").alias("wind_speed_mph"),
        # Demand indicators
        F.countDistinct("pu_location_id").alias("unique_pickup_zones"),
        F.countDistinct("do_location_id").alias("unique_dropoff_zones")
    )
    .orderBy("pickup_date", "pickup_hour")
)

# ── Write ──
df_hourly_demand.write.format("delta").mode("overwrite").save(gold_hourly_path)

row_count = spark.read.format("delta").load(gold_hourly_path).count()
print(f"✅ Gold Hourly Demand & Weather: {row_count:,} rows → {gold_hourly_path}")
display(df_hourly_demand)

# COMMAND ----------

# ── Advance the ADF watermark — reached only if every cell above succeeded ──
# Guard: advance ONLY when this run processed exactly the month the watermark asked for.
# Interactive runs (default 2024-01) and ad-hoc re-runs of old months never move it.
import json
from datetime import datetime, timezone

watermark_path      = f"{storage_root}/config/watermark.json"
BACKFILL_END_MONTH  = "2024-12"   # last month to process; afterwards status = BACKFILL_COMPLETE

def decide_watermark(current, processed_month, next_month, end_month):
    """Return the new watermark dict, or None if it must not be advanced."""
    if not current or current.get("next_month") != processed_month:
        return None
    return {
        "last_processed_month": processed_month,
        "next_month": next_month,
        "status": "BACKFILL_COMPLETE" if next_month > end_month else "SUCCESS",
        "last_run_timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

# Read the current watermark (None if the file does not exist yet)
try:
    current_wm = json.loads(dbutils.fs.head(watermark_path, 65536))
except Exception:
    current_wm = None

next_year_month = period_end[:7]          # period_end = 1st of next month, from Parameters cell
new_wm = decide_watermark(current_wm, year_month, next_year_month, BACKFILL_END_MONTH)

if new_wm:
    dbutils.fs.put(watermark_path, json.dumps(new_wm, indent=2), overwrite=True)
    watermark_action = f"advanced {year_month} -> {next_year_month} ({new_wm['status']})"
else:
    expected = current_wm.get("next_month") if current_wm else "<no watermark file>"
    watermark_action = f"not advanced (processed {year_month}, watermark expects {expected})"

print(f"Watermark: {watermark_action}")

# COMMAND ----------

# ── Run summary — returned to ADF as the notebook exit value (must stay the LAST cell) ──
import json

def _count(path):
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return 0   # e.g. quarantine table not written because there were no rejects

run_summary = {
    "status": "SUCCEEDED",
    "year_month": year_month,
    "bronze_trips": df_raw.count(),
    "silver_trips": _count(silver_path),
    "quarantine_trips": _count(quarantine_path),
    "silver_weather_hours": _count(weather_silver_path),
    "silver_zones": _count(zone_silver_path),
    "gold_enriched_trips": _count(gold_trips_path),
    "gold_daily_borough_rows": _count(gold_daily_path),
    "gold_hourly_rows": _count(gold_hourly_path),
    "watermark": watermark_action,
}
print(json.dumps(run_summary, indent=2))

dbutils.notebook.exit(json.dumps(run_summary))

# COMMAND ----------

