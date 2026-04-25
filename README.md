# PFSD Batch Analytics (Scala + Spark)

Standalone batch repository for the PFSD university project.

This project is intentionally separate from online/event-driven services and focuses only on reading historical events from S3 and generating batch analytics outputs.

## Scope

- Reads historical `orders` and `alerts` events from S3.
- Runs batch analytics with Spark SQL/DataFrame API.
- Writes simple report outputs (JSON + CSV) and prints a console summary.

Out of scope:

- Spring Boot
- Streaming
- ML
- Databases
- Full data lake platform features

## Event Models and Historical JSON Contract

This repository uses the real backend event model and historical payload shapes.

### OrderEvent (source model)

```java
public record OrderEvent(
        Long orderId,
        Long userId,
        BigDecimal total,
        int itemCount,
        LocalDateTime createdAt
) {}
```

### AlertEvent (source model)

```java
public record AlertEvent(
        Long orderId,
        Long userId,
        BigDecimal total,
        List<String> razones,
        LocalDateTime detectedAt
) {}
```

### Historical Orders JSON fields

- `eventType`
- `storedAt`
- `orderId`
- `userId`
- `total`
- `itemCount`
- `createdAt`

### Historical Alerts JSON fields

- `eventType`
- `storedAt`
- `orderId`
- `userId`
- `total`
- `razones`
- `detectedAt`

Important: this project preserves `razones` exactly and does not rename it.

## S3 Layout

The job reads directly from these prefixes:

- `s3://pfsd-order-history/orders/`
- `s3://pfsd-order-history/alerts/`

Historical keys are expected in partitioned style paths such as:

- `s3://pfsd-order-history/orders/year=YYYY/month=MM/day=DD/order-<timestamp>-<orderId>.json`
- `s3://pfsd-order-history/alerts/year=YYYY/month=MM/day=DD/alert-<timestamp>-<orderId>.json`

## Analytics Implemented

- Total number of orders
- Total number of alerts
- Percentage of orders that generated alerts
- Average order total
- Average item count per order
- Top users by number of alerts
- Highest order totals
- Alert reason frequency from `razones`

## Project Structure

```text
build.sbt
project/build.properties
src/main/resources/batch-defaults.properties
src/main/scala/com/pfsd/batch/BatchAnalyticsJob.scala
README.md
```

## Prerequisites

- Java 11+ (Java 17 recommended)
- sbt 1.12.x
- AWS credentials with read access to S3 input paths

## AWS Credentials (Local)

Use one of the standard approaches:

1. AWS CLI profile (`~/.aws/credentials`)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`)
3. IAM role (if running inside AWS environment)

Spark/Hadoop will use the default AWS provider chain.

## Configuration

Defaults are in `src/main/resources/batch-defaults.properties`:

- `orders.path=s3://pfsd-order-history/orders/`
- `alerts.path=s3://pfsd-order-history/alerts/`
- `output.path=output`
- `top.n=10`

Overrides are supported through command args or env vars.

### Command Arg Format

- `--ordersPath=...`
- `--alertsPath=...`
- `--outputPath=...`
- `--topN=...`

### Environment Variable Overrides

- `PFSD_ORDERS_PATH`
- `PFSD_ALERTS_PATH`
- `PFSD_OUTPUT_PATH`
- `PFSD_TOP_N`

## Run Locally

### 1) Compile

```bash
sbt compile
```

### 2) Run with defaults

```bash
sbt run
```

### 3) Run with explicit paths

```bash
sbt "run --ordersPath=s3://pfsd-order-history/orders/ --alertsPath=s3://pfsd-order-history/alerts/ --outputPath=output"
```

## Expected Outputs

Spark writes outputs as folders with part files:

- `output/batch-report-json/`
- `output/batch-report-csv/`
- `output/top-users-by-alerts/`
- `output/highest-order-totals/`
- `output/alert-reason-frequency/`

The job also prints a readable summary to console.

## Why this satisfies PFSD batch scope

- Uses Scala + Spark only.
- Reads historical events from S3 prefixes for `orders` and `alerts`.
- Uses exact payload fields from backend history layer.
- Produces defendable analytics outputs for an MVP batch component.

## Later migration to AWS Glue (notes)

- Keep the same Spark DataFrame logic; move entrypoint into Glue job script/class.
- Externalize paths and `topN` into Glue job parameters.
- Replace local output path with S3 output prefix.
- Keep dependencies aligned with Glue Spark runtime version.
- Optionally package as fat JAR for Glue job submission.