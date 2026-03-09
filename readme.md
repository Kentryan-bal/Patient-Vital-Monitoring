# Real-Time Patient Vitals Monitoring Pipeline

An end-to-end streaming data engineering pipeline that monitors simulated patient vitals (heart rate, blood pressure, oxygen levels, and temperature) in real-time. Built entirely on **Google Cloud Platform (GCP)** using the **Medallion Architecture**, with fully automated data ingestion, multi-stage transformation, and live visualization.

![Pipeline Architecture](<Patient Vital Monitoring Pipeline.png>)

---

## Architecture

The pipeline implements a **Medallion Architecture** to enforce strict data lineage and quality at every stage. By persisting data at each layer of maturity, the system enables auditing, reprocessing, and fault isolation without impacting the final analytics layer.

| Layer | Description | Storage |
|---|---|---|
| **Bronze** | Raw JSON ingested from the Pub/Sub stream, stored without manipulation. | Google Cloud Storage |
| **Silver** | Parsed, validated, and enriched records (invalid data filtered, risk scores calculated). | Google Cloud Storage |
| **Gold** | Aggregated per-patient averages and risk levels, ready for business consumption. | BigQuery |

### System Components

| Service | Role |
|---|---|
| **Google Cloud Pub/Sub** | Real-time messaging bus; decouples data ingestion from processing via a publisher/subscriber model. |
| **Google Cloud Dataflow** | Serverless execution engine for the Apache Beam pipeline; handles auto-scaling and windowing. |
| **Google Cloud Storage (GCS)** | Warm/cold storage for Bronze and Silver layers, plus Dataflow staging and temp files. |
| **Google BigQuery** | Data warehouse for the Gold layer; stores risk analytics for real-time querying. |
| **Power BI** | Dashboard connected via DirectQuery for live monitoring and visualization. |

---

## Tech Stack

- **Language:** Python
- **Stream Processing:** Apache Beam (executed on Cloud Dataflow)
- **Messaging:** Google Cloud Pub/Sub
- **Storage:** Google Cloud Storage, BigQuery
- **Visualization:** Power BI (DirectQuery)
- **Version Control:** GitHub

---

## Project Structure

```
patient-vital-monitoring/
├── simulator/
│   └── patient_vitals_simulator.py   # Data generator with error injection
├── dataflow/
│   └── streaming_medallion_pipeline.py  # Apache Beam streaming pipeline
├── .env                                 # Environment variables (not committed)
└── readme.md
```

---

## Data Simulator

The simulator (`simulator/patient_vitals_simulator.py`) generates one patient vital record every **2 seconds** and publishes it to Pub/Sub. It supports **20 unique patient IDs** (P001–P020) by default.

### Physiological Ranges

| Vital | Range | Notes |
|---|---|---|
| Heart Rate | 60–120 bpm | Extended above normal to trigger high-risk alerts |
| SPO2 | 90–100% | Blood oxygen saturation |
| Temperature | 36.0–39.0 °C | Values above 37.5 simulate fever |
| BP Systolic | 100–140 mmHg | |
| BP Diastolic | 60–90 mmHg | |

### Error Injection

To validate the pipeline's cleaning logic, the simulator injects corrupted records at a configurable **10% error rate**:

| Error Type | Behavior |
|---|---|
| **Missing Field** | A random field in the record is set to `None` |
| **Negative Value** | Heart rate is set to `-1` |
| **Out of Range** | SPO2 is set to `150` (physically impossible) |

---

## Pipeline — Medallion Transformation Logic

The Apache Beam pipeline (`dataflow/streaming_medallion_pipeline.py`) processes the stream using **60-second fixed windows** across three transformation stages.

### Bronze Layer (Raw)

- Reads byte messages from the Pub/Sub subscription.
- Decodes each message to a UTF-8 string.
- Writes raw, uncleaned JSON to `gs://[BUCKET]/bronze/`.

### Silver Layer (Cleaned & Enriched)

**Validation** — Records are filtered through strict physiological bounds:
- All required fields must be present (no `None` values).
- $0 < \text{SPO2} \leq 100$
- $30 \leq \text{Temperature} \leq 45$
- $\text{Heart Rate} > 0$

**Enrichment** — A weighted risk score is calculated for each valid record:

$$\text{Risk Score} = \left(\frac{\text{HR}}{200}\right) \times 0.4 + \left(\frac{\text{Temp}}{40}\right) \times 0.3 + \left(1 - \frac{\text{SPO2}}{100}\right) \times 0.3$$

| Risk Level | Score Range |
|---|---|
| Low | $< 0.3$ |
| Moderate | $0.3$ – $0.6$ |
| High | $\geq 0.6$ |

Cleaned, enriched records are written to `gs://[BUCKET]/silver/`.

### Gold Layer (Aggregated)

- Groups records by **Patient ID** within each window.
- Computes average heart rate, SPO2, and temperature.
- Determines the **maximum risk level** in the group (High > Moderate > Low).
- Writes aggregates to BigQuery using `WRITE_APPEND` and `CREATE_IF_NEEDED`.

### BigQuery Schema (`patient-risk-analytics`)

| Column | Type |
|---|---|
| `patient_id` | STRING |
| `average_heart_rate` | FLOAT |
| `average_spo2` | FLOAT |
| `average_temperature` | FLOAT |
| `max_risk_level` | STRING |

---

## Setup & Deployment

### 1. Prerequisites

- A GCP project with billing enabled.
- Google Cloud Shell (or a local environment with `gcloud` CLI configured).

### 2. Enable Required APIs

```bash
gcloud services enable \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  dataflow.googleapis.com \
  storage.googleapis.com
```

### 3. Provision GCP Resources

- **Pub/Sub:** Create a topic (e.g., `patient-vitals-topic`) and a dedicated pull subscription.
- **Cloud Storage:** Create a globally unique bucket. The pipeline will manage `bronze/`, `silver/`, `temp/`, and `staging/` folders automatically.

### 4. IAM Roles

Assign the following roles to the Dataflow service account:

- Pub/Sub Admin
- BigQuery Admin
- Storage Admin
- Editor (required for cross-service resource orchestration)

### 5. Environment Variables

Create a `.env` file in the project root:

```env
GCP_PROJECT=your-project-id
PUBSUB_TOPIC=patient-vitals-topic
PUBSUB_SUBSCRIPTION=projects/your-project-id/subscriptions/your-subscription
PATIENT_COUNT=20
STREAM_INTERVAL=2
ERROR_RATE=0.1
BRONZE_PATH=gs://your-bucket/bronze/
SILVER_PATH=gs://your-bucket/silver/
BIGQUERY_TABLE=your-project-id:healthcare.patient-risk-analytics
TEMP_LOCATION=gs://your-bucket/temp
STAGING_LOCATION=gs://your-bucket/staging
REGION=us-central1
```

### 6. Install Dependencies

```bash
pip install apache-beam[gcp] python-dotenv
```

### 7. Run the Simulator

```bash
python simulator/patient_vitals_simulator.py
```

### 8. Launch the Dataflow Pipeline

```bash
python dataflow/streaming_medallion_pipeline.py \
  --runner DataflowRunner \
  --project $GCP_PROJECT \
  --temp_location gs://$BUCKET_NAME/temp \
  --staging_location gs://$BUCKET_NAME/staging \
  --region us-central1
```

### 9. Verify

1. **Dataflow UI:** Inspect the job graph to confirm data is flowing through all transform nodes.
2. **GCS:** Verify that `bronze/` and `silver/` folders are populating with partitioned JSON files.
3. **BigQuery:** Run the following query to confirm risk scores are aggregating correctly:
   ```sql
   SELECT * FROM `healthcare.patient-risk-analytics` LIMIT 10;
   ```

---

## Dashboard

The **Power BI** dashboard (connected via DirectQuery to BigQuery) provides real-time monitoring:

- Average Heart Rate, SPO2, and Temperature per patient.
- A **Slicer** to filter by specific Patient IDs.
- Dynamic risk indicators that change color based on the latest risk level (e.g., red for High Risk).

