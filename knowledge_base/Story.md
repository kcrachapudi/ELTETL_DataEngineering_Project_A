# Scene 1
Three data sources, three different formats, all waiting
It's 6:00am. Nothing has run yet. Three sources have data sitting in them — a live weather API, a CSV file on disk, and a stack of EDI files dropped by a healthcare partner overnight.
Open Meteo API          → JSON (live, no key needed)
sample_data/stations.csv → CSV (flat file, on disk)
sample_data/healthcare/  → EDI 834, 837, 835, 270/271
None of it is in your database. None of it is clean. It's all raw, in its native format, in its native home. This is the starting state.

# Scene 2
run.sh kicks off — the extractors wake up
You run bash run.sh. The pipeline starts. The first thing it does is send the extractors out to fetch raw data from each source. Each extractor knows exactly one thing — where to go and how to get there.
OpenMeteoExtractor
→
fires GET to api.open-meteo.com with lat/lon + date range
FileExtractor
→
reads sample_data/stations.csv from local disk as raw bytes
FileExtractor
→
reads sample_834.edi, sample_837p.edi, sample_835.edi from disk
The extractors return raw bytes or strings — they have no idea what format the data is in. That is deliberately not their problem.

# Scene 3
Raw bytes hit the format plugin layer
The raw data from each extractor is handed to the matching parser plugin. This is the format normalisation boundary — the only place in the entire pipeline where format-specific logic lives. Everything downstream sees a clean DataFrame and nothing else.
JSON bytes
→
JSONParser
→
DataFrame
CSV bytes
→
CSVParser
→
DataFrame
EDI 834 bytes
→
EDI834Parser
→
DataFrame
EDI 837 bytes
→
EDI837Parser
→
DataFrame
EDI 835 bytes
→
EDI835Parser
→
DataFrame
Weather API response (raw JSON):
{"hourly": {"time": ["2024-01-15T00:00","2024-01-15T01:00"...],
            "temperature_2m": [38.2, 37.1, 36.8 ...]}}

After JSONParser + OpenMeteoExtractor flattening:
   time                 temperature_2m  precipitation  windspeed_10m
   2024-01-15T00:00     38.2            0.0            8.4
   2024-01-15T01:00     37.1            0.0            7.9
   ...
EDI 834 raw (benefit enrollment):
ISA*00*...*ZZ*EMPLOYER001*ZZ*BLUECROSS001~
INS*Y*18*021*28*A*FT*N~
NM1*IL*1*SMITH*JOHN*A~
DMG*D8*19850315*M~
HD*021**HLT*PLANHMO001*IND~

After EDI834Parser:
   last_name  first_name  dob         gender  plan_id     coverage_type  effective_date
   SMITH      JOHN        1985-03-15  male    PLANHMO001  health         2024-01-01
   SMITH      JANE        1988-06-22  female  PLANHMO001  health         2024-01-01
   ...

# Scene 4
A partner pushes an order in via the inbound API
While the batch pipeline is running, ACME Corporation's system fires a POST to your inbound API with a new order. The API is running in Docker, listening on port 8000.
POST /inbound/orders HTTP/1.1
X-Partner-ID: PARTNER-ACME
X-API-Key: key-acme-abc123
X-Idempotency-Key: uuid-ord-20240115-001

{"order_id": "PO-98765", "order_date": "2024-01-15",
 "lines": [{"product_id": "WIDGET-A", "quantity": 50, "unit_price": 9.99},
           {"product_id": "WIDGET-B", "quantity": 100, "unit_price": 4.50}]}
The auth middleware checks the API key, confirms the partner is active, checks the rate limit. The idempotency handler hashes the request — first time seen, so it proceeds. The order is validated, line totals computed, and the audit log records the event. Response fires back in milliseconds.
{"status": "accepted", "our_reference": "ORD-UUID001ABCD", "line_count": 2}
The HMAC and idempotency checks mean: even if ACME's system retries this exact request 5 times due to a network blip, the order is processed exactly once.

# Scene 5
A partner sends a healthcare EDI file directly
Blue Cross sends an 837P claim file — not over SFTP this time, but directly POSTed to the EDI endpoint. The inbound API detects the ISA envelope, reads the ST*837 segment, and routes it straight to EDI837Parser.
POST /inbound/edi HTTP/1.1
X-Partner-ID: PARTNER-BLUECROSS
X-API-Key: key-bcbs-def456
Content-Type: text/plain

ISA*00*...*ZZ*PROVIDER001*ZZ*BLUECROSS001~
GS*HC*...*005010X222A2~
ST*837*0001~
CLM*CLM-2024-001*350***11:B:1~
...
The parser runs, produces 5 service line rows across 2 claims, and the loader writes them to raw.edi_837 in PostgreSQL. Transaction type, row count, and idempotency key all go into the audit log.
{"status": "accepted", "transaction_type": "837",
 "row_count": 5, "message": "EDI 837 accepted — 5 rows parsed."}

# Scene 6
All DataFrames land in PostgreSQL
Every DataFrame — whether it came from the weather API, a CSV, an EDI file, or the inbound REST API — hits the same PostgresLoader. Upsert mode means running the pipeline twice produces identical results. No duplicates, no errors on re-run.
raw.weather_hourly   ← 240 rows  (10 days × 24 hours)
raw.stations         ← 12 rows   (CSV weather stations)
raw.edi_834          ← 4 rows    (member enrollments)
raw.edi_837          ← 5 rows    (claim service lines)
raw.edi_835          ← 5 rows    (remittance service lines)
raw.edi_270          ← 1 row     (eligibility inquiry)
raw.edi_271          ← 11 rows   (benefit responses)
raw.orders           ← 2 rows    (inbound REST order lines)
All tables live in the raw schema — untransformed, exactly as received. This is the ELT pattern: Load first, Transform later (Project 2 with dbt).

# Scene 7
Something needs to go back out
The claim CLM-2024-001 has been processed. The event router detects a new row in outbound_event_queue with event type claim.processed. It routes it to the webhook dispatcher.
POST https://provider001.example.com/webhooks/inbound
X-Webhook-Signature: sha256=a3f9b2c1...
X-Idempotency-Key: evt-uuid-abc123
X-Event-Type: claim.processed

{"event_type": "claim.processed", "claim_id": "CLM-2024-001",
 "status": "processed_primary", "payment": 282.50}
The provider's system receives it, verifies the HMAC signature, and returns 200. The event router marks it sent. If the POST had failed, the retry queue would have tried again at 30s, 60s, 120s, 300s, 600s before dead-lettering it for manual review.
End state
What Project 1 leaves behind
By end of day, your PostgreSQL database has raw data from multiple sources, multiple formats, multiple directions. Every event has an audit log entry. Every inbound request was deduplicated. Every outbound delivery was signed and retried on failure.
project1/
  Extracts from:  REST APIs · CSV files · EDI files · inbound webhooks
  Parses:         JSON · CSV · XML · EDI X12 (850/856/810/997/834/835/837/270/271)
  Loads into:     PostgreSQL (raw schema, upsert, idempotent)
  Sends out:      Webhooks · HTTP API calls · EDI over SFTP
  Tracks:         Audit log · idempotency · retry queue · dead letter
This is a fully working ETL pipeline. In Project 2, dbt reads from the raw schema and transforms it. In Project 3, Airflow runs all of this on a schedule. In Project 4, the whole thing moves to GCP.

That's the full Project 1 story — seven scenes from raw data sitting untouched all the way to a signed outbound webhook firing on the other side.
The thing worth noticing: the data never cares what format it started in. A weather API response, an EDI 834 enrollment file, a REST order JSON, and a CSV flat file all end up as identical-looking DataFrames before they hit the loader. That's the whole point of the parser plugin layer — kill the format complexity at the boundary, and everything downstream is just SQL tables.
Project 2 (dbt) picks up exactly where this story ends — it reads from those raw.* tables and starts transforming them into something an analyst can actually use.
