# Scalable Telemetry Pipeline for FSD Edge-Case Curation

**Author:** Siri Lahari Chava  
**Target Role:** Data Engineer, Fleet Data (Self-Driving) @ Tesla  

## 1. The Engineering Challenge: Signal vs. Noise
With millions of vehicles globally, the Fleet Data team manages petabytes of telemetry. 99% of this data is routine, uneventful highway driving (noise). The core data engineering bottleneck is not raw storage—it is **compute and query latency**. 

The pipeline must aggressively filter telemetry to isolate high-value edge cases (e.g., FSD disengagements, phantom braking, complex intersections) so the Self-Driving AI team can evaluate models without scanning petabytes of redundant logs.

## 2. Proposed Architecture Stack
Based on the need for extreme scale, reliability, and low-latency visualization, this architecture proposal leverages:
* **Ingestion Gateway:** Apache Kafka / Schema Registry (Enforcing strict Protobuf schemas).
* **Data Processing (ELT):** Apache Spark (PySpark) for distributed micro-batch processing.
* **Orchestration & Observability:** Dagster for data-aware orchestration and asset validation.
* **Analytical Serving Layer:** ClickHouse for lightning-fast, columnar aggregations.
* **Cold Storage:** Apache Iceberg / S3 for historical backfills without cloud compute bloat.

---

## 3. Pipeline Execution & Code Implementation

### Phase A: Schema Enforcement (Protobuf)
Processing raw JSON in Spark at petabyte scale causes severe string-deserialization overhead. All telemetry must be ingested as serialized binary (Protobuf) to save CPU cycles.

```protobuf
syntax = "proto3";
package tesla.fleet;

message VehicleTelemetry {
  string vehicle_id = 1;
  int64 timestamp = 2;
  float speed_mph = 3;
  string autopilot_state = 4;       // "ENGAGED", "IDLE", "WARNING"
  bool user_steering_override = 5;
  float acceleration_ms2 = 6;
}
