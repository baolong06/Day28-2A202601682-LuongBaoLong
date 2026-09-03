# ANSWERS.md — Lab 28 Track 2

**Repo:** Day28-2A202601682-LuongBaoLong · **Mã sinh viên:** 2A202601682 · **Tác giả:** Lương Bảo Long
**Ngày hoàn thành:** (điền ngày demo) · **Phiên bản code:** v3.0.0 (tag `v3.0.0`)

Tài liệu này trả lời mục 8 của `SUBMISSION.md`: trade-offs thiết kế, production gaps
còn thiếu khi lên môi trường thật, và phân công đóng góp từng thành viên. Toàn bộ
khẳng định đều neo vào mã nguồn trong repo — tên file được trích dẫn inline.
Cách vận hành thực tế trên máy host (PowerShell + Docker Desktop) xem
[`HOST_PLAYBOOK.md`](HOST_PLAYBOOK.md).

---

## DATA & ML PLANE (IP01–IP06)

### IP01 — HTTP ingestion vào Kafka

**Trade-offs**

- `EventPublisher` dùng `acks="all"` + `enable.idempotence=True` và flush-kiểm-tra ngay trong `publish()`, nên lời hứa 202 của API đồng nghĩa với việc broker đã ghi bền — đổi lại mỗi request trả giá bằng độ trễ đồng bộ (`event_bus.py`).
- Trace context W3C (`traceparent`) và `idempotency-key` được ghi vào Kafka headers qua `event_headers()` (`integration_tasks.py`), giữ nguyên dấu vết end-to-end qua bước nhảy bất đồng bộ; nhưng header chỉ là metadata — broker không xác thực chúng.
- Key partition chính là `idempotency_key`, đảm bảo mọi bản sao của cùng một sự kiện nằm trên một partition, giữ thứ tự cho dedupe về sau.

**Production gaps**

- `replication_factor` trong `TOPICS` dừng ở mức cấu hình lab; chưa có bằng chứng multi-broker ISR thực tế.
- Chưa có schema registry / versioning chặt chẽ ngoài header `schema_version` — schema drift do producer tự khai báo.

### IP02 — Kafka vào Airflow 3

**Trade-offs**

- `BatchConsumer` đặt `enable.auto.commit=False` và `process_batch_then_commit()` chỉ gọi `commit()` sau khi handler thành công: crash giữa xử lý và commit sẽ phát lại batch — chấp nhận at-least-once, dựa vào idempotency hạ nguồn (`event_bus.py`).
- Lỗi `DEPENDENCY_UNAVAILABLE` được raise lại mà KHÔNG commit, để lần chạy sau nhận lại sự kiện; lỗi vĩnh viễn (poison) sau `max_attempts=3` được đưa vào DLQ để không chặn partition (`event_bus.py`).
- DAG đặt `schedule=None` + `max_active_runs=1` (`lab28_ingestion_pipeline.py`): loại bỏ race giữa các run nhưng đổi throughput lấy tính đơn giản.

**Production gaps**

- Consumer đơn, không autoscale theo lag; `auto.offset.reset=earliest` chỉ cứu lần khởi động đầu của group.
- DAG được trigger thủ công/API trong lab, chưa có scheduling liên tục và backpressure.

### IP03 — Airflow/Spark vào Delta MERGE + time travel

**Trade-offs**

- `merge_sql()` dùng `MERGE INTO ... ON target.idempotency_key = source.idempotency_key WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` (`delta_store.py`) — một câu lệnh duy nhất biến replay thành cập nhật thay vì nhân đôi; writer chỉ-INSERT sẽ vượt mọi unit test mà vẫn gấp đôi bảng.
- `dedupe_latest()` gộp batch về một sự kiện/key, thắng theo `(occurred_at, event_id)` để kết quả không phụ thuộc thứ tự giao của Kafka (`integration_tasks.py`).
- Bảng định danh bằng đường dẫn (`delta.`path``) + `CREATE TABLE IF NOT EXISTS` (`delta_store.py`): job chạy lại được, transaction log giữ nguyên qua `compose down`, nhưng không có metastore trung tâm.
- Spark chạy qua Spark Connect (`spark/delta_merge.py`) nên image Airflow chỉ cần Python — đánh đổi: thêm một network hop và một điểm phụ thuộc.

**Production gaps**

- Chưa có `OPTIMIZE`/compaction hay Z-ordering; nhiều MERGE nhỏ sẽ tạo hàng nghìn file parquet.
- Time travel chứng minh bằng `time_travel_evidence()` so sánh số dòng giữa version sớm nhất/mới nhất (`delta_store.py`) và `VERSION AS OF` trong `feature_export.py`, nhưng chưa có chính sách retention/log compaction.

### IP04 — Delta sang Feast online features

**Trade-offs**

- `feature_export.py` export snapshot bằng `VERSION AS OF {delta_version}` + ghi `overwrite`: tính nhất quán điểm-ảnh, nhưng online store luôn trễ hơn hồ dữ liệu một nhịp.
- `definitions.py` đặt `ttl=timedelta(days=7)` trên `asker_activity_v1`: entity im lặng quá 7 ngày bị coi là stale (`OUTSIDE_MAX_AGE`) thay vì phục vụ dữ liệu cũ một cách im lặng.
- `materialize_incremental` gọi `POST /materialize` với `disable_event_timestamp=True` (`feature_store.py`): đơn giản hóa ingest nhưng mất khả năng point-in-time correctness.
- `get_asker_features()` không bao giờ raise cho entity lạnh — trả default suy giảm với `degraded=True`, biến missing feature thành quyết định sản phẩm thay vì lỗi.

**Production gaps**

- FileSource parquet trên disk cục bộ; không có online store phân tán/HA.
- Chưa có alert khi `materialize` thất bại liên tục — chỉ phát hiện qua status `NOT_FOUND`/`OUTSIDE_MAX_AGE` trên từng lookup.

### IP05 — Tài liệu Delta vào Qdrant

**Trade-offs**

- `stable_point_id(doc_id)` tạo UUIDv5 tất định từ `doc_id` (`contracts`), nên upsert trùng lặp ghi đè đúng point đó — re-index không bao giờ nhân đôi (`vector_store.py`).
- Hybrid retrieval: dense `COSINE` + sparse `Qdrant/bm25` với `Modifier.IDF` bắt buộc, hợp nhất bằng `FusionQuery(fusion=Fusion.RRF)` (`vector_store.py`); trả giá là chi phí embed gấp đôi mỗi lần index/query.
- Task `index_new_documents` đọc lại tài liệu từ Delta theo `idempotency_keys` thay vì XCom (`lab28_ingestion_pipeline.py`): Delta là nguồn sự thật duy nhất, nhưng mỗi run phải trả chi phí đọc lại.
- Upsert thay toàn bộ point nên cả hai named vector luôn được ghi cùng nhau — ghi thiếu một sẽ làm null vector kia (`vector_store.py`).

**Production gaps**

- Không có đường xóa tài liệu bị thu hồi khỏi collection.
- Embedding drift chỉ bị chặn tại thời điểm build image qua `verify_model_pins()`; mô hình trôi giữa các replica đang chạy sẽ không bị phát hiện.

### IP06 — Evaluation vào MLflow registry với alias champion

**Trade-offs**

- Dùng alias thay vì stages: `promote()` chỉ là một lệnh `set_registered_model_alias` di chuyển `champion` (`model_registry.py`) — promote/rollback nguyên tử, nhưng không có khái niệm staged rollout.
- Artifact đăng ký là pyfunc chỉ render prompt (`_PromptRelease`), không phải weights — vì release thực sự là `ReleaseSpec` (prompt_version, vllm_model_id, embedding_model_id, qdrant_collection, feature_service, top_k, delta_version); rollback vì vậy có ý nghĩa trọn vẹn.
- `resolve()` gọi `get_model_version_by_alias` trên mỗi request serving: luôn đúng version champion mới nhất, đổi lại thêm một round-trip tới MLflow mỗi request.

**Production gaps**

- Không có canary/percentage rollout — champion đổi là đổi toàn bộ lưu lượng.
- Chưa có cache phía serving cho `resolve()`; MLflow down đồng nghĩa serving không resolve được release (`RegistryUnavailable`).

### Idempotency & no-data-loss

Chuỗi chứng minh gồm bốn mắt xích: (1) `dedupe_latest()` gộp các bản sao trong cùng một batch, chọn bản mới nhất theo `(occurred_at, event_id)` (`integration_tasks.py`); (2) MERGE khớp trên `idempotency_key` nên bản sao đến sau batch chỉ UPDATE dòng đã có (`delta_store.py`); (3) offset chỉ commit sau khi xử lý bền, nên mọi crash trước commit đều được Kafka phát lại (`event_bus.py`); (4) time travel cho thấy mỗi lần replay chỉ tạo commit MERGE với `numTargetRowsUpdated` tăng, không tăng số dòng. IT-J2 chứng minh trực tiếp: gửi cùng một fact ba lần, sau hai run Delta vẫn đúng một dòng, Qdrant vẫn đúng một point vì UUIDv5 tất định (`test_j2_idempotent_replay.py`).

### Recovery

- **Kafka down**: API trả 503 vì `probe_kafka` là mandatory (`readiness.py`); phía consumer, lỗi `DEPENDENCY_UNAVAILABLE` không commit nên batch được phát lại khi broker hồi (`event_bus.py`). Sau phục hồi chỉ cần consume một lần — runbook cấm `docker compose down -v` vì sẽ xóa state (`failure-injection.md`).
- **Airflow down**: sự kiện nằm an toàn trong Kafka nhờ retention; offset chưa commit nên run kế tiếp nhặt đủ — không mất dữ liệu, chỉ trễ.
- **Feast down**: probe đặt `mandatory=False` (`readiness.py`), `/ready` chuyển `degraded` thay vì 503 — phục vụ câu trả lời suy giảm kèm lý do và tăng counter `lab28_degraded_responses_total`; khi Feast trở lại, lookup trả `PRESENT` và trạng thái phục hồi (`test_j4_degraded_recovery.py`).

---

## SERVING & OPERATIONS (IP07–IP10)

### IP07 — RAG prompt → vLLM (OpenAI-compatible)

Prompt render từ `Release.prompt_template` + sources (`pipeline.render_prompt()`), gọi `VLLMClient.complete()` tới `{LAB28_VLLM_BASE_URL}/chat/completions` (`configmap.yaml: http://vllm.lab28.svc:8000/v1`). `probe_identity()` (`llm_client.py`) đối chiếu `GET /version` (chỉ vLLM có) + `/metrics` chứa họ `vllm:`; cả hai mới `is_real_vllm=True`. Span `lab28.vllm.chat_completion`, metric `lab28_llm_seconds{model_id,outcome}` và `lab28_llm_tokens_total{model_id,direction}`. Mặc định `Qwen/Qwen3-1.7B`, `timeout 30s`, `max_tokens 320`, `temperature 0.2`.

- **Trade-offs:**
  - Xác thực kép `/version` + `vllm:` metrics thay vì chỉ `/health 200`: OpenAI-proxy bất kỳ cũng trả 200, nên `gate: gpu` (`integration-matrix.yaml`) cố ý fail mock/CPU classifier.
  - `LAB28_VLLM_REQUIRE_REAL` bật/tắt được: laptop dev vẫn chạy, K8s prod fail-closed (`configmap.yaml: "true"`) — GPU thiếu phải là gate đỏ chứ không xanh giả.
  - `httpx.Client` tái sử dụng + `identity()` cache để giữ p95 < 500ms (`ServingSettings.llm_budget_ms`), refresh khi readiness.
- **Production gaps:**
  - Không retry/backoff/circuit-breaker: `InferenceUnavailable` → 503 ngay, burst khuếch đại lỗi.
  - Không streaming: không đo TTFB cho SLO interactive.
  - Không mTLS/key rotation; `NetworkPolicy` cho phép cả namespace `lab28` tới vLLM.

### IP08 — Client → Envoy gateway

Envoy `v1.39.1` listen `8080` (`gateway/envoy.yaml`), `generate_request_id: true`. Route `/healthz` → `direct_response 200` (không chạm app), `/` → cluster `api` (`timeout 120s`, decorator `lab28.gateway.request`). `local_ratelimit`: `max_tokens 10`, `tokens_per_fill 10`, `fill_interval 1s` (10 req/s) → 429 + `x-lab28-rate-limited`. Health-check cluster dùng `path: /health` (liveness), không phải `/ready`.

- **Trade-offs:**
  - `local_ratelimit` thay vì global (Redis): không cần infra ngoài; đánh đổi là limit per-instance, không chính xác khi ≥2 replicas.
  - `/healthz` tự trả tại Envoy: chứng minh gateway khỏe mà không đánh thức upstream (`test_gateway_rate_limit.py` đếm `lab28_requests_total{route="/health"}` không tăng).
  - Burst test bắn `/health` (route rẻ nhất) để đo limiter thuần, lẫn `x-request-id` cả trên 429.
- **Production gaps:**
  - Token bucket 10/s tĩnh, không phân tenant/API-key — một client chiếm hết quota.
  - Chưa có `ext_authz`/JWT dù matrix ghi "auth policy"; hiện chỉ rate-limit.
  - `healthy_panic_threshold: 0` → không panic mode; cả 2 pod fail health thì trả 503, thiếu outlier detection/retry per-route.

### IP09 — Components → Prometheus/Grafana

`prometheus.yml` scrape 10 jobs @5s (`api:8000`, `gateway:9901/stats/prometheus`, `kafka-exporter:9308`, `pushgateway:9091`, `qdrant:6333`, `mlflow`/`feast` qua `/metrics`, `otel-collector:8888`, `vllm-optional`). `alerts.yml`: `Lab28ApiUnavailable` (`up{job="lab28-api"}==0 for 30s`, critical), `Lab28HighErrorRatio` (5xx > 5% for 2m). `platform-overview.json` (uid `lab28-platform`) 4 panels: req rate, error ratio, `histogram_quantile(0.95, lab28_request_seconds_bucket)`, `lab28_consumer_lag`.

- **Trade-offs:**
  - Pushgateway cho batch (IP02/IP03) qua `metrics.push_batch_metrics()`: process Airflow sống vài giây không scrape được; long-lived service vẫn scrape thường để liveness không bị che. Push fail trả `False` chứ không làm hỏng pipeline.
  - Bucket `_REQUEST/_FAST/_LLM_BUCKETS` khớp latency budget slide → alert đọc đúng boundary thay vì nội suy.
  - `lab28_component_ready{component,owner}` publish từ `/ready` để alert và endpoint không mâu thuẫn (`test_j5` assert).
- **Production gaps:**
  - `vllm-optional` dùng `host.docker.internal` → không chạy trên K8s; target down by design được `test_prometheus_targets.py` filter theo keyword `vllm`.
  - Chỉ 2 alert, chưa burn-rate cho `lab28_llm_seconds`/`consumer_lag`; `lab28_latency_budget_exceeded_total` tăng mà không alert.
  - Dashboard thiếu panel `lab28_release_version_info`/`lab28_degraded_responses_total`.

### IP10 — Components → OTLP: một trace HTTP→Kafka→Airflow→data/ML→response

11 span bắt buộc (`required_spans`) từ `lab28.gateway.request` tới `lab28.vllm.chat_completion` (`telemetry.py`). W3C `traceparent` propagate qua HTTP (`current_traceparent`) và Kafka headers (`inject_kafka_headers`). `otel-collector.yaml`: otlp grpc 4317/http 4318 → memory_limiter+batch → `otlp/jaeger`+debug. `test_j5` chia SYNCHRONOUS/ASYNCHRONOUS/SERVING legs trên cùng trace id caller sinh; `test_trace_span_coverage` assert đủ 11 span + `otelcol_exporter_sent_spans>0`, `send_failed==0`.

- **Trade-offs:**
  - Trace context trong Kafka header thay vì payload: giữ `IngestionEvent` sạch; không có nó consumer mở trace mới, gate IP10 fail.
  - Jaeger local + LangSmith: CI/offline luôn có evidence; LangSmith báo `UNVERIFIED` khi thiếu key (`gate: langsmith`).
  - `ParentBased(TraceIdRatioBased 1.0)` mặc định: mọi request lab đều trace, đổi ratio không sửa code.
- **Production gaps:**
  - Collector chưa có exporter LangSmith thật (chỉ `otlp/jaeger`), test chỉ suy qua ≥2 exporter.
  - Batch processor không bền khi collector/Jaeger down; thiếu file/DLQ exporter.
  - Không tail-based sampling theo error → 100% head sampling quá tải ở traffic thật.

### Readiness semantics

`integration_tasks.readiness_status()`: mọi probe pass → `ready`; probe fail nhưng `mandatory=False` → `degraded`; bất kỳ probe `mandatory=True` fail → `not_ready`. `serving_probes()`: kafka/mlflow/qdrant (mandatory), vllm (mandatory ↔ `require_real`), feast (`mandatory=False`), delta tách riêng cho `integration_report()`. `GET /health` = liveness không chạm dependency; `GET /startup` = config+clients đã dựng; `GET /ready` = "pod có serving được ngay không", publish gauge + trả 503 **chỉ khi** `not_ready` — `degraded` vẫn 200 vì Feast lạnh làm answer kém chứ không sai, drain pod sẽ biến lỗi một phần thành mất dịch vụ. IP08/09/10 không probe được từ process serving báo `unverified`, không xanh giả.

### Degraded mode

Cờ `LAB28_ALLOW_DEGRADED=true`. Feast cold/unreachable → `AskerFeatures` defaults (`feedback_count 0`, `avg_rating 0.0`) + `degraded=True`, `stage.degrade(reason="feast")` → `lab28_degraded_responses_total`. Qdrant empty/unavailable → sources `[]`, degrade `"qdrant"/"qdrant_empty"`, prompt ghi "(không có tài liệu nào được truy hồi)" nhưng vẫn gọi LLM. vLLM không có đường degrade — `_infer()` ném `ServingError(DEPENDENCY_UNAVAILABLE)` → 503. Khi `allow_degraded=false`, `_require_degraded_allowed()` biến feast/qdrant failure thành hard 503 (demo fail-closed). `finish_reason=="length"` → degrade `llm_truncated`.

### Kubernetes & GitOps

`api.yaml`: readiness `/ready` (10s, failureThreshold 3), liveness `/health` (20s, 3), startup `/health` (5s, 24), `runAsNonRoot`, `readOnlyRootFilesystem`. `resilience.yaml`: HPA 2→8 @ CPU 70% (scaleDown stabilize 300s), PDB `minAvailable 1`. `network-policy.yaml`: ingress chỉ từ namespace gateway (`...gateway-controller: envoy-gateway`) port 8000; egress chỉ trong `lab28` + DNS. `gateway.yaml`: Gateway API + HTTPRoute `/` → `lab28-api:8000`.

`gitops/application.yaml`: ArgoCD Application, `targetRevision: refs/tags/v3.0.0`, `path: deploy/kubernetes/base`, `syncPolicy.automated {prune: true, selfHeal: true}` — promotion/revert là đổi tag + immutable image trong Git, ArgoCD tự heal drift.

**Rollback bad model/alias** (hai tầng). (1) Hạ tầng: `runbooks/gitops-rollback.md` — validate manifests, đổi image tag trong Git, revert revision, ArgoCD re-sync, kiểm replicas/gateway/trace. (2) Model: `ReleaseRegistry.rollback()` dịch chuyển alias `champion` về version thấp hơn kế cận (`model_registry.py`) — không cần redeploy vì serving đọc alias mỗi request (`test_serving_switches_release_without_a_restart`); `lab28_release_transitions_total{action="rolled_back"}` tăng và `set_release()` clear series cũ để dashboard chỉ một release. **Ownership**: `team-data` sở hữu registry/alias promotion, `team-platform` sở hữu ArgoCD/K8s rollout, `team-serving` sở hữu pipeline đọc release (`contracts/integration-matrix.yaml:roles` + `SERVICE_OWNERS`).


---

## ĐÓNG GÓP TỪNG THÀNH VIÊN

> Nhóm tự điền tỷ lệ % theo thỏa thuận; bảng dưới là phân công mặc định lấy từ
> `contracts/integration-matrix.yaml:roles` và `docs/team-role-cards.md`.

| Thành viên | Vai trò (role) | Phạm vi IP | Đóng góp chính | Tỷ lệ % |
|---|---|---|---|---|
| Lương Bảo Long | Ingestion & Orchestration | IP01–IP02 | Triển khai `event_headers()`, publisher/consumer, Kafka topic, Airflow DAG | … |
| (tên) | Data & ML | IP03–IP04–IP06 | Triển khai `dedupe_latest()`, Spark MERGE, Feast snapshot, `feast_online_request()`, MLflow alias | … |
| (tên) | Serving & Retrieval | IP05–IP07 | Qdrant indexing, RAG pipeline, `readiness_status()`, vLLM client | … |
| (tên) | Platform & Observability | IP08–IP10 | Envoy gateway, Prometheus/Grafana/alert, OTLP trace, K8s manifests | … |
| (tên) | Presenter / Incident Commander | evidence, ANSWERS.md | Chạy journeys, ghi evidence, viết narrative incident, chuẩn bị Q&A | … |

**Nguyên tắc evidence:** mọi file trong `evidence/` đều có timestamp, run/trace/
version IDs lấy từ stack đang chạy — không có file nào được sinh offline hay làm
giả. Các gate `gpu`/`langsmith` không thỏa mãn trên máy không có GPU/credential
thì được báo `UNVERIFIED` kèm lý do, đúng yêu cầu của `SUBMISSION.md`.
