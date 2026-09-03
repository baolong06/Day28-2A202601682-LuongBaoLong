# HOST PLAYBOOK — Day 28 Track 2 (Windows PowerShell, máy host có Docker + uv)

Mọi lệnh chạy ở thư mục repo trên host. Nguồn tham số: `src/lab28_platform/cli.py`, `load-tests/run_profile.py`, `integration-tests/conftest.py`, `contracts/integration-matrix.yaml`.

## Bước 0 — Đồng bộ repo về host

```text
git clone https://github.com/VinUni-AI20k/Day28-Modern-Platform-Lab-Student.git
cd Day28-2A202601682-LuongBaoLong
git switch -c ca-nhan-long
git status
```

Kỳ vọng: đúng nhánh mới, không có file đã sửa bị lạ.

## Bước 1 — Cài môi trường (Python 3.11)

```text
uv sync --python 3.11 --extra dev --extra integration --no-editable
uv run lab28 --help
uv run lab28 preflight
```

Kỳ vọng: `--help` liệt kê `preflight, topics, index, release, rollback, seed, dlq, ask, serve, inspect, ready, integration, evidence, reset`; `preflight` in JSON có `profile`, `python`, `docker_daemon`, `memory_gib`, `next`. `profile=local-standard` mới đi tiếp Docker; `browser-fallback` thì làm Bước 2 rồi mượn máy chung. Live suite đọc các biến `LAB28_*` với mặc định localhost: `LAB28_API_URL=:8000`, `LAB28_GATEWAY_URL=:8080`, `LAB28_KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, `LAB28_AIRFLOW_URL=:8082` (mật khẩu tự đọc từ `.lab28/airflow/simple-auth-passwords.json`), `LAB28_PROMETHEUS_URL=:9090`, `LAB28_TRACE_BACKEND_URL=:16686` (Jaeger), `LAB28_GRAFANA_URL=:3000` (admin/admin), `LAB28_GATEWAY_ADMIN_URL=http://localhost:9901`, `LAB28_VLLM_BASE_URL`, `LANGSMITH_API_KEY`. Không cần set gì thêm khi cổng mặc định.

## Bước 2 — Offline tests

```text
uv run pytest starter-tests -q
uv run pytest tests -q
uv run ruff check .
uv run python scripts/verify_matrix.py
uv run python scripts/check_portability.py
uv run python scripts/validate_manifests.py
```

Kỳ vọng (4 hàm `integration_tasks.py` đã làm sẵn): `starter-tests` → `4 passed` (lưu ý: `scripts/verify_starter_state.py` CHỈ đạt khi scaffold còn đúng 4 lỗi `NotImplementedError` — sau khi code xong thì KHÔNG chạy script này, nó sẽ báo lỗi_by_design). `tests` → all passed; `ruff` → `All checks passed!`; 3 script còn lại exit 0. Chưa xanh tuyệt đối chưa sang Docker.

## Bước 3 — Dựng stack

```text
docker compose --env-file ports.template config --quiet
docker compose --env-file ports.template up -d --build --wait
docker compose --env-file ports.template ps
uv run lab28 topics
```

Kỳ vọng: `config --quiet` im lặng exit 0 (thêm `--profile full` cũng vậy); `ps` toàn bộ `running/healthy`; `topics` in JSON, mỗi topic `created` hoặc `exists`. Trùng cổng → copy `ports.template`, chỉ đổi cổng host, dùng file mới sau `--env-file` trong mọi lệnh.

## Bước 4 — Core checkpoint

```text
uv run lab28 index --source file
uv run lab28 release
uv run lab28 seed --via-gateway
uv run lab28 inspect
uv run lab28 ready
```

Cờ đúng từ cli.py: `index --source delta|file [--limit N]`; `release [--prompt-version v1] [--template PATH] [--top-k 3] [--promote/--no-promote]`; `seed [--limit N] [--via-gateway]`; `inspect`, `ready` không cờ. Kỳ vọng: `index` có `points_upserted > 0`; `release` in `registered lab28-rag-release v1 as champion`; `seed` không có `rejected` (exit 0); `inspect` đủ 6 mục `kafka, spark-delta, feast, qdrant, mlflow, vllm`; `ready` = `ready` hoặc `degraded` (vLLM chưa có GPU → `degraded` là hợp lệ; `not_ready` exit 1 phải điều tra).

## Bước 5 — Full profile + 5 journeys

```text
docker compose --env-file ports.template --profile full up -d --build --wait
uv run pytest integration-tests/test_j1_golden_path.py -q
uv run pytest integration-tests/test_j2_idempotent_replay.py -q
uv run pytest integration-tests/test_j3_promotion_rollback.py -q
uv run pytest integration-tests/test_j4_degraded_recovery.py -q
uv run pytest integration-tests/test_j5_trace_metrics_continuity.py -q
uv run pytest integration-tests -m "not gpu and not langsmith" -q
```

Kỳ vọng: J1 dữ liệu đi API→Kafka→Airflow→Delta→Feast/Qdrant→answer; J2 replay không tăng row; J3 promote+rollback alias; J4 degraded rồi recover không mất dữ liệu; J5 trace/metrics xuyên suốt. Gate `gpu`: conftest tự SKIP khi endpoint không chứng minh là vLLM thật (`/version`, `/v1/models`, metric `vllm:`) — không có GPU thì các test GPU skip, báo IP07 `UNVERIFIED` trong ANSWERS.md, KHÔNG giả lập. Gate `langsmith`: skip khi thiếu `LANGSMITH_API_KEY`; phần Jaeger local của IP10 vẫn phải chạy. Nếu stack chưa up, cả session fail một lần kèm danh sách endpoint unreachable.

## Bước 6 — 10 evidence files (11 file, ip09 có 2)

```text
uv run lab28 evidence --out evidence
uv run lab28 integration
```

`lab28 evidence` tự ghi ĐƯỢC 4 file + báo 6 file `outstanding`:
- `ip03-delta-history.json`, `ip05-qdrant-search.json`, `ip06-mlflow-release.json`, `ip07-vllm-identity.json` (ip07 cần vLLM thật; không GPU → UNVERIFIED).

6 file còn lại do test ghi qua `stack.write_evidence`:
- `ip01-kafka-consume.json` ← `test_j1_golden_path.py`
- `ip02-airflow-run.json` ← `test_j1_golden_path.py`
- `ip04-feast-online.json` ← `test_j1_golden_path.py`
- `ip08-gateway.json` ← `test_gateway_rate_limit.py`
- `ip09-prometheus-targets.json` + `ip09-grafana-dashboards.json` ← `test_prometheus_targets.py`
- `ip10-trace.json` ← `test_j5_trace_metrics_continuity.py` (và `test_trace_span_coverage.py`, cần GPU cho 5 span phía inference)

Kỳ vọng: `dir evidence` đếm đủ 11 file; `lab28 integration` in `IP01..IP10` kèm status và `x/y verified points passing`.

## Bước 7 — Load profile

```text
uv run python load-tests/run_profile.py --requests 200 --workers 8
uv run python load-tests/run_profile.py --requests 200 --workers 16
```

Cờ đúng: `--url` (mặc định `http://localhost:8080`, probe `GET /ready`), `--requests`, `--workers`. Kỳ vọng: JSON in `status_counts` toàn `200` và `latency_ms.p50/p95/p99`. Ghi thêm CPU/RAM, Kafka lag, error rate (runbooks/performance.md). UI: Grafana :3000, Prometheus :9090/targets, Jaeger :16686, Airflow :8082, MLflow :5000, Qdrant :6333/dashboard, gateway :8080/health, API docs :8000/docs.

## Bước 8 — Failure injection (runbooks/failure-injection.md)

```text
docker compose stop feast   # degraded, reason chứa "feature store"
docker compose start feast  # lookup trở lại
docker compose stop qdrant  # not_ready/protected request; start lại → same count
docker compose stop kafka   # ingestion 503; start lại → consume once
uv run lab28 dlq            # xem dead letters
uv run lab28 dlq --replay --limit 20   # CHỈ sau khi sửa nguyên nhân
```

Kỳ vọng: mỗi scenario có timestamp trước/sau + recovery proof như bảng runbook. KHÔNG dùng `down -v`/`lab28 reset` trong demo recovery (xóa state).

## Bước 9 — GitOps validation + rollback (runbooks/gitops-rollback.md)

```text
uv run python scripts/validate_manifests.py
```

Exit 0 = `deploy/kubernetes/base/*.yaml` đủ 9 kinds (Deployment, Service, ServiceAccount, ConfigMap, HPA, PDB, NetworkPolicy, Gateway, HTTPRoute). Demo trên cluster (kind): build image tag bất biến → đổi tag trong Git → review diff → Argo CD sync (`gitops/application.yaml`, `selfHeal: true`) → tạo drift, quan sát self-heal → revert revision Git → kiểm tra replicas, gateway, trace. Ghi screenshot diff + sync state.

## Bước 10 — Nộp bài (checklist SUBMISSION.md)

```text
uv run ruff check .
uv run python scripts/verify_matrix.py
uv run python scripts/check_portability.py
uv run python scripts/validate_manifests.py
uv run pytest tests -q
uv run pytest integration-tests -m "not gpu and not langsmith" -q
```

| Item SUBMISSION | Bằng chứng |
|---|---|
| 1. `integration-report.json` + output fast suite | `evidence/integration-report.json` (Bước 6) + log lệnh trên |
| 2. 10 evidence files | 11 file `evidence/ip01..ip10` (Bước 6) |
| 3. Architecture/ownership diagram | `docs/images/lab28-architecture-overview.png` + `docs/team-role-cards.md` |
| 4. Happy-path trace (run/trace/Delta/MLflow IDs) | ip02 (run_id) + ip10 (trace_id) + ip03 (delta version) + ip06 (model version) |
| 5. Failure/recovery + no-data-loss | log Bước 8 + J4 + J2 |
| 6. P50/P95/P99 + bottleneck | JSON Bước 7 + ghi chú hardware/model/concurrency |
| 7. GitOps validation + drift/rollback | validate_manifests + screenshot Argo (Bước 9) |
| 8. `ANSWERS.md` | tự viết: trade-offs, production gaps, đóng góp từng thành viên, GPU/LangSmith `UNVERIFIED` nếu không có endpoint |

Không nộp: secret, `.env`, DB, cache, weights, `.lab28/`. Dọn: `docker compose --env-file ports.template --profile full down --remove-orphans` (giữ dữ liệu).

## Discrepancies đã phát hiện
1. `ip07-vllm-identity.json` chỉ sinh được khi có vLLM THẬT (cả `lab28 evidence` lẫn gate `gpu` cùng chặn mock) — máy không GPU phải nộp kèm tuyên bố UNVERIFIED.
2. `verify_starter_state.py` mong đợi 4 test fail — chỉ dùng TRƯỚC khi code xong, không nằm trong chuỗi verify cuối.
3. Ma trận ghi "10 evidence files" nhưng thực tế là 11 file vì IP09 có 2 (`ip09-prometheus-targets.json`, `ip09-grafana-dashboards.json`).
4. `ip06-mlflow-release.json` được ghi bởi CẢ `lab28 evidence` lẫn J3; J3 ghi phiên bản giàu hơn (provenance), nên chạy J3 SAU `lab28 evidence`.
5. J1/J3/J4/J5/prometheus/trace-span đều có test `gpu` bên trong — không GPU vẫn chạy được phần lớn bằng `-m "not gpu and not langsmith"`, nhưng ip10 cần 5 span phía vLLM nên file này phụ thuộc GPU.
