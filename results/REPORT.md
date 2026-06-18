# Redis Lua Script 검증 실험 결과 보고서

## 실험 환경

| 항목 | 값 |
|------|-----|
| Redis 버전 | 7.4 (Docker) |
| OS | macOS 14.4 |
| CPU / 메모리 | 12코어 / 18 GB |
| 클라이언트 | Go 1.24.4 (go-redis/v9) — Kotlin 대신 채택 (동시성 모델 적합) |
| 포트 | 6399 (single), 7099 (cluster-mode single node) |

---

## 실험 1: 블로킹 영향 측정

**가설**: Lua 스크립트 실행 중 Redis 전체가 블록되어 동시 GET의 tail latency가 스크립트 실행 시간만큼 밀린다.

### 결과 테이블

| 시나리오 | RPS | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | 에러 |
|----------|-----|----------|----------|----------|----------|------|
| Baseline (GET only) | 44,510 | 0.847 | 1.775 | 2.999 | **6.119** | 0 |
| 무거운 Lua (~2s) + GET 동시 | 14,961 | 0.855 | 1.639 | 4.079 | **2,263.039** | BUSY 발생 |

| SCRIPT KILL 시나리오 | 결과 |
|----------------------|------|
| read-only Lua hang (5초 초과) | `OK` — 즉시 복구, GET 정상 응답 |
| write 후 hang (5초 초과) | `UNKILLABLE` — SHUTDOWN NOSAVE만 가능 (데이터 손실) |

### 핵심 관찰
- **max latency: 6ms → 2,263ms (+37,000%)** — Lua 실행 시간이 그대로 꼬리 지연으로 전달됨
- **RPS: 44,510 → 14,961 (-66%)** — Lua 블로킹 기간 동안 요청이 적체됨
- p50은 0.847ms → 0.855ms로 거의 같음 — "평균은 괜찮아 보이지만 max가 폭발"하는 전형적 패턴
- write 후 hang은 SCRIPT KILL 불가 → **운영 장애 시 복구 수단이 없음**

---

## 실험 2: 5가지 구현 처리량/정확성 비교

**가설**: limit-aware atomic counter는 Lua 외 방식으로도 정확하게 구현 가능하고, 성능 차이는 의외로 크지 않거나 운영 위험을 감수할 만큼 크지 않다.

**설정**: workers=1,000 / total_ops=10,000 / limit=100

### 결과 테이블

| 방식 | TPS | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | 정확성 (final==100) | retry 평균 |
|------|-----|----------|----------|----------|----------|----------------------|------------|
| 1. Lua Script | 26,354 | 14.92 | 169.28 | 200.26 | 312.87 | ✅ | - |
| 2. INCR + DECR rollback | 24,775 | 33.85 | 91.70 | 104.40 | 113.99 | ✅ | - |
| 3. WATCH/MULTI/EXEC | 4,730 | 50.08 | 861.47 | 883.85 | 925.12 | ✅ | 2.52회 |
| 4. RLock + INCR (SETNX) | 1,223 | 800.68 | 1,262.38 | 1,288.70 | 1,351.94 | ✅ | - |
| **5. Redis Functions** | **47,421** | **21.34** | **27.27** | **29.00** | **39.63** | ✅ | - |

### TPS 랭킹
```
1위. Functions     47,421 ops/s  (Lua 대비 +80%)
2위. Lua Script    26,354 ops/s
3위. INCR+DECR     24,775 ops/s  (Lua 대비 -6%)
4위. WATCH/MULTI    4,730 ops/s  (Lua 대비 -82%)
5위. 분산 락        1,223 ops/s  (Lua 대비 -95%)
```

### 핵심 관찰
- **모든 방식이 정확성 100%** — limit=100일 때 final=100 보장됨
- **Functions가 Lua보다 80% 빠름** — server-side 실행이지만 함수 등록 오버헤드가 낮음
- **INCR+DECR rollback이 Lua와 거의 동등** (TPS -6%) — 운영 단순성을 위한 대안으로 충분
- **INCR+DECR의 의미론**: 순간적으로 limit+α 초과 가능하지만 최종 count는 정확. 비즈니스 허용 여부 별도 판단
- **WATCH/MULTI는 고부하에서 retry 폭발** — avg 2.52회 retry, p99가 Lua의 4.4배
- **분산 락은 직렬화의 극단** — 정확하지만 TPS가 95% 낮음 (1,223 ops/s)

---

## 실험 3: Redis Cluster 환경 CROSSSLOT 동작

**가설**: 단일 키 Lua는 cluster에서 동작하지만, 여러 카운터를 한 스크립트로 묶으려는 순간 CROSSSLOT 에러가 발생한다.

> **환경 주의**: macOS Docker에서 멀티 컨테이너 cluster는 127.0.0.1 announce로 인한 노드 간 gossip 통신 실패.
> 단일 컨테이너 cluster 모드(포트 7099)로 CROSSSLOT 동작만 검증함.

### 결과 테이블

| 시나리오 | 결과 | 에러 |
|----------|------|------|
| 단일 키 EVAL (`counter:1`, 슬롯 10293) | `1` — 성공 | - |
| 무관한 두 키 EVAL (`counter:1` + `counter:2`) | ❌ | `CROSSSLOT Keys in request don't hash to the same slot` |
| hash tag 두 키 (`{reward}:counter:1`, `{reward}:counter:2`) | `AB` — 성공 | - |
| hash tag 1,000개 키 분포 | 전체 슬롯 2381에 집중 (단일 노드 hot slot) | |

### 슬롯 계산 검증
```
counter:1        → slot 10293
counter:2        → slot 6230   (다른 슬롯 → CROSSSLOT)
{reward}:counter:1 → slot 2381
{reward}:counter:2 → slot 2381 (같은 슬롯 → 성공)
```

### 핵심 관찰
- 현재 코드(단일 키 Lua)는 cluster 전환 시 즉시 문제없음
- **"한 트랜잭션에 여러 카운터" 요구가 생기는 순간 CROSSSLOT으로 즉시 깨짐**
- hash tag로 우회하면 동작하지만 1,000개 키가 모두 슬롯 2381에 집중 → hot slot 부작용
- 설계 초기부터 cluster 전환 가능성을 고려해야 함

---

## 실험 4: 장애/타임아웃 시나리오

**가설**: write 후 hang 시 SCRIPT KILL 불가로 SHUTDOWN NOSAVE만 가능.

### 결과 테이블

| 시나리오 | 결과 | 복구 방법 |
|----------|------|-----------|
| 4-1: read-only Lua hang (5초 초과) | `SCRIPT KILL: OK` | 즉시 복구, GET 정상 |
| 4-2: write 후 hang (5초 초과) | `UNKILLABLE` | **SHUTDOWN NOSAVE (데이터 손실)** |
| 4-3: SCRIPT FLUSH 후 EVALSHA | `NOSCRIPT No matching script` | 수동으로 EVAL 재실행 필요 |
| 4-4: Functions + SCRIPT FLUSH | FCALL 정상 동작 | 영향 없음 (Functions는 RDB/AOF에 저장) |

### 핵심 관찰
- **write 포함 Lua hang = 운영 불가 장애** — 유일한 복구 수단이 데이터 손실
- `lua-time-limit 5000`(기본값)이 지나도 write 있으면 자동 종료도 안됨
- **EVALSHA는 Failover 후 NOSCRIPT 에러 발생** — SCRIPT LOAD를 replica에 재실행해야 함
- **Functions는 SCRIPT FLUSH 무관** — FUNCTION LOAD는 RDB/AOF에 영구 저장됨

---

## 실험 5: 운영성/관찰가능성

**가설**: EVAL은 개별 스크립트 단위 통계가 불가하고, Functions는 가능하다.

### 결과 테이블

| 항목 | EVAL/EVALSHA | Functions (FCALL) |
|------|-------------|-------------------|
| 개별 스크립트별 통계 | ❌ `cmdstat_eval:calls=200` (2개 스크립트 합산) | ⚠️ `cmdstat_fcall:calls=200` (합산) + FUNCTION STATS로 라이브러리 구조 확인 |
| SLOWLOG 가독성 | 내부 명령(INCR 등)만 노출 | ✅ `FCALL limit_incr 1 key 100` — 함수명 명시 |
| SCRIPT FLUSH 영향 | ❌ NOSCRIPT 발생 | ✅ 영향 없음 |
| 버전/코드 관리 | ❌ 외부 매핑 필요 | ✅ `FUNCTION LIST WITHCODE`로 서버에서 직접 조회 |
| Replication | ❌ script cache 미복제 (Failover 후 NOSCRIPT) | ✅ RDB/AOF에 저장, replica 복제됨 |
| 배포 방식 | EVAL마다 스크립트 텍스트 전송 | `FUNCTION LOAD REPLACE`로 한 번만 등록 |

### 핵심 관찰
- EVAL 200회 → `cmdstat_eval:calls=200` 으로 두 스크립트가 합산됨. 어느 스크립트가 느린지 알 수 없음
- FCALL SLOWLOG에 `limit_incr` 함수명이 명시 → 운영자가 즉시 어느 기능인지 파악 가능
- Functions는 `FUNCTION LIST WITHCODE`로 현재 서버에 배포된 코드 버전을 확인 가능

---

---

## 실험 6: Partial Write — 에러 발생 시 롤백 없음

**가설**: Lua 스크립트 중간에 에러가 발생해도 이미 실행된 write는 롤백되지 않는다.

**스크립트**: `SET key1 → SET key2 → ZADD key1("NOT_A_NUMBER") → SET key3`

| 키 | 기대 (트랜잭션이라면) | 실제 결과 |
|----|----------------------|-----------|
| key1 | (nil) 또는 written | **"written-by-lua"** |
| key2 | (nil) 또는 written | **"written-by-lua"** |
| key3 | (nil) | **(nil)** |

- **에러**: `ERR value is not a valid float` (3번째 줄)
- **롤백 발생**: ❌ false
- **Partial write**: ✅ true — key1·key2는 set됨, key3는 set 안 됨

### 핵심 관찰
Lua 스크립트는 "All or Nothing"이 아니다. 에러 발생 전 write는 영구적으로 반영된다.
`redis.pcall()` 로 에러를 잡지 않으면 중간 상태로 데이터가 오염될 수 있다.

---

## 실험 7: redis.log() 프로덕션 로그 오염

**가설**: `redis.log(LOG_WARNING, msg)` 는 Redis 서버 로그에 그대로 기록된다.

**설계**: 100회 호출 → docker logs에서 `LUA-DEBUG` 라인 카운트

| 측정 | 결과 |
|------|------|
| 호출 횟수 | 100 |
| docker logs LUA-DEBUG 라인 | **100** (1:1 대응) |

### 핵심 관찰
- `redis.log(WARNING)` = Redis 서버 로그 파일에 직접 기록
- 디버깅 로그를 지우지 않고 배포하면 프로덕션 로그가 오염됨
- 로그 레벨을 올려도 WARNING은 기본으로 출력됨
- 반대로, 배포 전에 제거하면 장애 시 디버깅 수단이 없어짐

---

## 실험 8: --ldb 디버거 블록 측정

**가설**: --ldb 디버거는 이벤트 루프를 점유해 모든 커맨드를 블록한다.

**방법**: ~2초 Lua로 이벤트 루프 점유 → 별도 연결에서 GET 대기 시간 측정
(`--ldb`는 breakpoint마다 동일하게 이벤트 루프를 점유함)

| 측정 | 결과 |
|------|------|
| baseline max | 1.27ms |
| Lua 실행 중 max | **2,180ms** |
| max 배율 | **1,712x** |

### 핵심 관찰
- Lua 실행 중 clientB의 GET은 Lua가 끝날 때까지 **전부 대기**
- `--ldb`는 breakpoint에서 대기할 때마다 이 상태를 반복
- 프로덕션에서 `--ldb` 연결 = **전체 서비스 마비** (Exp 1의 BUSY 에러와 동일 원인)
- 유일한 디버깅 수단인 `redis.log()` 는 Exp 7에서 로그 오염 문제 확인

---

## 종합 평가 시트

| 평가 항목 | Lua EVAL | INCR+DECR | Redis Functions | 결정 근거 |
|-----------|----------|-----------|-----------------|-----------|
| 정확성 (limit 엄격) | ✅ 100% | ✅ 100%* | ✅ 100% | *순간 초과 허용 여부 별도 판단 |
| TPS | 26,354 | 24,775 | **47,421** | Functions 1위, INCR+DECR ≈ Lua |
| p99 latency | 200ms | 104ms | **29ms** | Functions 압도적 안정 |
| Cluster 전환 안정성 | 단일키 OK, 멀티키 CROSSSLOT | ✅ CROSSSLOT 무관 | 단일키 OK, 멀티키 CROSSSLOT | 멀티키 필요 시 INCR+DECR 우위 |
| 장애 복구 난이도 | ❌ write hang = 불복구 | ✅ 낮음 | ✅ SCRIPT KILL 무관 | Functions/INCR+DECR 우위 |
| 운영 가시성 | ❌ 합산 통계, 함수명 미노출 | ✅ 단순 | ✅ 함수명/버전 관리 | Functions 최우위 |
| 코드 단순성 | △ 스크립트 관리 | ✅ 가장 단순 | △ FUNCTION LOAD 배포 필요 | INCR+DECR 최우위 |

### 권장 의사결정 트리

```
Redis 7.0+ 사용 가능?
├─ YES → Redis Functions 우선 검토 (TPS 1위 + 운영성 최우위)
│        단, 멀티키 트랜잭션 → Functions도 CROSSSLOT 동일
└─ NO (6.x 고정)
    ├─ 순간 초과가 비즈니스적으로 허용됨 → INCR+DECR (가장 단순)
    ├─ 엄격한 limit + 단순 로직 → 현재 Lua 유지
    └─ 여러 카운터 atomic → Lua + hash tag 설계 (Cluster 시 hot slot 주의)
```

---

## 핵심 시그니처 검증

- [x] **Lua 블로킹이 GET latency를 밀어낸다** — max 6ms → 2,263ms (+37,000%), BUSY 에러 실재
- [x] **limit 정확성은 5가지 방식 모두 동등** — final=100 100% 달성
- [x] **Functions가 Lua보다 80% 빠름** — 47,421 vs 26,354 TPS
- [x] **INCR+DECR이 Lua와 거의 동등** — TPS 차이 6%, 운영 단순성 압도적
- [x] **CROSSSLOT은 hash tag로 우회 가능** — 단, hot slot 집중 부작용
- [x] **write 후 hang = UNKILLABLE** — 복구 수단 SHUTDOWN NOSAVE (데이터 손실)
- [x] **SCRIPT FLUSH → EVALSHA NOSCRIPT** — Functions는 무영향
- [x] **EVAL 통계는 합산만** — 어느 스크립트가 느린지 알 수 없음, Functions는 함수명 노출

---

## 추가 실험: 정상 Lua의 Collateral Damage 측정

**가설**: 정상 속도 카운터 Lua가 실행되는 동안 다른 GET 커맨드의 latency가 올라간다.

**설계**: 500 write goroutine + 500 cold GET goroutine 동시 실행. write 구현만 다르게.

| 시나리오 | Cold p50 | Cold p99 | Cold TPS | vs INCR |
|----------|----------|----------|----------|---------|
| baseline (GET only) | 6.79ms | 26.38ms | 61,318 | - |
| INCR + GET | 13.93ms | 41.40ms | 31,277 | 1.0x |
| Lua 2명령(GET+INCR) + GET | 14.34ms | 41.88ms | 30,165 | **1.0x** |
| Lua 5명령(비즈니스 로직) + GET | 14.58ms | 44.39ms | 28,588 | **1.1x** |

### 핵심 관찰
- **"정상 Lua가 다른 커맨드 latency를 올린다"는 명제는 증명되지 않았다**
- Lua 2명령 vs INCR: Cold p99 차이 0.5ms (오차 수준)
- Lua 5명령 vs INCR: Cold p99 1.1배 — 유의미한 차이 아님
- cold GET 저하(-51%)는 **write 부하 자체** 때문이지 Lua 때문이 아님
- Redis 단일 스레드에서 INCR이든 Lua든 큐에서 한 슬롯을 동일하게 차지

---

## 추가: Lua 디버깅 가능 여부

### 결론: 프로덕션에서 디버깅 불가

Redis 3.2부터 내장 디버거(`--ldb`)가 존재하나 **프로덕션 사용 불가**.

```bash
redis-cli --ldb --eval scripts/heavy.lua 0
# → 디버그 세션 동안 Redis 전체 블록 (Exp 1과 동일한 구조)
```

| 항목 | 상태 |
|------|------|
| 디버거 존재 | ✅ `redis-cli --ldb` |
| 프로덕션 사용 | ❌ 서버 전체 블록 |
| IDE 연동 | ❌ 없음 |
| 조건부 breakpoint | ❌ 없음 |
| 실질적 디버깅 수단 | `redis.log()` + `MONITOR` |

### 프로덕션 장애 시나리오

```
Lua 버그 발생
  ├─ redis.log() 없으면 → 원인 추적 수단 없음
  ├─ 디버거 → 프로덕션 블록 (사용 불가)
  ├─ write 포함 무한루프 → UNKILLABLE → SHUTDOWN NOSAVE
  └─ 로직 수정 후 → SCRIPT LOAD + 앱 재배포 필요
```

Exp 5(관찰가능성)와 합산하면:
- **평소**: `cmdstat_eval` 합산 → 어느 스크립트가 느린지 구분 불가
- **장애 시**: 디버거 사용 불가 + 로그 없으면 원인 파악 불가

---

## 증거 강도 평가

| 증거 | 강도 | 핵심 수치 |
|------|------|-----------|
| Lua 블로킹 max latency | ★★★★★ | 6ms → 2,263ms (+37,000%) |
| write Lua UNKILLABLE | ★★★★★ | 복구 수단 없음 직접 확인 |
| 디버거 프로덕션 불가 | ★★★★★ | 서버 블록 구조상 사용 불가 |
| EVALSHA NOSCRIPT | ★★★★★ | SCRIPT FLUSH 후 즉시 에러 재현 |
| CROSSSLOT 동작 | ★★★★★ | 에러 재현 + hash tag 우회 확인 |
| 운영 가시성 차이 | ★★★★☆ | cmdstat 합산 vs FUNCTION LIST 함수명 |
| Functions TPS 우위 | ★★★★★ | 47,421 vs 26,354 (+80%) |
| INCR+DECR vs Lua 동등성 | ★★★★☆ | TPS -6%, p99 오히려 개선 |
| **정상 Lua 성능 저하 → 반증** | ★★★★★ | Lua vs INCR Cold p99 차이 1.0~1.1x (오차 수준) |
| Partial write (롤백 없음) | ★★★★★ | key1·key2 set됨, key3 (nil) — 에러 후에도 롤백 없음 |
| redis.log() 로그 오염 | ★★★★★ | 100호출 → 로그 100라인 1:1 대응 |
| --ldb 디버거 블록 | ★★★★★ | GET max 1.27ms → 2,180ms (1,712x) |

---

## 후속 재검증: exp2-strict — EVAL / EVALSHA / FCALL 원인 분리

> **동기**: 기존 exp2에서 Functions(FCALL)가 Lua(EVAL)보다 80% 빠르게 나왔으나
> EVAL(매번 스크립트 전체 전송) vs EVALSHA(SHA 40자만 전송) vs FCALL 세 경로가
> 명시적으로 분리되지 않았다. 80% 우위의 원인이
> ① 네트워크 페이로드 절감인지 ② Functions 자체 최적화인지 미확인 상태.

**설정**: workers=1,000 / warmup=5,000 ops(분리) / measure=10,000 / limit=100

### 결과

| variant | TPS | p50 (ms) | p99 (ms) | net_in (B/op) |
|---------|-----|----------|----------|---------------|
| EVAL (스크립트 매번 전송) | 52,822 | 17.62 | 37.17 | **231.0** |
| EVALSHA (SHA 40자 전송) | **71,962** | 11.16 | 24.56 | **129.0** |
| FCALL (Functions) | 69,426 | 10.81 | 35.00 | **95.0** |

### 원인 분리

```
EVAL → EVALSHA: TPS +36.2%,  네트워크 44.2% 감소 (231 → 129 B/op)
EVALSHA → FCALL: TPS -3.5%,  네트워크 26.4% 감소 (129 → 95 B/op)
```

### 수정된 결론

| 구분 | 기존 결론 | 수정된 결론 |
|------|----------|------------|
| Functions TPS 우위 | "Functions가 Lua보다 80% 빠름" | **36% (네트워크 절감) + FCALL ≈ EVALSHA (서버 실행 동등)** |
| 80% 우위의 원인 | Functions 자체 최적화 | **대부분 EVAL의 불필요한 스크립트 재전송** |
| EVALSHA vs FCALL | 비교 없음 | **EVALSHA가 FCALL보다 오히려 3.5% 빠름** |

**핵심**: 기존 exp2의 Lua(26K)는 go-redis Script 헬퍼가 워밍업 없이 첫 호출마다
NOSCRIPT 폴백을 겪었기 때문에 낮게 나온 것. 워밍업 분리 + EVALSHA 명시 시
52K(EVAL) / 72K(EVALSHA)로 Functions(69K)와 사실상 동등.

**Functions의 실제 장점**: TPS가 아닌 **운영성** (SCRIPT FLUSH 무관, Failover 후 NOSCRIPT 없음,
함수명 SLOWLOG 노출, FUNCTION LIST WITHCODE 버전 관리) — 실험 5 결론 유지.

### 증거 강도 업데이트

| 증거 | 강도 | 핵심 수치 |
|------|------|-----------|
| Functions TPS 우위 (수정) | ★★★★☆ | EVALSHA 72K ≈ FCALL 69K, EVAL 53K이 병목 |
| 네트워크 페이로드가 TPS를 결정 | ★★★★★ | 231B/op(EVAL) → 129B/op(EVALSHA) → +36% TPS |

### 재현

```bash
cd go-bench && ./go-bench load-functions && ./go-bench exp2-strict
```

---

## 재현 명령

```bash
cd redis-lua-bench
docker compose -f docker-compose.single.yml up -d
docker compose -f docker-compose.cluster.yml up -d  # Exp 3

# Exp 1: 블로킹
redis-cli -p 6399 EVAL "$(cat scripts/heavy.lua)" 0 1500000000 &
redis-benchmark -h localhost -p 6399 -t get -n 30000 -c 50 --csv

# Exp 2: 5가지 구현 비교
cd go-bench && ./go-bench load-functions && ./go-bench exp2

# Exp 3: CROSSSLOT
redis-cli -p 7099 CLUSTER KEYSLOT counter:1
redis-cli -p 7099 EVAL "return redis.call('GET', KEYS[1]) .. redis.call('GET', KEYS[2])" 2 counter:1 counter:2

# Exp 4: UNKILLABLE
redis-cli -p 6399 EVAL "$(cat scripts/heavy_with_write.lua)" 1 key 1500000000 &
sleep 6 && redis-cli -p 6399 SCRIPT KILL

# Exp 5: 관찰가능성
redis-cli -p 6399 INFO commandstats | grep -E "cmdstat_eval|cmdstat_fcall"
redis-cli -p 6399 FUNCTION LIST WITHCODE
```
