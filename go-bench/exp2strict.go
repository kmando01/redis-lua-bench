package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ── 재검증 목적 ──────────────────────────────────────────────────────────────
//
// 기존 exp2에서 Functions(FCALL)가 Lua(EVAL)보다 80% 빠르게 나왔다.
// 원인이 두 가지로 분리되지 않은 상태:
//   ① 네트워크 페이로드 절감: EVAL은 매번 스크립트 전체(~150B) 전송
//                             EVALSHA는 SHA 40자만 전송
//   ② FCALL 자체 최적화:     Functions 실행 경로의 서버 사이드 개선
//
// 이 실험이 측정하는 것:
//   EVAL → EVALSHA TPS 차이 = ①의 기여분
//   EVALSHA → FCALL TPS 차이 = ②의 기여분
//   --measure-network-bytes = INFO stats delta로 bytes/op 실측
//
// EVALSHA ≈ FCALL 이면 → 80% 우위는 pure network payload 절감
// FCALL >> EVALSHA 이면 → Functions에 별도 실행 최적화 존재
// ────────────────────────────────────────────────────────────────────────────

const strictScript = `
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current >= tonumber(ARGV[1]) then return -1 end
return redis.call('INCR', KEYS[1])
`

// Exp2StrictResult : exp2 기본 Result + 네트워크 바이트 측정 추가
type Exp2StrictResult struct {
	Result
	Variant        string  `json:"variant"`          // eval | evalsha | fcall
	WarmupOps      int     `json:"warmup_ops"`
	NetInBytesPerOp  float64 `json:"net_in_bytes_per_op"`
	NetOutBytesPerOp float64 `json:"net_out_bytes_per_op"`
	ScriptSHA      string  `json:"script_sha,omitempty"`
}

// ── 네트워크 바이트 스냅샷 ────────────────────────────────────────────────

type netSnapshot struct {
	inputBytes  int64
	outputBytes int64
}

func takeNetSnapshot(rdb *redis.Client) netSnapshot {
	info, err := rdb.Info(ctx, "stats").Result()
	if err != nil {
		return netSnapshot{}
	}
	var snap netSnapshot
	for _, line := range strings.Split(info, "\n") {
		kv := strings.SplitN(strings.TrimSpace(line), ":", 2)
		if len(kv) != 2 {
			continue
		}
		v, _ := strconv.ParseInt(strings.TrimSpace(kv[1]), 10, 64)
		switch kv[0] {
		case "total_net_input_bytes":
			snap.inputBytes = v
		case "total_net_output_bytes":
			snap.outputBytes = v
		}
	}
	return snap
}

// ── 3가지 카운터 구현 (명시적 분리) ─────────────────────────────────────────

// evalStrict: 항상 EVAL 사용. go-redis Script 헬퍼는 내부적으로 EVALSHA를
// 시도하므로 여기서는 rdb.Eval()을 직접 호출해 스크립트 본문을 매번 전송.
func evalStrict(rdb *redis.Client, key string, limit int64) int64 {
	r, err := rdb.Eval(ctx, strictScript, []string{key}, limit).Int64()
	if err != nil {
		return -1
	}
	return r
}

// evalSHAStrict: 미리 SCRIPT LOAD로 SHA를 캐싱한 뒤 항상 EVALSHA 사용.
// SHA 40자만 전송 → EVAL 대비 ~60% 더 작은 요청 페이로드.
func evalSHAStrict(rdb *redis.Client, sha, key string, limit int64) int64 {
	r, err := rdb.EvalSha(ctx, sha, []string{key}, limit).Int64()
	if err != nil {
		return -1
	}
	return r
}

// fcallStrict: Functions API 사용. 이미 로드된 limit_incr 함수 호출.
func fcallStrict(rdb *redis.Client, key string, limit int64) int64 {
	r, err := rdb.Do(ctx, "FCALL", "limit_incr", 1, key, limit).Int64()
	if err != nil {
		return -1
	}
	return r
}

// ── 엄격 벤치마크 러너 ────────────────────────────────────────────────────

func runStrictBench(
	variant string,
	workers, warmupOps, measureOps int,
	limit int64,
	rdb *redis.Client,
	fn func(key string) int64,
) Exp2StrictResult {

	warmupKey := fmt.Sprintf("strict:warmup:%s:%d", variant, time.Now().UnixNano())
	measureKey := fmt.Sprintf("strict:measure:%s:%d", variant, time.Now().UnixNano())
	rdb.Del(ctx, warmupKey, measureKey)

	// ── 워밍업 (결과 미포함) ──────────────────────────────────────────────
	fmt.Printf("  [%s] 워밍업 %d ops...\n", variant, warmupOps)
	var wwg sync.WaitGroup
	perWWorker := warmupOps / workers
	for w := 0; w < workers; w++ {
		wwg.Add(1)
		go func() {
			defer wwg.Done()
			for i := 0; i < perWWorker; i++ {
				fn(warmupKey)
			}
		}()
	}
	wwg.Wait()
	fmt.Printf("  [%s] 워밍업 완료\n", variant)

	// 워밍업 후 잠시 안정화
	time.Sleep(300 * time.Millisecond)

	// ── 네트워크 스냅샷 (측정 전) ─────────────────────────────────────────
	snapBefore := takeNetSnapshot(rdb)

	// ── 본 측정 ──────────────────────────────────────────────────────────
	var (
		mu      sync.Mutex
		lats    []float64
		success int64
		fail    int64
		wg      sync.WaitGroup
	)

	perWorker := measureOps / workers
	start := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]float64, 0, perWorker)
			var ls, lf int64
			for i := 0; i < perWorker; i++ {
				t0 := time.Now()
				v := fn(measureKey)
				local = append(local, time.Since(t0).Seconds())
				if v > 0 {
					ls++
				} else {
					lf++
				}
			}
			mu.Lock()
			lats = append(lats, local...)
			atomic.AddInt64(&success, ls)
			atomic.AddInt64(&fail, lf)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	// ── 네트워크 스냅샷 (측정 후) ─────────────────────────────────────────
	snapAfter := takeNetSnapshot(rdb)
	completedOps := int64(len(lats))
	netInPerOp := float64(snapAfter.inputBytes-snapBefore.inputBytes) / float64(completedOps)
	netOutPerOp := float64(snapAfter.outputBytes-snapBefore.outputBytes) / float64(completedOps)

	finalVal, _ := rdb.Get(ctx, measureKey).Int64()
	tps := float64(completedOps) / elapsed

	base := Result{
		Name:      "strict_" + variant,
		Workers:   workers,
		TotalOps:  measureOps,
		Limit:     limit,
		Success:   success,
		Fail:      fail,
		FinalVal:  finalVal,
		Accurate:  finalVal == limit,
		TPS:       tps,
		Latency:   percentiles(lats),
		ElapsedMs: elapsed * 1000,
	}

	res := Exp2StrictResult{
		Result:           base,
		Variant:          variant,
		WarmupOps:        warmupOps,
		NetInBytesPerOp:  netInPerOp,
		NetOutBytesPerOp: netOutPerOp,
	}

	fmt.Printf("\n=== exp2-strict: %s ===\n", variant)
	fmt.Printf("tps=%.0f  success=%d  fail=%d  final=%d  accurate=%v\n",
		tps, success, fail, finalVal, base.Accurate)
	fmt.Printf("p50=%.2fms  p95=%.2fms  p99=%.2fms  max=%.2fms\n",
		base.Latency["p50"], base.Latency["p95"],
		base.Latency["p99"], base.Latency["max"])
	fmt.Printf("net_in=%.1f B/op  net_out=%.1f B/op\n", netInPerOp, netOutPerOp)

	return res
}

// ── 메인 진입점 ────────────────────────────────────────────────────────────

func runExp2Strict(rdb *redis.Client) {
	const (
		workers    = 1000
		warmupOps  = 5000
		measureOps = 10000
		limit      = int64(100)
	)

	fmt.Println("=== exp2-strict: EVAL / EVALSHA / FCALL 분리 재검증 ===")
	fmt.Printf("workers=%d  warmup=%d  measure=%d  limit=%d\n\n",
		workers, warmupOps, measureOps, limit)

	// EVALSHA용 SHA 사전 로드
	sha, err := rdb.ScriptLoad(ctx, strictScript).Result()
	if err != nil {
		fmt.Println("SCRIPT LOAD 실패:", err)
		os.Exit(1)
	}
	fmt.Printf("Script SHA: %s\n\n", sha)

	results := []Exp2StrictResult{}

	// ── 1. EVAL (항상 스크립트 본문 전송) ──────────────────────────────────
	results = append(results, runStrictBench(
		"eval", workers, warmupOps, measureOps, limit, rdb,
		func(key string) int64 { return evalStrict(rdb, key, limit) },
	))

	// ── 2. EVALSHA (SHA 40자만 전송) ────────────────────────────────────────
	r2 := runStrictBench(
		"evalsha", workers, warmupOps, measureOps, limit, rdb,
		func(key string) int64 { return evalSHAStrict(rdb, sha, key, limit) },
	)
	r2.ScriptSHA = sha
	results = append(results, r2)

	// ── 3. FCALL (Functions API) ────────────────────────────────────────────
	results = append(results, runStrictBench(
		"fcall", workers, warmupOps, measureOps, limit, rdb,
		func(key string) int64 { return fcallStrict(rdb, key, limit) },
	))

	// ── 원인 분리 분석 ────────────────────────────────────────────────────
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("원인 분리 분석")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	byVariant := map[string]Exp2StrictResult{}
	for _, r := range results {
		byVariant[r.Variant] = r
	}

	evalTPS := byVariant["eval"].TPS
	evalshaТPS := byVariant["evalsha"].TPS
	fcallTPS := byVariant["fcall"].TPS

	evalNet := byVariant["eval"].NetInBytesPerOp
	evalshaNet := byVariant["evalsha"].NetInBytesPerOp
	fcallNet := byVariant["fcall"].NetInBytesPerOp

	fmt.Printf("\n%-12s %10s %15s\n", "variant", "TPS", "net_in B/op")
	fmt.Printf("%-12s %10.0f %15.1f\n", "eval",    evalTPS,    evalNet)
	fmt.Printf("%-12s %10.0f %15.1f\n", "evalsha", evalshaТPS, evalshaNet)
	fmt.Printf("%-12s %10.0f %15.1f\n", "fcall",   fcallTPS,   fcallNet)

	fmt.Printf("\n▶ EVAL → EVALSHA TPS 차이 (네트워크 페이로드 절감 효과, ①):\n")
	fmt.Printf("  TPS 변화: %.0f → %.0f  (+%.1f%%)\n",
		evalTPS, evalshaТPS, (evalshaТPS-evalTPS)/evalTPS*100)
	fmt.Printf("  네트워크 절감: %.1f B/op → %.1f B/op  (%.1f%% 감소)\n",
		evalNet, evalshaNet, (evalNet-evalshaNet)/evalNet*100)

	fmt.Printf("\n▶ EVALSHA → FCALL TPS 차이 (Functions 자체 최적화, ②):\n")
	fmt.Printf("  TPS 변화: %.0f → %.0f  (%+.1f%%)\n",
		evalshaТPS, fcallTPS, (fcallTPS-evalshaТPS)/evalshaТPS*100)

	fmt.Printf("\n▶ 전체 EVAL → FCALL: %.0f → %.0f  (+%.1f%%)\n",
		evalTPS, fcallTPS, (fcallTPS-evalTPS)/evalTPS*100)

	ratio12 := (evalshaТPS - evalTPS) / (fcallTPS - evalTPS) * 100
	ratio23 := (fcallTPS - evalshaТPS) / (fcallTPS - evalTPS) * 100
	if fcallTPS > evalTPS {
		fmt.Printf("\n원인 기여도:\n")
		fmt.Printf("  ① 네트워크 페이로드 절감 (EVAL→EVALSHA): %.1f%%\n", ratio12)
		fmt.Printf("  ② Functions 자체 최적화 (EVALSHA→FCALL): %.1f%%\n", ratio23)
	}

	// TPS 랭킹
	fmt.Println("\n--- TPS 랭킹 ---")
	sorted := make([]Exp2StrictResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].TPS > sorted[j].TPS })
	for i, r := range sorted {
		fmt.Printf("%d. %-10s  %.0f ops/s  net_in=%.1fB/op\n",
			i+1, r.Variant, r.TPS, r.NetInBytesPerOp)
	}

	// JSON 저장
	os.MkdirAll("../results", 0755)
	data, _ := json.MarshalIndent(results, "", "  ")
	_ = os.WriteFile("../results/exp2_strict.json", data, 0644)
	fmt.Println("\n결과 저장: ../results/exp2_strict.json")
}
