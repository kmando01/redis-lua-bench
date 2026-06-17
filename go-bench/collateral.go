package main

// 실험: 정상 속도 Lua가 cold GET에 유발하는 collateral blocking 측정
//
// [Baseline]  500 cold goroutine → GET only (write 부하 없음)
// [Control]   500 write goroutine → INCR  +  500 cold goroutine → GET
// [Variant]   500 write goroutine → Lua GET+check+INCR  +  500 cold goroutine → GET
//
// 변수 고립: goroutine 수·ops 수 동일, 다른 것은 write 구현(INCR vs Lua)만
// cold GET latency가 Variant에서 더 높으면 "Lua가 다른 커맨드 latency를 올린다" 증명

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	writeWorkers = 500
	coldWorkers  = 500
	opsPerWorker = 2000
	coldKey      = "cold:read:key"
	hotKey       = "hot:counter:key"
)

var luaCounterScript = redis.NewScript(`
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current >= tonumber(ARGV[1]) then return -1 end
return redis.call('INCR', KEYS[1])
`)

// 현실적인 비즈니스 로직이 추가된 Lua:
// 카운터 증가 + 로그 키에 기록 + TTL 갱신 + 통계 키 업데이트 (5개 명령)
var luaHeavyScript = redis.NewScript(`
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current >= tonumber(ARGV[1]) then return -1 end
local next = redis.call('INCR', KEYS[1])
redis.call('RPUSH', KEYS[2], next)
redis.call('EXPIRE', KEYS[1], 3600)
redis.call('INCR', KEYS[3])
redis.call('HSET', KEYS[4], 'last', next)
return next
`)

type ScenarioResult struct {
	Name          string             `json:"name"`
	WriteOps      int64              `json:"write_ops"`
	ColdOps       int64              `json:"cold_ops"`
	TotalSec      float64            `json:"total_sec"`
	WriteTPS      float64            `json:"write_tps"`
	ColdTPS       float64            `json:"cold_tps"`
	WriteLatency  map[string]float64 `json:"write_latency_ms"`
	ColdLatency   map[string]float64 `json:"cold_latency_ms"`
}

func runScenario(
	name string,
	rdb *redis.Client,
	writeFn func() error, // nil이면 write 없음 (baseline)
) ScenarioResult {
	// 초기화
	rdb.Del(ctx, hotKey)
	rdb.Set(ctx, coldKey, "hello", 0)

	var (
		mu           sync.Mutex
		writeLats    []float64
		coldLats     []float64
		wg           sync.WaitGroup
	)

	start := time.Now()

	// write goroutines
	if writeFn != nil {
		for w := 0; w < writeWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				local := make([]float64, 0, opsPerWorker)
				for i := 0; i < opsPerWorker; i++ {
					t0 := time.Now()
					writeFn()
					local = append(local, time.Since(t0).Seconds())
				}
				mu.Lock()
				writeLats = append(writeLats, local...)
				mu.Unlock()
			}()
		}
	}

	// cold GET goroutines
	for w := 0; w < coldWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]float64, 0, opsPerWorker)
			for i := 0; i < opsPerWorker; i++ {
				t0 := time.Now()
				rdb.Get(ctx, coldKey)
				local = append(local, time.Since(t0).Seconds())
			}
			mu.Lock()
			coldLats = append(coldLats, local...)
			mu.Unlock()
		}()
	}

	wg.Wait()
	totalSec := time.Since(start).Seconds()

	res := ScenarioResult{
		Name:         name,
		WriteOps:     int64(len(writeLats)),
		ColdOps:      int64(len(coldLats)),
		TotalSec:     totalSec,
		WriteTPS:     float64(len(writeLats)) / totalSec,
		ColdTPS:      float64(len(coldLats)) / totalSec,
		WriteLatency: percentiles(writeLats),
		ColdLatency:  percentiles(coldLats),
	}

	fmt.Printf("\n%s\n%s\n%s\n", cline(60), name, cline(60))
	if writeFn != nil {
		fmt.Printf("[WRITE] tps=%6.0f  p50=%6.2fms  p99=%7.2fms  max=%8.2fms\n",
			res.WriteTPS, res.WriteLatency["p50"], res.WriteLatency["p99"], res.WriteLatency["max"])
	}
	fmt.Printf("[COLD]  tps=%6.0f  p50=%6.2fms  p99=%7.2fms  max=%8.2fms\n",
		res.ColdTPS, res.ColdLatency["p50"], res.ColdLatency["p99"], res.ColdLatency["max"])

	return res
}

func cline(n int) string {
	b := make([]byte, n)
	for i := range b { b[i] = '=' }
	return string(b)
}

func runCollateral(rdb *redis.Client) {
	results := []ScenarioResult{}

	// 1. Baseline: GET only (write 없음)
	r0 := runScenario("baseline_get_only", rdb, nil)
	results = append(results, r0)

	// 2. Control: INCR + GET
	r1 := runScenario("control_incr+get", rdb, func() error {
		return rdb.Incr(ctx, hotKey).Err()
	})
	results = append(results, r1)

	// 3. Variant A: Lua 2개 명령 (GET+INCR) + GET
	r2 := runScenario("variant_lua2cmd+get", rdb, func() error {
		luaCounterScript.Run(context.Background(), rdb, []string{hotKey}, 999999999)
		return nil
	})
	results = append(results, r2)

	// 4. Variant B: Lua 5개 명령 (비즈니스 로직 누적) + GET
	r3 := runScenario("variant_lua5cmd+get", rdb, func() error {
		keys := []string{hotKey, "lua:log", "lua:stat", "lua:hash"}
		luaHeavyScript.Run(context.Background(), rdb, keys, 999999999)
		return nil
	})
	results = append(results, r3)

	// 비교 출력
	base := r0.ColdLatency
	ctrl := r1.ColdLatency
	vrnt := r2.ColdLatency
	hvy  := r3.ColdLatency

	fmt.Printf("\n%s\nCold GET Latency 비교\n%s\n", cline(60), cline(60))
	fmt.Printf("%-20s  %8s  %8s  %8s  %8s\n", "시나리오", "p50", "p99", "max", "tps")
	fmt.Printf("%-20s  %7.2fms  %7.2fms  %7.2fms  %6.0f\n", "baseline", base["p50"], base["p99"], base["max"], r0.ColdTPS)
	fmt.Printf("%-20s  %7.2fms  %7.2fms  %7.2fms  %6.0f\n", "INCR+GET", ctrl["p50"], ctrl["p99"], ctrl["max"], r1.ColdTPS)
	fmt.Printf("%-20s  %7.2fms  %7.2fms  %7.2fms  %6.0f\n", "Lua(2cmd)+GET", vrnt["p50"], vrnt["p99"], vrnt["max"], r2.ColdTPS)
	fmt.Printf("%-20s  %7.2fms  %7.2fms  %7.2fms  %6.0f\n", "Lua(5cmd)+GET", hvy["p50"], hvy["p99"], hvy["max"], r3.ColdTPS)

	fmt.Printf("\nCold p99 배율 (vs baseline): INCR=%.1fx  Lua2=%.1fx  Lua5=%.1fx\n",
		ctrl["p99"]/base["p99"], vrnt["p99"]/base["p99"], hvy["p99"]/base["p99"])
	fmt.Printf("Cold p99 배율 (vs INCR):     Lua2=%.1fx  Lua5=%.1fx\n",
		vrnt["p99"]/ctrl["p99"], hvy["p99"]/ctrl["p99"])

	// JSON 저장
	os.MkdirAll("../results", 0755)
	data, _ := json.MarshalIndent(results, "", "  ")
	_ = os.WriteFile("../results/exp_collateral.json", data, 0644)
	fmt.Println("\n결과 저장: results/exp_collateral.json")
}
