package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// --- Latency 수집 ---

func percentiles(lats []float64) map[string]float64 {
	if len(lats) == 0 {
		return nil
	}
	s := make([]float64, len(lats))
	copy(s, lats)
	sort.Float64s(s)
	n := len(s)
	pct := func(p float64) float64 {
		i := int(math.Ceil(float64(n)*p)) - 1
		if i < 0 { i = 0 }
		if i >= n { i = n - 1 }
		return s[i] * 1000
	}
	sum := 0.0
	for _, v := range s { sum += v }
	return map[string]float64{
		"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99),
		"max": s[n-1] * 1000, "min": s[0] * 1000,
		"mean": sum / float64(n) * 1000,
	}
}

// --- 5가지 카운터 구현 ---

const luaScript = `
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current >= tonumber(ARGV[1]) then return -1 end
return redis.call('INCR', KEYS[1])
`

// 1. Lua Script
func luaCounter(rdb *redis.Client, key string, limit int64) int64 {
	script := redis.NewScript(luaScript)
	r, err := script.Run(ctx, rdb, []string{key}, limit).Int64()
	if err != nil { return -1 }
	return r
}

// 2. INCR + 조건부 DECR 롤백
func incrDecrCounter(rdb *redis.Client, key string, limit int64) int64 {
	v, err := rdb.Incr(ctx, key).Result()
	if err != nil { return -1 }
	if v > limit {
		rdb.Decr(ctx, key)
		return -1
	}
	return v
}

// 3. WATCH / MULTI / EXEC (낙관적 락)
func watchCounter(rdb *redis.Client, key string, limit int64) (int64, int) {
	retries := 0
	for retries < 10 {
		var result int64 = -1
		err := rdb.Watch(ctx, func(tx *redis.Tx) error {
			cur, err := tx.Get(ctx, key).Int64()
			if err != nil && err != redis.Nil { return err }
			if cur >= limit { result = -1; return nil }
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Incr(ctx, key)
				return nil
			})
			if err == nil { result = cur + 1 }
			return err
		}, key)
		if err == nil { return result, retries }
		if err == redis.TxFailedErr { retries++; continue }
		return -1, retries
	}
	return -1, retries
}

// 4. 분산 락 (SET NX) + INCR
func lockCounter(rdb *redis.Client, key string, limit int64) int64 {
	lockKey := "lock:" + key
	for i := 0; i < 50; i++ {
		ok, err := rdb.SetNX(ctx, lockKey, "1", 5*time.Second).Result()
		if err != nil { return -1 }
		if ok {
			defer rdb.Del(ctx, lockKey)
			cur, _ := rdb.Get(ctx, key).Int64()
			if cur >= limit { return -1 }
			return rdb.Incr(ctx, key).Val()
		}
		time.Sleep(2 * time.Millisecond)
	}
	return -1
}

// 5. Redis Functions (FCALL)
func functionCounter(rdb *redis.Client, key string, limit int64) int64 {
	r, err := rdb.Do(ctx, "FCALL", "limit_incr", 1, key, limit).Int64()
	if err != nil { return -1 }
	return r
}

// --- 벤치마크 러너 ---

type Result struct {
	Name      string             `json:"name"`
	Workers   int                `json:"workers"`
	TotalOps  int                `json:"total_ops"`
	Limit     int64              `json:"limit"`
	Success   int64              `json:"success"`
	Fail      int64              `json:"fail"`
	FinalVal  int64              `json:"final_value"`
	Accurate  bool               `json:"accurate"`
	TPS       float64            `json:"tps"`
	Latency   map[string]float64 `json:"latency_ms"`
	AvgRetry  float64            `json:"avg_retry,omitempty"`
	ElapsedMs float64            `json:"elapsed_ms"`
}

func runBench(name string, workers, totalOps int, limit int64,
	rdb *redis.Client, keyPrefix string,
	fn func(key string) (int64, int)) Result {

	key := fmt.Sprintf("%s:%s:%d", keyPrefix, name, time.Now().UnixNano())
	rdb.Del(ctx, key)

	var (
		mu      sync.Mutex
		lats    []float64
		success int64
		fail    int64
		retries int64
		wg      sync.WaitGroup
	)

	perWorker := totalOps / workers
	start := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]float64, 0, perWorker)
			var ls, lf, lr int64
			for i := 0; i < perWorker; i++ {
				t0 := time.Now()
				v, retry := fn(key)
				local = append(local, time.Since(t0).Seconds())
				if v > 0 { ls++ } else { lf++ }
				lr += int64(retry)
			}
			mu.Lock()
			lats = append(lats, local...)
			atomic.AddInt64(&success, ls)
			atomic.AddInt64(&fail, lf)
			atomic.AddInt64(&retries, lr)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	finalVal, _ := rdb.Get(ctx, key).Int64()
	completed := int64(len(lats))
	avgRetry := 0.0
	if completed > 0 { avgRetry = float64(retries) / float64(completed) }

	res := Result{
		Name: name, Workers: workers, TotalOps: totalOps, Limit: limit,
		Success: success, Fail: fail, FinalVal: finalVal,
		Accurate:  finalVal == limit,
		TPS:       float64(completed) / elapsed,
		Latency:   percentiles(lats),
		AvgRetry:  avgRetry,
		ElapsedMs: elapsed * 1000,
	}

	fmt.Printf("\n=== %s ===\n", name)
	fmt.Printf("tps=%.0f  success=%d  fail=%d  final=%d  accurate=%v\n",
		res.TPS, success, fail, finalVal, res.Accurate)
	fmt.Printf("p50=%.2fms  p95=%.2fms  p99=%.2fms  max=%.2fms\n",
		res.Latency["p50"], res.Latency["p95"], res.Latency["p99"], res.Latency["max"])
	if avgRetry > 0 {
		fmt.Printf("avg_retry=%.2f\n", avgRetry)
	}
	return res
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go-bench <exp2|exp2-load-functions>")
		os.Exit(1)
	}

	port := "6399"
	if len(os.Args) > 2 { port = os.Args[2] }

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:" + port,
		PoolSize: 600,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Println("Redis 연결 실패:", err)
		os.Exit(1)
	}

	container := "redis-lua-bench-redis-1"
	if len(os.Args) > 2 { container = os.Args[2] }

	switch os.Args[1] {
	case "exp2":
		runExp2(rdb)
	case "load-functions":
		loadFunctions(rdb)
	case "collateral":
		runCollateral(rdb)
	case "operational":
		runOperational(rdb, container)
	}
}

func loadFunctions(rdb *redis.Client) {
	script, _ := os.ReadFile("../scripts/counterlib.lua")
	res, err := rdb.Do(ctx, "FUNCTION", "LOAD", "REPLACE", string(script)).Result()
	fmt.Println("FUNCTION LOAD:", res, err)
}

func runExp2(rdb *redis.Client) {
	const (
		workers  = 1000
		totalOps = 10000
		limit    = int64(100)
	)

	results := []Result{}

	// 1. Lua Script
	results = append(results, runBench("1_lua_script", workers, totalOps, limit, rdb, "bench",
		func(key string) (int64, int) { return luaCounter(rdb, key, limit), 0 }))

	// 2. INCR + DECR rollback
	results = append(results, runBench("2_incr_decr", workers, totalOps, limit, rdb, "bench",
		func(key string) (int64, int) { return incrDecrCounter(rdb, key, limit), 0 }))

	// 3. WATCH/MULTI/EXEC
	results = append(results, runBench("3_watch_multi", workers, totalOps, limit, rdb, "bench",
		func(key string) (int64, int) { return watchCounter(rdb, key, limit) }))

	// 4. 분산 락
	results = append(results, runBench("4_lock", workers, totalOps, limit, rdb, "bench",
		func(key string) (int64, int) { return lockCounter(rdb, key, limit), 0 }))

	// 5. Redis Functions
	results = append(results, runBench("5_functions", workers, totalOps, limit, rdb, "bench",
		func(key string) (int64, int) { return functionCounter(rdb, key, limit), 0 }))

	// JSON 저장
	os.MkdirAll("../results", 0755)
	data, _ := json.MarshalIndent(results, "", "  ")
	_ = os.WriteFile("../results/exp2.json", data, 0644)

	// 정확성 요약
	fmt.Println("\n--- 정확성 요약 ---")
	for _, r := range results {
		fmt.Printf("%-20s  final=%d  accurate=%v\n", r.Name, r.FinalVal, r.Accurate)
	}

	// TPS 랭킹
	fmt.Println("\n--- TPS 랭킹 ---")
	sorted := make([]Result, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].TPS > sorted[j].TPS })
	for i, r := range sorted {
		fmt.Printf("%d. %-20s  %.0f ops/s\n", i+1, r.Name, r.TPS)
	}

	// workers 숫자 파싱용 임시 변수
	_ = strconv.Itoa(workers)
}
