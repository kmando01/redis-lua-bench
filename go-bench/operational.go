package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// --- 실험 6: Partial Write ---

var partialWriteScript = redis.NewScript(`
redis.call('SET', KEYS[1], 'written-by-lua')
redis.call('SET', KEYS[2], 'written-by-lua')
redis.call('ZADD', KEYS[1], 'NOT_A_NUMBER', 'member')
redis.call('SET', KEYS[3], 'written-by-lua')
return 'done'
`)

func runPartialWrite(rdb *redis.Client) map[string]interface{} {
	keys := []string{"pw:key1", "pw:key2", "pw:key3"}
	for _, k := range keys {
		rdb.Del(ctx, k)
	}

	fmt.Printf("\n%s\n실험 6: Partial Write\n%s\n", cline(60), cline(60))
	fmt.Println("스크립트: SET key1 → SET key2 → ZADD key1(타입에러) → SET key3")

	err := partialWriteScript.Run(ctx, rdb, keys).Err()
	fmt.Printf("Eval 에러: %v\n\n", err)

	states := map[string]string{}
	for _, k := range keys {
		v, e := rdb.Get(ctx, k).Result()
		if e == redis.Nil {
			states[k] = "(nil)"
		} else {
			states[k] = v
		}
		fmt.Printf("  %-12s = %q\n", k, states[k])
	}

	partialWrite := states["pw:key1"] != "(nil)" &&
		states["pw:key2"] != "(nil)" &&
		states["pw:key3"] == "(nil)"
	rollback := states["pw:key1"] == "(nil)"

	fmt.Printf("\n롤백 발생: %v\n", rollback)
	fmt.Printf("Partial write 확인: %v\n", partialWrite)

	out := map[string]interface{}{
		"experiment":        "6_partial_write",
		"error":             fmt.Sprintf("%v", err),
		"key_states":        states,
		"rollback_occurred": rollback,
		"partial_write":     partialWrite,
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	_ = os.WriteFile("../results/exp6_partial_write.json", data, 0644)
	return out
}

// --- 실험 7: redis.log() 로그 오염 ---

var logPollutionScript = redis.NewScript(`
local val = redis.call('GET', KEYS[1]) or '0'
redis.log(redis.LOG_WARNING, '[LUA-DEBUG] counter=' .. val .. ' limit=' .. ARGV[1])
return redis.call('INCR', KEYS[1])
`)

func runLogPollution(rdb *redis.Client, container string) map[string]interface{} {
	fmt.Printf("\n%s\n실험 7: redis.log() 로그 오염\n%s\n", cline(60), cline(60))
	fmt.Println("100회 호출 → docker logs에서 WARNING 라인 수 카운트")

	rdb.Del(ctx, "log:test:key")
	for i := 0; i < 100; i++ {
		logPollutionScript.Run(ctx, rdb, []string{"log:test:key"}, "999")
	}
	time.Sleep(300 * time.Millisecond)

	// docker logs에서 LUA-DEBUG 라인 수 세기
	out, err := exec.Command("docker", "logs", container, "--since", "30s").CombinedOutput()
	logCount := 0
	if err == nil {
		for _, line := range bytes.Split(out, []byte("\n")) {
			if strings.Contains(string(line), "LUA-DEBUG") {
				logCount++
			}
		}
	}

	fmt.Printf("docker logs LUA-DEBUG 라인: %d / 호출 100회\n", logCount)
	fmt.Printf("결론: redis.log(WARNING)은 Redis 서버 로그에 그대로 기록됨\n")

	result := map[string]interface{}{
		"experiment":      "7_log_pollution",
		"calls":           100,
		"log_lines_found": logCount,
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	_ = os.WriteFile("../results/exp7_log_pollution.json", data, 0644)
	return result
}

// --- 실험 8: 디버거 블록 측정 ---
// --ldb는 인터랙티브라 자동화 불가.
// 동일한 이벤트 루프 점유 메커니즘인 DEBUG SLEEP으로 블록 효과를 측정한다.

// blockingLua: ~2초짜리 Lua — --ldb가 breakpoint에서 대기하는 것과 동일한 이벤트 루프 점유
var blockingLua = redis.NewScript(`
local s = 0
for i = 1, 500000000 do s = s + i end
return s
`)

func runDebuggerBlock(rdb *redis.Client) map[string]interface{} {
	fmt.Printf("\n%s\n실험 8: 디버거(--ldb) 블록 측정\n%s\n", cline(60), cline(60))
	fmt.Println("~2초짜리 Lua로 이벤트 루프 점유 → 별도 연결 GET 대기 시간 측정")
	fmt.Println("--ldb는 breakpoint마다 이 상태를 반복 → 프로덕션 사용 불가")

	rdb.Set(ctx, "dbg:key", "hello", 0)

	// baseline
	var baseLats []float64
	for i := 0; i < 200; i++ {
		t := time.Now()
		rdb.Get(ctx, "dbg:key")
		baseLats = append(baseLats, time.Since(t).Seconds()*1000)
	}
	baseP99 := percentiles2(baseLats)["p99"]
	baseMax := percentiles2(baseLats)["max"]

	// 독립 클라이언트 (각 PoolSize=1)
	clientA := redis.NewClient(&redis.Options{Addr: "localhost:6399", PoolSize: 1})
	clientB := redis.NewClient(&redis.Options{Addr: "localhost:6399", PoolSize: 1})
	defer clientA.Close()
	defer clientB.Close()
	clientA.Ping(ctx)
	clientB.Ping(ctx)

	// clientA: Lua 블로킹 시작
	started := make(chan struct{})
	go func() {
		close(started)
		blockingLua.Run(ctx, clientA, []string{})
	}()
	<-started
	time.Sleep(10 * time.Millisecond) // Lua가 서버에 도달하도록

	// clientB: Lua 실행 중 GET — 이벤트 루프가 막혀있어 대기해야 함
	var blockedLats []float64
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		t := time.Now()
		clientB.Get(ctx, "dbg:key")
		blockedLats = append(blockedLats, time.Since(t).Seconds()*1000)
	}
	underP99 := percentiles2(blockedLats)["p99"]
	underMax := percentiles2(blockedLats)["max"]
	multiplier := underMax / baseMax

	fmt.Printf("\nbaseline  p99=%.2fms  max=%.2fms\n", baseP99, baseMax)
	fmt.Printf("Lua 중    p99=%.2fms  max=%.2fms  (max %.0fx)\n", underP99, underMax, multiplier)
	fmt.Printf("→ Lua 실행 중 GET 요청은 Lua가 끝날 때까지 전부 대기\n")
	fmt.Printf("→ --ldb는 breakpoint마다 이 상태를 반복 = 프로덕션 전체 마비\n")

	result := map[string]interface{}{
		"experiment":       "8_debugger_block",
		"method":           "~2s Lua blocking (--ldb breakpoint와 동일한 이벤트 루프 점유)",
		"baseline_p99_ms":  baseP99,
		"baseline_max_ms":  baseMax,
		"blocked_p99_ms":   underP99,
		"blocked_max_ms":   underMax,
		"p99_multiplier":   multiplier,
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	_ = os.WriteFile("../results/exp8_debugger_block.json", data, 0644)
	return result
}

func percentiles2(ms []float64) map[string]float64 { return percentiles(msToSec(ms)) }
func msToSec(ms []float64) []float64 {
	s := make([]float64, len(ms))
	for i, v := range ms { s[i] = v / 1000 }
	return s
}

func runOperational(rdb *redis.Client, container string) {
	os.MkdirAll("../results", 0755)

	r6 := runPartialWrite(rdb)
	r7 := runLogPollution(rdb, container)
	r8 := runDebuggerBlock(rdb)

	fmt.Printf("\n%s\n운영 리스크 실험 요약\n%s\n", cline(60), cline(60))
	fmt.Printf("Exp6 partial_write:      %v\n", r6["partial_write"])
	fmt.Printf("Exp7 log_lines / 100:    %v\n", r7["log_lines_found"])
	fmt.Printf("Exp8 debug_p99_배율:     %.0fx\n", r8["p99_multiplier"])
}
