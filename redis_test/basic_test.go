package redis_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yann0917/redis-usage/internal"
	redisops "github.com/yann0917/redis-usage/redis"
)

// 测试环境设置
var (
	testConfig = &internal.RedisConfig{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           15, // 使用数据库 15 进行测试，避免影响其他数据
		PoolSize:     5,
		MinIdleConns: 2,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}

	// 全局 Redis 管理器
	globalManager *redisops.RedisManager
)

// TestMain 管理测试生命周期
func TestMain(m *testing.M) {
	// 设置测试环境
	var err error
	globalManager, err = redisops.NewRedisManager(testConfig)
	if err != nil {
		log.Fatalf("创建 Redis 管理器失败: %v", err)
	}

	// 确保 Redis 连接正常
	ctx := context.Background()
	if err := globalManager.Ping(ctx); err != nil {
		log.Fatalf("Redis 连接测试失败: %v", err)
	}

	// 清空测试数据库
	if err := globalManager.FlushDB(ctx); err != nil {
		log.Fatalf("初始化时清空测试数据库失败: %v", err)
	}

	log.Println("Redis 测试环境初始化完成")

	// 运行所有测试
	code := m.Run()

	// 清理测试环境
	if err := globalManager.FlushDB(ctx); err != nil {
		log.Printf("清理测试数据失败: %v", err)
	}

	if err := globalManager.Close(); err != nil {
		log.Printf("关闭 Redis 连接失败: %v", err)
	}

	log.Println("Redis 测试环境清理完成")

	// 退出测试
	os.Exit(code)
}

// setupTest 为每个测试准备独立的测试环境
func setupTest(t *testing.T, testName string) (context.Context, string) {
	ctx := context.Background()

	// 使用测试名称作为键前缀，确保测试隔离
	keyPrefix := "test:" + testName + ":"

	// 清理可能存在的测试数据（可选，因为我们使用唯一前缀）
	// 这里可以根据需要添加特定的清理逻辑

	return ctx, keyPrefix
}

// 辅助函数：生成测试专用的键名
func testKey(prefix, key string) string {
	return prefix + key
}

// =============================================================================
// 连接管理测试
// =============================================================================

func TestRedisManager_Ping(t *testing.T) {
	ctx, _ := setupTest(t, "ping")

	if err := globalManager.Ping(ctx); err != nil {
		t.Errorf("Ping 测试失败: %v", err)
	}
}

func TestRedisManager_Info(t *testing.T) {
	ctx, _ := setupTest(t, "info")

	info, err := globalManager.Info(ctx)
	if err != nil {
		t.Errorf("获取 Redis 信息失败: %v", err)
	}

	if info["raw"] == "" {
		t.Error("Redis 信息为空")
	}
}

// =============================================================================
// 字符串操作测试
// =============================================================================

func TestRedisManager_Set_Get(t *testing.T) {
	ctx, prefix := setupTest(t, "set_get")

	key := testKey(prefix, "string")
	value := "hello world"

	// 测试设置
	if err := globalManager.Set(ctx, key, value, time.Minute); err != nil {
		t.Errorf("设置字符串失败: %v", err)
	}

	// 测试获取
	result, err := globalManager.Get(ctx, key)
	if err != nil {
		t.Errorf("获取字符串失败: %v", err)
	}

	if result != value {
		t.Errorf("期望值 %s，实际值 %s", value, result)
	}
}

func TestRedisManager_Get_NotExists(t *testing.T) {
	ctx, prefix := setupTest(t, "get_not_exists")

	key := testKey(prefix, "not_exists")

	// 测试获取不存在的键
	_, err := globalManager.Get(ctx, key)
	if err == nil {
		t.Error("期望获取不存在键时返回错误")
	}
}

func TestRedisManager_Incr(t *testing.T) {
	ctx, prefix := setupTest(t, "incr")

	key := testKey(prefix, "counter")

	// 测试自增
	result1, err := globalManager.Incr(ctx, key)
	if err != nil {
		t.Errorf("自增操作失败: %v", err)
	}
	if result1 != 1 {
		t.Errorf("期望值 1，实际值 %d", result1)
	}

	// 再次自增
	result2, err := globalManager.Incr(ctx, key)
	if err != nil {
		t.Errorf("自增操作失败: %v", err)
	}
	if result2 != 2 {
		t.Errorf("期望值 2，实际值 %d", result2)
	}
}

// =============================================================================
// 哈希操作测试
// =============================================================================

func TestRedisManager_HSet_HGet(t *testing.T) {
	ctx, prefix := setupTest(t, "hset_hget")

	key := testKey(prefix, "hash")
	field := "name"
	value := "张三"

	// 测试设置哈希字段
	if err := globalManager.HSet(ctx, key, field, value); err != nil {
		t.Errorf("设置哈希字段失败: %v", err)
	}

	// 测试获取哈希字段
	result, err := globalManager.HGet(ctx, key, field)
	if err != nil {
		t.Errorf("获取哈希字段失败: %v", err)
	}

	if result != value {
		t.Errorf("期望值 %s，实际值 %s", value, result)
	}
}

func TestRedisManager_HMSet_HGetAll(t *testing.T) {
	ctx, prefix := setupTest(t, "hmset_hgetall")

	key := testKey(prefix, "hash_multi")
	fields := map[string]interface{}{
		"name": "李四",
		"age":  "30",
		"city": "上海",
	}

	// 测试批量设置哈希字段
	if err := globalManager.HMSet(ctx, key, fields); err != nil {
		t.Errorf("批量设置哈希字段失败: %v", err)
	}

	// 测试获取所有哈希字段
	result, err := globalManager.HGetAll(ctx, key)
	if err != nil {
		t.Errorf("获取所有哈希字段失败: %v", err)
	}

	for k, v := range fields {
		if result[k] != v {
			t.Errorf("字段 %s 期望值 %v，实际值 %s", k, v, result[k])
		}
	}
}

func TestRedisManager_HDel(t *testing.T) {
	ctx, prefix := setupTest(t, "hdel")

	key := testKey(prefix, "hash_del")

	// 先设置一些字段
	if err := globalManager.HSet(ctx, key, "field1", "value1"); err != nil {
		t.Errorf("设置哈希字段失败: %v", err)
	}
	if err := globalManager.HSet(ctx, key, "field2", "value2"); err != nil {
		t.Errorf("设置哈希字段失败: %v", err)
	}

	// 删除字段
	if err := globalManager.HDel(ctx, key, "field1"); err != nil {
		t.Errorf("删除哈希字段失败: %v", err)
	}

	// 验证字段已删除
	_, err := globalManager.HGet(ctx, key, "field1")
	if err == nil {
		t.Error("期望获取已删除字段时返回错误")
	}

	// 验证其他字段仍存在
	value, err := globalManager.HGet(ctx, key, "field2")
	if err != nil {
		t.Errorf("获取未删除字段失败: %v", err)
	}
	if value != "value2" {
		t.Errorf("期望值 value2，实际值 %s", value)
	}
}

// =============================================================================
// 列表操作测试
// =============================================================================

func TestRedisManager_LPush_LRange(t *testing.T) {
	ctx, prefix := setupTest(t, "lpush_lrange")

	key := testKey(prefix, "list")
	values := []interface{}{"item1", "item2", "item3"}

	// 测试左推入
	if err := globalManager.LPush(ctx, key, values...); err != nil {
		t.Errorf("左推入列表失败: %v", err)
	}

	// 测试获取范围
	result, err := globalManager.LRange(ctx, key, 0, -1)
	if err != nil {
		t.Errorf("获取列表范围失败: %v", err)
	}

	// 验证顺序（左推入的顺序是反序的）
	expected := []string{"item3", "item2", "item1"}
	if len(result) != len(expected) {
		t.Errorf("期望长度 %d，实际长度 %d", len(expected), len(result))
	}

	for i, v := range expected {
		if result[i] != v {
			t.Errorf("索引 %d 期望值 %s，实际值 %s", i, v, result[i])
		}
	}
}

func TestRedisManager_RPush_LPop(t *testing.T) {
	ctx, prefix := setupTest(t, "rpush_lpop")

	key := testKey(prefix, "list_pop")

	// 右推入元素
	if err := globalManager.RPush(ctx, key, "first", "second"); err != nil {
		t.Errorf("右推入列表失败: %v", err)
	}

	// 左弹出元素
	result, err := globalManager.LPop(ctx, key)
	if err != nil {
		t.Errorf("左弹出列表失败: %v", err)
	}

	if result != "first" {
		t.Errorf("期望值 first，实际值 %s", result)
	}

	// 验证列表长度
	length, err := globalManager.LLen(ctx, key)
	if err != nil {
		t.Errorf("获取列表长度失败: %v", err)
	}

	if length != 1 {
		t.Errorf("期望长度 1，实际长度 %d", length)
	}
}

// =============================================================================
// 集合操作测试
// =============================================================================

func TestRedisManager_SAdd_SMembers(t *testing.T) {
	ctx, prefix := setupTest(t, "sadd_smembers")

	key := testKey(prefix, "set")
	members := []interface{}{"member1", "member2", "member3"}

	// 测试添加成员
	if err := globalManager.SAdd(ctx, key, members...); err != nil {
		t.Errorf("添加集合成员失败: %v", err)
	}

	// 测试获取所有成员
	result, err := globalManager.SMembers(ctx, key)
	if err != nil {
		t.Errorf("获取集合成员失败: %v", err)
	}

	if len(result) != len(members) {
		t.Errorf("期望成员数量 %d，实际成员数量 %d", len(members), len(result))
	}

	// 验证成员存在（集合是无序的，需要检查包含关系）
	memberMap := make(map[string]bool)
	for _, member := range result {
		memberMap[member] = true
	}

	for _, member := range members {
		if !memberMap[member.(string)] {
			t.Errorf("成员 %s 未找到", member)
		}
	}
}

func TestRedisManager_SIsMember(t *testing.T) {
	ctx, prefix := setupTest(t, "sismember")

	key := testKey(prefix, "set_member")

	// 添加成员
	if err := globalManager.SAdd(ctx, key, "test_member"); err != nil {
		t.Errorf("添加集合成员失败: %v", err)
	}

	// 测试成员存在
	exists, err := globalManager.SIsMember(ctx, key, "test_member")
	if err != nil {
		t.Errorf("检查集合成员失败: %v", err)
	}
	if !exists {
		t.Error("期望成员存在")
	}

	// 测试成员不存在
	exists, err = globalManager.SIsMember(ctx, key, "not_exists")
	if err != nil {
		t.Errorf("检查集合成员失败: %v", err)
	}
	if exists {
		t.Error("期望成员不存在")
	}
}

// =============================================================================
// 有序集合操作测试
// =============================================================================

func TestRedisManager_ZAdd_ZRange(t *testing.T) {
	ctx, prefix := setupTest(t, "zadd_zrange")

	key := testKey(prefix, "zset")
	members := []redis.Z{
		{Score: 90, Member: "math"},
		{Score: 85, Member: "english"},
		{Score: 95, Member: "chinese"},
	}

	// 测试添加有序集合成员
	if err := globalManager.ZAdd(ctx, key, members...); err != nil {
		t.Errorf("添加有序集合成员失败: %v", err)
	}

	// 测试按排名获取成员（按分数升序）
	result, err := globalManager.ZRange(ctx, key, 0, -1)
	if err != nil {
		t.Errorf("获取有序集合排名范围失败: %v", err)
	}

	// 验证顺序（按分数升序）
	expected := []string{"english", "math", "chinese"}
	if len(result) != len(expected) {
		t.Errorf("期望成员数量 %d，实际成员数量 %d", len(expected), len(result))
	}

	for i, member := range expected {
		if result[i] != member {
			t.Errorf("索引 %d 期望成员 %s，实际成员 %s", i, member, result[i])
		}
	}
}

func TestRedisManager_ZRangeByScore(t *testing.T) {
	ctx, prefix := setupTest(t, "zrangebyscore")

	key := testKey(prefix, "zset_score")

	// 添加成员
	members := []redis.Z{
		{Score: 60, Member: "fail"},
		{Score: 75, Member: "pass"},
		{Score: 90, Member: "good"},
		{Score: 95, Member: "excellent"},
	}

	if err := globalManager.ZAdd(ctx, key, members...); err != nil {
		t.Errorf("添加有序集合成员失败: %v", err)
	}

	// 测试按分数范围获取成员
	result, err := globalManager.ZRangeByScore(ctx, key, "80", "100")
	if err != nil {
		t.Errorf("按分数范围获取有序集合失败: %v", err)
	}

	// 验证结果
	expected := []string{"good", "excellent"}
	if len(result) != len(expected) {
		t.Errorf("期望成员数量 %d，实际成员数量 %d", len(expected), len(result))
	}

	for i, member := range expected {
		if result[i] != member {
			t.Errorf("索引 %d 期望成员 %s，实际成员 %s", i, member, result[i])
		}
	}
}

// =============================================================================
// 键操作测试
// =============================================================================

func TestRedisManager_Del_Exists(t *testing.T) {
	ctx, prefix := setupTest(t, "del_exists")

	key := testKey(prefix, "key_ops")

	// 先设置一个键
	if err := globalManager.Set(ctx, key, "test_value", time.Minute); err != nil {
		t.Errorf("设置键失败: %v", err)
	}

	// 验证键存在
	count, err := globalManager.Exists(ctx, key)
	if err != nil {
		t.Errorf("检查键存在性失败: %v", err)
	}
	if count != 1 {
		t.Errorf("期望键存在，实际存在数量 %d", count)
	}

	// 删除键
	if err := globalManager.Del(ctx, key); err != nil {
		t.Errorf("删除键失败: %v", err)
	}

	// 验证键不存在
	count, err = globalManager.Exists(ctx, key)
	if err != nil {
		t.Errorf("检查键存在性失败: %v", err)
	}
	if count != 0 {
		t.Errorf("期望键不存在，实际存在数量 %d", count)
	}
}

func TestRedisManager_Expire_TTL(t *testing.T) {
	ctx, prefix := setupTest(t, "expire_ttl")

	key := testKey(prefix, "expire")

	// 设置键
	if err := globalManager.Set(ctx, key, "test_value", 0); err != nil {
		t.Errorf("设置键失败: %v", err)
	}

	// 设置过期时间
	expiration := 10 * time.Second
	if err := globalManager.Expire(ctx, key, expiration); err != nil {
		t.Errorf("设置键过期时间失败: %v", err)
	}

	// 检查 TTL
	ttl, err := globalManager.TTL(ctx, key)
	if err != nil {
		t.Errorf("获取键 TTL 失败: %v", err)
	}

	if ttl <= 0 || ttl > expiration {
		t.Errorf("期望 TTL 在 0 到 %v 之间，实际 TTL %v", expiration, ttl)
	}
}

func TestRedisManager_Type(t *testing.T) {
	ctx, prefix := setupTest(t, "type")

	tests := []struct {
		name     string
		setup    func() string
		expected string
	}{
		{
			name: "字符串类型",
			setup: func() string {
				key := testKey(prefix, "type_string")
				globalManager.Set(ctx, key, "value", time.Minute)
				return key
			},
			expected: "string",
		},
		{
			name: "哈希类型",
			setup: func() string {
				key := testKey(prefix, "type_hash")
				globalManager.HSet(ctx, key, "field", "value")
				return key
			},
			expected: "hash",
		},
		{
			name: "列表类型",
			setup: func() string {
				key := testKey(prefix, "type_list")
				globalManager.LPush(ctx, key, "item")
				return key
			},
			expected: "list",
		},
		{
			name: "集合类型",
			setup: func() string {
				key := testKey(prefix, "type_set")
				globalManager.SAdd(ctx, key, "member")
				return key
			},
			expected: "set",
		},
		{
			name: "有序集合类型",
			setup: func() string {
				key := testKey(prefix, "type_zset")
				globalManager.ZAdd(ctx, key, redis.Z{Score: 1, Member: "member"})
				return key
			},
			expected: "zset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := tt.setup()
			keyType, err := globalManager.Type(ctx, key)
			if err != nil {
				t.Errorf("获取键类型失败: %v", err)
			}
			t.Logf("keyType: %s", keyType)

			if keyType != tt.expected {
				t.Errorf("期望类型 %s，实际类型 %s", tt.expected, keyType)
			}
		})
	}
}

// =============================================================================
// 性能基准测试
// =============================================================================

func BenchmarkRedisManager_Set(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench:set:" + string(rune(i))
		if err := globalManager.Set(ctx, key, "benchmark_value", time.Minute); err != nil {
			b.Errorf("设置键失败: %v", err)
		}
	}
}

func BenchmarkRedisManager_Get(b *testing.B) {
	ctx := context.Background()
	key := "bench:get"

	// 预设值
	if err := globalManager.Set(ctx, key, "benchmark_value", time.Minute); err != nil {
		b.Fatalf("预设键失败: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := globalManager.Get(ctx, key); err != nil {
			b.Errorf("获取键失败: %v", err)
		}
	}
}

func BenchmarkRedisManager_HSet(b *testing.B) {
	ctx := context.Background()
	key := "bench:hset"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := "field_" + string(rune(i))
		if err := globalManager.HSet(ctx, key, field, "benchmark_value"); err != nil {
			b.Errorf("设置哈希字段失败: %v", err)
		}
	}
}
