package internal

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestDefaultRedisConfig 测试默认配置生成
func TestDefaultRedisConfig(t *testing.T) {
	config := DefaultRedisConfig()

	// 验证默认配置的各个字段
	if config.Addr != "localhost:6379" {
		t.Errorf("期望地址为 localhost:6379，实际为 %s", config.Addr)
	}

	if config.Password != "" {
		t.Errorf("期望密码为空，实际为 %s", config.Password)
	}

	if config.DB != 0 {
		t.Errorf("期望 DB 为 0，实际为 %d", config.DB)
	}

	if config.PoolSize != 10 {
		t.Errorf("期望连接池大小为 10，实际为 %d", config.PoolSize)
	}

	if config.MinIdleConns != 5 {
		t.Errorf("期望最小空闲连接为 5，实际为 %d", config.MinIdleConns)
	}
}

// TestNewRedisClient 测试基础 Redis 客户端创建
func TestNewRedisClient(t *testing.T) {
	// 测试正常创建客户端
	rdb := NewRedisClient("localhost:6379", "", 0)
	if rdb == nil {
		t.Fatal("创建 Redis 客户端失败，返回 nil")
	}

	// 确保清理资源
	defer func() {
		if err := CloseRedis(rdb); err != nil {
			t.Logf("关闭 Redis 客户端时出错: %v", err)
		}
	}()

	// 可以进一步测试连接是否正常（需要运行 Redis 服务器）
	// 这里只是简单验证客户端对象不为空
}

// TestNewRedisClientWithConfig 测试使用配置创建客户端
func TestNewRedisClientWithConfig(t *testing.T) {
	// 测试使用自定义配置
	config := &RedisConfig{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           1,
		PoolSize:     20,
		MinIdleConns: 10,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	rdb := NewRedisClientWithConfig(config)
	if rdb == nil {
		t.Fatal("使用配置创建 Redis 客户端失败，返回 nil")
	}

	defer CloseRedis(rdb)

	// 测试传入 nil 配置，应该使用默认配置
	rdbDefault := NewRedisClientWithConfig(nil)
	if rdbDefault == nil {
		t.Fatal("使用 nil 配置创建 Redis 客户端失败，返回 nil")
	}

	defer CloseRedis(rdbDefault)
}

// TestCloseRedis 测试关闭 Redis 客户端
func TestCloseRedis(t *testing.T) {
	// 测试关闭正常的客户端
	rdb := NewRedisClient("localhost:6379", "", 0)
	err := CloseRedis(rdb)
	if err != nil {
		t.Errorf("关闭 Redis 客户端失败: %v", err)
	}

	// 测试关闭 nil 客户端（应该不报错）
	err = CloseRedis(nil)
	if err != nil {
		t.Errorf("关闭 nil 客户端应该不报错，但收到错误: %v", err)
	}
}

// TestPingRedis 测试 Redis 连接检测
// 注意：此测试需要运行中的 Redis 服务器
func TestPingRedis(t *testing.T) {
	rdb := NewRedisClient("localhost:6379", "", 0)
	defer CloseRedis(rdb)

	ctx := context.Background()

	// 测试正常的 ping（如果没有 Redis 服务器，这个测试会失败）
	err := PingRedis(ctx, rdb)
	if err != nil {
		t.Logf("Ping Redis 失败（可能没有运行 Redis 服务器）: %v", err)
		// 在没有 Redis 服务器的环境中，我们只记录日志而不让测试失败
		return
	}

	t.Log("Ping Redis 成功")
}

// TestPingRedisWithTimeout 测试带超时的 Redis 连接检测
func TestPingRedisWithTimeout(t *testing.T) {
	rdb := NewRedisClient("localhost:6379", "", 0)
	defer CloseRedis(rdb)

	// 创建一个很短的超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// 这个测试可能会因为超时而失败，这是期望的行为
	err := PingRedis(ctx, rdb)
	if err != nil {
		t.Logf("带超时的 Ping 测试失败（这是预期的）: %v", err)
	}
}

// TestGetRedisInfo 测试获取 Redis 信息
// 注意：此测试需要运行中的 Redis 服务器
func TestGetRedisInfo(t *testing.T) {
	rdb := NewRedisClient("localhost:6379", "", 0)
	defer CloseRedis(rdb)

	ctx := context.Background()

	info, err := GetRedisInfo(ctx, rdb)
	if err != nil {
		t.Logf("获取 Redis 信息失败（可能没有运行 Redis 服务器）: %v", err)
		// 在没有 Redis 服务器的环境中，我们只记录日志而不让测试失败
		return
	}

	// 验证返回的信息包含 raw 字段
	if _, exists := info["raw"]; !exists {
		t.Error("Redis 信息中缺少 raw 字段")
	}

	t.Logf("成功获取 Redis 信息，包含 %d 个字段", len(info))
}

// TestFlushDB 测试清空数据库
// 注意：此测试需要运行中的 Redis 服务器，且会清空数据库！
func TestFlushDB(t *testing.T) {
	rdb := NewRedisClient("localhost:6379", "", 15) // 使用 DB 15 进行测试
	defer CloseRedis(rdb)

	ctx := context.Background()

	// 先设置一个测试键值
	err := rdb.Set(ctx, "test_key", "test_value", 0).Err()
	if err != nil {
		t.Logf("设置测试键值失败（可能没有运行 Redis 服务器）: %v", err)
		return
	}

	// 清空数据库
	err = FlushDB(ctx, rdb)
	if err != nil {
		t.Logf("清空数据库失败: %v", err)
		return
	}

	// 验证键值是否被清空
	val, err := rdb.Get(ctx, "test_key").Result()
	if err != redis.Nil {
		t.Errorf("期望键不存在，但找到值: %s", val)
	}

	t.Log("FlushDB 测试成功")
}

// BenchmarkNewRedisClient 性能测试：创建 Redis 客户端
func BenchmarkNewRedisClient(b *testing.B) {
	for i := 0; i < b.N; i++ {
		rdb := NewRedisClient("localhost:6379", "", 0)
		CloseRedis(rdb)
	}
}

// BenchmarkNewRedisClientWithConfig 性能测试：使用配置创建客户端
func BenchmarkNewRedisClientWithConfig(b *testing.B) {
	config := DefaultRedisConfig()

	for i := 0; i < b.N; i++ {
		rdb := NewRedisClientWithConfig(config)
		CloseRedis(rdb)
	}
}
