package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig Redis 连接配置结构体
type RedisConfig struct {
	Addr         string        // Redis 服务器地址，格式：host:port
	Password     string        // Redis 密码，为空表示无密码
	DB           int           // 数据库编号，默认为 0
	PoolSize     int           // 连接池大小，默认为 10
	MinIdleConns int           // 最小空闲连接数，默认为 5
	DialTimeout  time.Duration // 连接超时时间，默认为 5 秒
	ReadTimeout  time.Duration // 读取超时时间，默认为 3 秒
	WriteTimeout time.Duration // 写入超时时间，默认为 3 秒
}

// DefaultRedisConfig 返回默认的 Redis 配置
func DefaultRedisConfig() *RedisConfig {
	return &RedisConfig{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		PoolSize:     10,
		MinIdleConns: 5,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

// RedisOperator Redis 操作接口，支持依赖注入和测试 mock
type RedisOperator interface {
	// 连接管理
	Ping(ctx context.Context) error
	Close() error
	Info(ctx context.Context) (map[string]string, error)
	FlushDB(ctx context.Context) error

	// 字符串操作
	Set(ctx context.Context, key, value string, expiration time.Duration) error
	SetNX(ctx context.Context, key, value string, expiration time.Duration) (bool, error)
	SetXX(ctx context.Context, key, value string, expiration time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, error)
	Incr(ctx context.Context, key string) (int64, error)

	// 哈希操作
	HSet(ctx context.Context, key, field, value string) error
	HGet(ctx context.Context, key, field string) (string, error)
	HMSet(ctx context.Context, key string, fields map[string]interface{}) error
	HMGet(ctx context.Context, key string, fields ...string) ([]interface{}, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HDel(ctx context.Context, key string, fields ...string) error

	// 列表操作
	LPush(ctx context.Context, key string, values ...interface{}) error
	RPush(ctx context.Context, key string, values ...interface{}) error
	LPop(ctx context.Context, key string) (string, error)
	RPop(ctx context.Context, key string) (string, error)
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	LLen(ctx context.Context, key string) (int64, error)

	// 集合操作
	SAdd(ctx context.Context, key string, members ...interface{}) error
	SRem(ctx context.Context, key string, members ...interface{}) error
	SIsMember(ctx context.Context, key string, member interface{}) (bool, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	SCard(ctx context.Context, key string) (int64, error)

	// 有序集合操作
	ZAdd(ctx context.Context, key string, members ...redis.Z) error
	ZRem(ctx context.Context, key string, members ...interface{}) error
	ZRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	ZRangeByScore(ctx context.Context, key string, min, max string) ([]string, error)
	ZCard(ctx context.Context, key string) (int64, error)

	// 键操作
	Del(ctx context.Context, keys ...string) error
	Exists(ctx context.Context, keys ...string) (int64, error)
	Expire(ctx context.Context, key string, expiration time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)
	Type(ctx context.Context, key string) (string, error)
}

// =============================================================================
// 向后兼容的函数式 API（保留原有函数）
// =============================================================================

// NewRedisClient 创建新的 Redis 客户端实例（向后兼容）
func NewRedisClient(addr, password string, db int) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     10,
		MinIdleConns: 5,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	return rdb
}

// NewRedisClientWithConfig 使用配置结构体创建 Redis 客户端（向后兼容）
func NewRedisClientWithConfig(config *RedisConfig) *redis.Client {
	if config == nil {
		config = DefaultRedisConfig()
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})
	return rdb
}

// PingRedis 测试 Redis 连接是否正常（向后兼容）
func PingRedis(ctx context.Context, rdb *redis.Client) error {
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("Redis 连接失败: %w", err)
	}
	return nil
}

// CloseRedis 安全关闭 Redis 客户端连接（向后兼容）
func CloseRedis(rdb *redis.Client) error {
	if rdb == nil {
		return nil
	}
	return rdb.Close()
}

// GetRedisInfo 获取 Redis 服务器信息（向后兼容）
func GetRedisInfo(ctx context.Context, rdb *redis.Client) (map[string]string, error) {
	info, err := rdb.Info(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("获取 Redis 信息失败: %w", err)
	}

	infoMap := make(map[string]string)
	infoMap["raw"] = info
	return infoMap, nil
}

// FlushDB 清空当前数据库的所有数据（向后兼容）
func FlushDB(ctx context.Context, rdb *redis.Client) error {
	err := rdb.FlushDB(ctx).Err()
	if err != nil {
		return fmt.Errorf("清空数据库失败: %w", err)
	}
	return nil
}
