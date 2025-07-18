package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yann0917/redis-usage/internal"
)

// RedisManager Redis 管理器，封装 Redis 操作
type RedisManager struct {
	client *redis.Client
	config *internal.RedisConfig
}

// NewRedisManager 创建新的 Redis 管理器实例
// 参数：
//   - config: Redis 配置，为 nil 时使用默认配置
//
// 返回：
//   - *RedisManager: Redis 管理器实例
//   - error: 创建失败时返回错误
func NewRedisManager(config *internal.RedisConfig) (*RedisManager, error) {
	if config == nil {
		config = internal.DefaultRedisConfig()
	}

	client := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})

	manager := &RedisManager{
		client: client,
		config: config,
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := manager.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("Redis 连接测试失败: %w", err)
	}

	return manager, nil
}

// NewRedisManagerWithClient 使用现有的 Redis 客户端创建管理器（用于测试或特殊场景）
// 参数：
//   - client: 现有的 Redis 客户端
//
// 返回：
//   - *RedisManager: Redis 管理器实例
func NewRedisManagerWithClient(client *redis.Client) *RedisManager {
	return &RedisManager{
		client: client,
		config: nil, // 外部客户端不管理配置
	}
}

// GetClient 获取底层的 Redis 客户端（用于高级操作）
func (r *RedisManager) GetClient() *redis.Client {
	return r.client
}

// GetConfig 获取 Redis 配置信息
func (r *RedisManager) GetConfig() *internal.RedisConfig {
	return r.config
}

// =============================================================================
// 连接管理方法
// =============================================================================

// Ping 测试 Redis 连接是否正常
func (r *RedisManager) Ping(ctx context.Context) error {
	_, err := r.client.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("Redis 连接失败: %w", err)
	}
	return nil
}

// Close 安全关闭 Redis 客户端连接
func (r *RedisManager) Close() error {
	if r.client == nil {
		return nil
	}
	return r.client.Close()
}

// Info 获取 Redis 服务器信息
func (r *RedisManager) Info(ctx context.Context) (map[string]string, error) {
	info, err := r.client.Info(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("获取 Redis 信息失败: %w", err)
	}

	// 简单解析 INFO 命令的输出
	infoMap := make(map[string]string)
	infoMap["raw"] = info
	return infoMap, nil
}

// FlushDB 清空当前数据库的所有数据（谨慎使用！）
func (r *RedisManager) FlushDB(ctx context.Context) error {
	err := r.client.FlushDB(ctx).Err()
	if err != nil {
		return fmt.Errorf("清空数据库失败: %w", err)
	}
	return nil
}

// =============================================================================
// 字符串操作方法
// =============================================================================

// Set 设置字符串键值对，支持过期时间
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 键名
//   - value: 值
//   - expiration: 过期时间，0 表示永不过期
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) Set(ctx context.Context, key, value string, expiration time.Duration) error {
	err := r.client.Set(ctx, key, value, expiration).Err()
	if err != nil {
		return fmt.Errorf("设置键 %s 失败: %w", key, err)
	}
	return nil
}

// SetNX 仅在键不存在时设置字符串键值对（原子操作）
// 常用于分布式锁、防重复提交等场景
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 键名
//   - value: 值
//   - expiration: 过期时间，0 表示永不过期
//
// 返回：
//   - bool: 是否设置成功，true 表示键不存在且设置成功，false 表示键已存在
//   - error: 操作失败时返回错误
func (r *RedisManager) SetNX(ctx context.Context, key, value string, expiration time.Duration) (bool, error) {
	success, err := r.client.SetNX(ctx, key, value, expiration).Result()
	if err != nil {
		return false, fmt.Errorf("SetNX 键 %s 失败: %w", key, err)
	}
	return success, nil
}

// SetXX 仅在键存在时设置字符串键值对（原子操作）
// 与 SetNX 相对，用于更新已存在的键
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 键名
//   - value: 值
//   - expiration: 过期时间，0 表示永不过期
//
// 返回：
//   - bool: 是否设置成功，true 表示键存在且设置成功，false 表示键不存在
//   - error: 操作失败时返回错误
func (r *RedisManager) SetXX(ctx context.Context, key, value string, expiration time.Duration) (bool, error) {
	success, err := r.client.SetXX(ctx, key, value, expiration).Result()
	if err != nil {
		return false, fmt.Errorf("SetXX 键 %s 失败: %w", key, err)
	}
	return success, nil
}

// Get 获取字符串值
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 键名
//
// 返回：
//   - string: 键对应的值
//   - error: 操作失败时返回错误，键不存在时返回 redis.Nil
func (r *RedisManager) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("键 %s 不存在", key)
		}
		return "", fmt.Errorf("获取键 %s 失败: %w", key, err)
	}
	return val, nil
}

// Incr 原子性地增加键的整数值
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 键名
//
// 返回：
//   - int64: 增加后的值
//   - error: 操作失败时返回错误
func (r *RedisManager) Incr(ctx context.Context, key string) (int64, error) {
	val, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("增加键 %s 失败: %w", key, err)
	}
	return val, nil
}

// =============================================================================
// 哈希操作方法
// =============================================================================

// HSet 设置哈希字段的值
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 哈希键名
//   - field: 字段名
//   - value: 字段值
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) HSet(ctx context.Context, key, field, value string) error {
	err := r.client.HSet(ctx, key, field, value).Err()
	if err != nil {
		return fmt.Errorf("设置哈希 %s 字段 %s 失败: %w", key, field, err)
	}
	return nil
}

// HGet 获取哈希字段的值
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 哈希键名
//   - field: 字段名
//
// 返回：
//   - string: 字段对应的值
//   - error: 操作失败时返回错误
func (r *RedisManager) HGet(ctx context.Context, key, field string) (string, error) {
	val, err := r.client.HGet(ctx, key, field).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("哈希 %s 字段 %s 不存在", key, field)
		}
		return "", fmt.Errorf("获取哈希 %s 字段 %s 失败: %w", key, field, err)
	}
	return val, nil
}

// HMSet 批量设置哈希字段
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 哈希键名
//   - fields: 字段映射
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) HMSet(ctx context.Context, key string, fields map[string]interface{}) error {
	err := r.client.HMSet(ctx, key, fields).Err()
	if err != nil {
		return fmt.Errorf("批量设置哈希 %s 失败: %w", key, err)
	}
	return nil
}

// HMGet 批量获取哈希字段的值
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 哈希键名
//   - fields: 要获取的字段名列表
//
// 返回：
//   - []interface{}: 字段值列表，顺序与输入字段顺序一致
//   - error: 操作失败时返回错误
func (r *RedisManager) HMGet(ctx context.Context, key string, fields ...string) ([]interface{}, error) {
	vals, err := r.client.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return nil, fmt.Errorf("批量获取哈希 %s 字段失败: %w", key, err)
	}
	return vals, nil
}

// HGetAll 获取哈希的所有字段和值
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 哈希键名
//
// 返回：
//   - map[string]string: 包含所有字段和值的映射
//   - error: 操作失败时返回错误
func (r *RedisManager) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	result, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("获取哈希 %s 所有字段失败: %w", key, err)
	}
	return result, nil
}

// HDel 删除哈希字段
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 哈希键名
//   - fields: 要删除的字段名列表
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) HDel(ctx context.Context, key string, fields ...string) error {
	err := r.client.HDel(ctx, key, fields...).Err()
	if err != nil {
		return fmt.Errorf("删除哈希 %s 字段失败: %w", key, err)
	}
	return nil
}

// =============================================================================
// 列表操作方法
// =============================================================================

// LPush 从列表左端推入元素
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 列表键名
//   - values: 要推入的值列表
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) LPush(ctx context.Context, key string, values ...interface{}) error {
	err := r.client.LPush(ctx, key, values...).Err()
	if err != nil {
		return fmt.Errorf("左推入列表 %s 失败: %w", key, err)
	}
	return nil
}

// RPush 从列表右端推入元素
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 列表键名
//   - values: 要推入的值列表
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) RPush(ctx context.Context, key string, values ...interface{}) error {
	err := r.client.RPush(ctx, key, values...).Err()
	if err != nil {
		return fmt.Errorf("右推入列表 %s 失败: %w", key, err)
	}
	return nil
}

// LPop 从列表左端弹出元素
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 列表键名
//
// 返回：
//   - string: 弹出的元素值
//   - error: 操作失败时返回错误
func (r *RedisManager) LPop(ctx context.Context, key string) (string, error) {
	val, err := r.client.LPop(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("列表 %s 为空", key)
		}
		return "", fmt.Errorf("左弹出列表 %s 失败: %w", key, err)
	}
	return val, nil
}

// RPop 从列表右端弹出元素
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 列表键名
//
// 返回：
//   - string: 弹出的元素值
//   - error: 操作失败时返回错误
func (r *RedisManager) RPop(ctx context.Context, key string) (string, error) {
	val, err := r.client.RPop(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("列表 %s 为空", key)
		}
		return "", fmt.Errorf("右弹出列表 %s 失败: %w", key, err)
	}
	return val, nil
}

// LRange 获取列表指定范围的元素
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 列表键名
//   - start: 起始索引（包含）
//   - stop: 结束索引（包含）
//
// 返回：
//   - []string: 指定范围的元素列表
//   - error: 操作失败时返回错误
func (r *RedisManager) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	vals, err := r.client.LRange(ctx, key, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("获取列表 %s 范围失败: %w", key, err)
	}
	return vals, nil
}

// LLen 获取列表长度
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 列表键名
//
// 返回：
//   - int64: 列表长度
//   - error: 操作失败时返回错误
func (r *RedisManager) LLen(ctx context.Context, key string) (int64, error) {
	length, err := r.client.LLen(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("获取列表 %s 长度失败: %w", key, err)
	}
	return length, nil
}

// =============================================================================
// 集合操作方法
// =============================================================================

// SAdd 向集合添加成员
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 集合键名
//   - members: 要添加的成员列表
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) SAdd(ctx context.Context, key string, members ...interface{}) error {
	err := r.client.SAdd(ctx, key, members...).Err()
	if err != nil {
		return fmt.Errorf("向集合 %s 添加成员失败: %w", key, err)
	}
	return nil
}

// SRem 从集合移除成员
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 集合键名
//   - members: 要移除的成员列表
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) SRem(ctx context.Context, key string, members ...interface{}) error {
	err := r.client.SRem(ctx, key, members...).Err()
	if err != nil {
		return fmt.Errorf("从集合 %s 移除成员失败: %w", key, err)
	}
	return nil
}

// SIsMember 检查成员是否在集合中
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 集合键名
//   - member: 要检查的成员
//
// 返回：
//   - bool: 如果成员在集合中返回 true，否则返回 false
//   - error: 操作失败时返回错误
func (r *RedisManager) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	isMember, err := r.client.SIsMember(ctx, key, member).Result()
	if err != nil {
		return false, fmt.Errorf("检查集合 %s 成员失败: %w", key, err)
	}
	return isMember, nil
}

// SMembers 获取集合的所有成员
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 集合键名
//
// 返回：
//   - []string: 集合中所有成员的列表
//   - error: 操作失败时返回错误
func (r *RedisManager) SMembers(ctx context.Context, key string) ([]string, error) {
	members, err := r.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("获取集合 %s 成员失败: %w", key, err)
	}
	return members, nil
}

// SCard 获取集合的成员数量
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 集合键名
//
// 返回：
//   - int64: 集合中成员的数量
//   - error: 操作失败时返回错误
func (r *RedisManager) SCard(ctx context.Context, key string) (int64, error) {
	count, err := r.client.SCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("获取集合 %s 成员数量失败: %w", key, err)
	}
	return count, nil
}

// =============================================================================
// 有序集合操作方法
// =============================================================================

// ZAdd 向有序集合添加成员和分数
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 有序集合键名
//   - members: 成员和分数的列表
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	err := r.client.ZAdd(ctx, key, members...).Err()
	if err != nil {
		return fmt.Errorf("向有序集合 %s 添加成员失败: %w", key, err)
	}
	return nil
}

// ZRem 从有序集合移除成员
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 有序集合键名
//   - members: 要移除的成员列表
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) ZRem(ctx context.Context, key string, members ...interface{}) error {
	err := r.client.ZRem(ctx, key, members...).Err()
	if err != nil {
		return fmt.Errorf("从有序集合 %s 移除成员失败: %w", key, err)
	}
	return nil
}

// ZRange 按排名范围获取有序集合成员
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 有序集合键名
//   - start: 起始排名（包含）
//   - stop: 结束排名（包含）
//
// 返回：
//   - []string: 指定排名范围的成员列表
//   - error: 操作失败时返回错误
func (r *RedisManager) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	members, err := r.client.ZRange(ctx, key, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("获取有序集合 %s 排名范围失败: %w", key, err)
	}
	return members, nil
}

// ZRangeByScore 按分数范围获取有序集合成员
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 有序集合键名
//   - min: 最小分数
//   - max: 最大分数
//
// 返回：
//   - []string: 指定分数范围的成员列表
//   - error: 操作失败时返回错误
func (r *RedisManager) ZRangeByScore(ctx context.Context, key string, min, max string) ([]string, error) {
	members, err := r.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("获取有序集合 %s 分数范围失败: %w", key, err)
	}
	return members, nil
}

// ZCard 获取有序集合的成员数量
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 有序集合键名
//
// 返回：
//   - int64: 有序集合中成员的数量
//   - error: 操作失败时返回错误
func (r *RedisManager) ZCard(ctx context.Context, key string) (int64, error) {
	count, err := r.client.ZCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("获取有序集合 %s 成员数量失败: %w", key, err)
	}
	return count, nil
}

// =============================================================================
// 键操作方法
// =============================================================================

// Del 删除一个或多个键
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - keys: 要删除的键名列表
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) Del(ctx context.Context, keys ...string) error {
	err := r.client.Del(ctx, keys...).Err()
	if err != nil {
		return fmt.Errorf("删除键失败: %w", err)
	}
	return nil
}

// Exists 检查键是否存在
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - keys: 要检查的键名列表
//
// 返回：
//   - int64: 存在的键的数量
//   - error: 操作失败时返回错误
func (r *RedisManager) Exists(ctx context.Context, keys ...string) (int64, error) {
	count, err := r.client.Exists(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("检查键存在性失败: %w", err)
	}
	return count, nil
}

// Expire 设置键的过期时间
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 键名
//   - expiration: 过期时间
//
// 返回：
//   - error: 操作失败时返回错误
func (r *RedisManager) Expire(ctx context.Context, key string, expiration time.Duration) error {
	err := r.client.Expire(ctx, key, expiration).Err()
	if err != nil {
		return fmt.Errorf("设置键 %s 过期时间失败: %w", key, err)
	}
	return nil
}

// TTL 获取键的剩余生存时间
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 键名
//
// 返回：
//   - time.Duration: 剩余生存时间，-1 表示永不过期，-2 表示键不存在
//   - error: 操作失败时返回错误
func (r *RedisManager) TTL(ctx context.Context, key string) (time.Duration, error) {
	ttl, err := r.client.TTL(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("获取键 %s TTL 失败: %w", key, err)
	}
	return ttl, nil
}

// Type 获取键的数据类型
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - key: 键名
//
// 返回：
//   - string: 键的数据类型（string, list, set, zset, hash, none）
//   - error: 操作失败时返回错误
func (r *RedisManager) Type(ctx context.Context, key string) (string, error) {
	keyType, err := r.client.Type(ctx, key).Result()
	if err != nil {
		return "", fmt.Errorf("获取键 %s 类型失败: %w", key, err)
	}
	return keyType, nil
}

// =============================================================================
// 示例使用函数
// =============================================================================

// ExampleUsage 演示如何使用 RedisManager 进行各种操作
func ExampleUsage() error {
	// 创建 Redis 管理器
	config := internal.DefaultRedisConfig()
	config.DB = 1 // 使用数据库 1 进行演示

	manager, err := NewRedisManager(config)
	if err != nil {
		return fmt.Errorf("创建 Redis 管理器失败: %w", err)
	}
	defer manager.Close()

	ctx := context.Background()

	// 字符串操作示例
	fmt.Println("=== 字符串操作 ===")
	if err := manager.Set(ctx, "demo:string", "Hello Redis", 60*time.Second); err != nil {
		return err
	}

	val, err := manager.Get(ctx, "demo:string")
	if err != nil {
		return err
	}
	fmt.Printf("获取到的值: %s\n", val)

	// 哈希操作示例
	fmt.Println("\n=== 哈希操作 ===")
	if err := manager.HSet(ctx, "demo:hash", "name", "张三"); err != nil {
		return err
	}
	if err := manager.HSet(ctx, "demo:hash", "age", "25"); err != nil {
		return err
	}

	hashData, err := manager.HGetAll(ctx, "demo:hash")
	if err != nil {
		return err
	}
	fmt.Printf("哈希数据: %+v\n", hashData)

	// 列表操作示例
	fmt.Println("\n=== 列表操作 ===")
	if err := manager.LPush(ctx, "demo:list", "item1", "item2", "item3"); err != nil {
		return err
	}

	listData, err := manager.LRange(ctx, "demo:list", 0, -1)
	if err != nil {
		return err
	}
	fmt.Printf("列表数据: %+v\n", listData)

	// 集合操作示例
	fmt.Println("\n=== 集合操作 ===")
	if err := manager.SAdd(ctx, "demo:set", "member1", "member2", "member3"); err != nil {
		return err
	}

	setData, err := manager.SMembers(ctx, "demo:set")
	if err != nil {
		return err
	}
	fmt.Printf("集合数据: %+v\n", setData)

	// 有序集合操作示例
	fmt.Println("\n=== 有序集合操作 ===")
	if err := manager.ZAdd(ctx, "demo:zset",
		redis.Z{Score: 100, Member: "player1"},
		redis.Z{Score: 85, Member: "player2"},
		redis.Z{Score: 95, Member: "player3"}); err != nil {
		return err
	}

	zsetData, err := manager.ZRange(ctx, "demo:zset", 0, -1)
	if err != nil {
		return err
	}
	fmt.Printf("有序集合数据（按分数排序）: %+v\n", zsetData)

	fmt.Println("\n=== 演示完成 ===")
	return nil
}
