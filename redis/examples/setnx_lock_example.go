package examples

import (
	"context"
	"fmt"
	"time"

	"github.com/yann0917/redis-usage/internal"
	redisops "github.com/yann0917/redis-usage/redis"
)

// DistributedLock 分布式锁结构体
type DistributedLock struct {
	manager   *redisops.RedisManager
	lockKey   string
	lockValue string
	ttl       time.Duration
}

// NewDistributedLock 创建分布式锁实例
// 参数：
//   - manager: Redis 管理器
//   - lockKey: 锁的键名
//   - lockValue: 锁的值（通常是唯一标识，如进程ID+线程ID）
//   - ttl: 锁的过期时间
func NewDistributedLock(manager *redisops.RedisManager, lockKey, lockValue string, ttl time.Duration) *DistributedLock {
	return &DistributedLock{
		manager:   manager,
		lockKey:   lockKey,
		lockValue: lockValue,
		ttl:       ttl,
	}
}

// TryLock 尝试获取锁
// 返回：
//   - bool: 是否获取成功
//   - error: 操作错误
func (dl *DistributedLock) TryLock(ctx context.Context) (bool, error) {
	acquired, err := dl.manager.SetNX(ctx, dl.lockKey, dl.lockValue, dl.ttl)
	if err != nil {
		return false, fmt.Errorf("尝试获取锁失败: %w", err)
	}
	return acquired, nil
}

// Unlock 释放锁（仅当锁的值匹配时才释放）
// 使用 Lua 脚本确保原子性
func (dl *DistributedLock) Unlock(ctx context.Context) error {
	// 这里简化实现，实际项目中应使用 Lua 脚本确保原子性
	currentValue, err := dl.manager.Get(ctx, dl.lockKey)
	if err != nil {
		// 键不存在，认为锁已释放
		return nil
	}

	// 只有当前持有锁的进程才能释放锁
	if currentValue == dl.lockValue {
		return dl.manager.Del(ctx, dl.lockKey)
	}

	return fmt.Errorf("锁不属于当前进程，无法释放")
}

// SetNXExample SetNX 基本用法示例
func SetNXExample() error {
	// 创建 Redis 管理器
	config := internal.DefaultRedisConfig()
	manager, err := redisops.NewRedisManager(config)
	if err != nil {
		return fmt.Errorf("创建 Redis 管理器失败: %w", err)
	}
	defer manager.Close()

	ctx := context.Background()

	// 示例 1: 基本 SetNX 用法
	fmt.Println("=== SetNX 基本用法示例 ===")
	key := "example:setnx"
	value1 := "第一个值"
	value2 := "第二个值"

	// 第一次设置，应该成功
	success, err := manager.SetNX(ctx, key, value1, 30*time.Second)
	if err != nil {
		return fmt.Errorf("SetNX 操作失败: %w", err)
	}
	fmt.Printf("第一次 SetNX: %t\n", success)

	// 第二次设置相同键，应该失败
	success, err = manager.SetNX(ctx, key, value2, 30*time.Second)
	if err != nil {
		return fmt.Errorf("SetNX 操作失败: %w", err)
	}
	fmt.Printf("第二次 SetNX: %t\n", success)

	// 获取当前值
	currentValue, err := manager.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("获取值失败: %w", err)
	}
	fmt.Printf("当前值: %s\n", currentValue)

	// 清理
	manager.Del(ctx, key)

	return nil
}

// DistributedLockExample 分布式锁示例
func DistributedLockExample() error {
	// 创建 Redis 管理器
	config := internal.DefaultRedisConfig()
	manager, err := redisops.NewRedisManager(config)
	if err != nil {
		return fmt.Errorf("创建 Redis 管理器失败: %w", err)
	}
	defer manager.Close()

	ctx := context.Background()

	fmt.Println("\n=== 分布式锁示例 ===")

	// 创建分布式锁
	lockKey := "example:distributed_lock"
	lockValue1 := "process_1"
	lockValue2 := "process_2"
	lockTTL := 10 * time.Second

	lock1 := NewDistributedLock(manager, lockKey, lockValue1, lockTTL)
	lock2 := NewDistributedLock(manager, lockKey, lockValue2, lockTTL)

	// 进程1 尝试获取锁
	acquired1, err := lock1.TryLock(ctx)
	if err != nil {
		return fmt.Errorf("进程1获取锁失败: %w", err)
	}
	fmt.Printf("进程1获取锁: %t\n", acquired1)

	// 进程2 尝试获取相同的锁
	acquired2, err := lock2.TryLock(ctx)
	if err != nil {
		return fmt.Errorf("进程2获取锁失败: %w", err)
	}
	fmt.Printf("进程2获取锁: %t\n", acquired2)

	// 模拟业务逻辑执行
	fmt.Println("进程1执行业务逻辑...")
	time.Sleep(2 * time.Second)

	// 进程1 释放锁
	if err := lock1.Unlock(ctx); err != nil {
		return fmt.Errorf("进程1释放锁失败: %w", err)
	}
	fmt.Println("进程1释放锁")

	// 现在进程2 可以获取锁
	acquired3, err := lock2.TryLock(ctx)
	if err != nil {
		return fmt.Errorf("进程2重新获取锁失败: %w", err)
	}
	fmt.Printf("进程2重新获取锁: %t\n", acquired3)

	// 进程2 释放锁
	if err := lock2.Unlock(ctx); err != nil {
		return fmt.Errorf("进程2释放锁失败: %w", err)
	}
	fmt.Println("进程2释放锁")

	return nil
}

// PreventDuplicateSubmissionExample 防重复提交示例
func PreventDuplicateSubmissionExample() error {
	// 创建 Redis 管理器
	config := internal.DefaultRedisConfig()
	manager, err := redisops.NewRedisManager(config)
	if err != nil {
		return fmt.Errorf("创建 Redis 管理器失败: %w", err)
	}
	defer manager.Close()

	ctx := context.Background()

	fmt.Println("\n=== 防重复提交示例 ===")

	// 模拟用户提交请求
	userID := "user123"
	requestID := "req456"
	submitKey := fmt.Sprintf("submit:%s:%s", userID, requestID)
	expiration := 60 * time.Second // 1分钟内不允许重复提交

	// 第一次提交
	firstSubmit, err := manager.SetNX(ctx, submitKey, "processing", expiration)
	if err != nil {
		return fmt.Errorf("检查重复提交失败: %w", err)
	}

	if firstSubmit {
		fmt.Println("✓ 请求接受，开始处理业务逻辑")
		// 模拟业务处理时间
		time.Sleep(1 * time.Second)
		fmt.Println("✓ 业务处理完成")
	} else {
		fmt.Println("✗ 重复提交，请求被拒绝")
	}

	// 模拟用户重复提交
	secondSubmit, err := manager.SetNX(ctx, submitKey, "processing", expiration)
	if err != nil {
		return fmt.Errorf("检查重复提交失败: %w", err)
	}

	if secondSubmit {
		fmt.Println("✓ 请求接受，开始处理业务逻辑")
	} else {
		fmt.Println("✗ 重复提交，请求被拒绝")
	}

	// 查看剩余过期时间
	ttl, err := manager.TTL(ctx, submitKey)
	if err != nil {
		return fmt.Errorf("获取TTL失败: %w", err)
	}
	fmt.Printf("防重复提交键剩余过期时间: %v\n", ttl)

	// 清理
	manager.Del(ctx, submitKey)

	return nil
}

// RunAllSetNXExamples 运行所有 SetNX 示例
func RunAllSetNXExamples() error {
	fmt.Println("Redis SetNX 操作示例")
	fmt.Println("=====================")

	if err := SetNXExample(); err != nil {
		return err
	}

	if err := DistributedLockExample(); err != nil {
		return err
	}

	if err := PreventDuplicateSubmissionExample(); err != nil {
		return err
	}

	fmt.Println("\n所有示例执行完成！")
	return nil
}
