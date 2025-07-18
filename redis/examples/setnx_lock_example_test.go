package examples

import (
	"context"
	"testing"
	"time"

	"github.com/yann0917/redis-usage/internal"
	redisops "github.com/yann0917/redis-usage/redis"
)

// 测试配置
var testConfig = &internal.RedisConfig{
	Addr:         "localhost:6379",
	Password:     "",
	DB:           15, // 使用数据库 15 进行测试
	PoolSize:     5,
	MinIdleConns: 2,
	DialTimeout:  5 * time.Second,
	ReadTimeout:  3 * time.Second,
	WriteTimeout: 3 * time.Second,
}

func TestSetNXExample(t *testing.T) {
	// 创建测试用的 Redis 管理器
	manager, err := redisops.NewRedisManager(testConfig)
	if err != nil {
		t.Fatalf("创建 Redis 管理器失败: %v", err)
	}
	defer manager.Close()

	// 清空测试数据库
	ctx := context.Background()
	if err := manager.FlushDB(ctx); err != nil {
		t.Fatalf("清空测试数据库失败: %v", err)
	}

	// 运行基本 SetNX 示例测试
	if err := SetNXExample(); err != nil {
		t.Errorf("SetNX 示例执行失败: %v", err)
	}
}

func TestDistributedLockExample(t *testing.T) {
	// 创建测试用的 Redis 管理器
	manager, err := redisops.NewRedisManager(testConfig)
	if err != nil {
		t.Fatalf("创建 Redis 管理器失败: %v", err)
	}
	defer manager.Close()

	// 清空测试数据库
	ctx := context.Background()
	if err := manager.FlushDB(ctx); err != nil {
		t.Fatalf("清空测试数据库失败: %v", err)
	}

	// 运行分布式锁示例测试
	if err := DistributedLockExample(); err != nil {
		t.Errorf("分布式锁示例执行失败: %v", err)
	}
}

func TestPreventDuplicateSubmissionExample(t *testing.T) {
	// 创建测试用的 Redis 管理器
	manager, err := redisops.NewRedisManager(testConfig)
	if err != nil {
		t.Fatalf("创建 Redis 管理器失败: %v", err)
	}
	defer manager.Close()

	// 清空测试数据库
	ctx := context.Background()
	if err := manager.FlushDB(ctx); err != nil {
		t.Fatalf("清空测试数据库失败: %v", err)
	}

	// 运行防重复提交示例测试
	if err := PreventDuplicateSubmissionExample(); err != nil {
		t.Errorf("防重复提交示例执行失败: %v", err)
	}
}

func TestDistributedLock_ConcurrentAccess(t *testing.T) {
	// 创建测试用的 Redis 管理器
	manager, err := redisops.NewRedisManager(testConfig)
	if err != nil {
		t.Fatalf("创建 Redis 管理器失败: %v", err)
	}
	defer manager.Close()

	ctx := context.Background()
	if err := manager.FlushDB(ctx); err != nil {
		t.Fatalf("清空测试数据库失败: %v", err)
	}

	lockKey := "test:concurrent_lock"
	lockTTL := 5 * time.Second

	// 测试并发获取锁
	lock1 := NewDistributedLock(manager, lockKey, "process_1", lockTTL)
	lock2 := NewDistributedLock(manager, lockKey, "process_2", lockTTL)

	// 进程1获取锁
	acquired1, err := lock1.TryLock(ctx)
	if err != nil {
		t.Fatalf("进程1获取锁失败: %v", err)
	}
	if !acquired1 {
		t.Error("期望进程1获取锁成功")
	}

	// 进程2尝试获取锁，应该失败
	acquired2, err := lock2.TryLock(ctx)
	if err != nil {
		t.Fatalf("进程2尝试获取锁失败: %v", err)
	}
	if acquired2 {
		t.Error("期望进程2获取锁失败")
	}

	// 进程1释放锁
	if err := lock1.Unlock(ctx); err != nil {
		t.Fatalf("进程1释放锁失败: %v", err)
	}

	// 现在进程2应该能获取锁
	acquired3, err := lock2.TryLock(ctx)
	if err != nil {
		t.Fatalf("进程2重新获取锁失败: %v", err)
	}
	if !acquired3 {
		t.Error("期望进程2重新获取锁成功")
	}

	// 清理
	lock2.Unlock(ctx)
}

func TestDistributedLock_WrongOwnerUnlock(t *testing.T) {
	// 创建测试用的 Redis 管理器
	manager, err := redisops.NewRedisManager(testConfig)
	if err != nil {
		t.Fatalf("创建 Redis 管理器失败: %v", err)
	}
	defer manager.Close()

	ctx := context.Background()
	if err := manager.FlushDB(ctx); err != nil {
		t.Fatalf("清空测试数据库失败: %v", err)
	}

	lockKey := "test:wrong_owner_lock"
	lockTTL := 5 * time.Second

	lock1 := NewDistributedLock(manager, lockKey, "process_1", lockTTL)
	lock2 := NewDistributedLock(manager, lockKey, "process_2", lockTTL)

	// 进程1获取锁
	acquired, err := lock1.TryLock(ctx)
	if err != nil {
		t.Fatalf("进程1获取锁失败: %v", err)
	}
	if !acquired {
		t.Error("期望进程1获取锁成功")
	}

	// 进程2尝试释放不属于自己的锁，应该失败
	err = lock2.Unlock(ctx)
	if err == nil {
		t.Error("期望进程2释放他人的锁失败，但成功了")
	}

	// 验证锁仍然存在且属于进程1
	exists, err := manager.Exists(ctx, lockKey)
	if err != nil {
		t.Fatalf("检查锁存在性失败: %v", err)
	}
	if exists == 0 {
		t.Error("期望锁仍然存在")
	}

	// 进程1正常释放锁
	if err := lock1.Unlock(ctx); err != nil {
		t.Fatalf("进程1释放锁失败: %v", err)
	}
}
