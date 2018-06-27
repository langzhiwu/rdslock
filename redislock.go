package redislock

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type RedisLock struct {
	Redis_conn  redis.Conn
	LockedNames map[string]int64
}

func NewRedisLock() *RedisLock {
	return &RedisLock{}
}

/**
* 加锁
* @param string name   锁的标识名
* @param int timeout   循环获取锁的等待超时时间，在此时间内会一直尝试获取锁直到超时，为0表示失败后直接返回不等待
* @param int expire    当前锁的最大生存时间(秒)，必须大于0，如果超过生存时间锁仍未被释放，则系统会自动强制释放
* @param int waitIntervalUs    获取锁失败后挂起再试的时间间隔(微秒)
* @return bool     true成功 false失败
 */
func (rlock *RedisLock) Lock(name string, timeout int, expire int, waitIntervalUs int) bool {
	if name == "" {
		return false
	}

	//获取当前时间
	nowtime := time.Now().Unix()
	//获取锁失败时的等待超时时刻
	timeoutAt := nowtime + int64(timeout)
	//锁的最大生存时刻
	expireAt := nowtime + int64(expire)
	//redis存放时的key
	redisKey := "kx:" + name

	for {
		//将rediskey的最大生存时刻存到redis里，过了这个时刻该锁会被自动释放
		result, err := rlock.Redis_conn.Do("SETNX", redisKey, expire)
		if err != nil {
			return false
		}

		if result != 0 {
			//设置key的失效时间
			rlock.Redis_conn.Do("EXPIRE", redisKey, expire)
			//将锁标志放到lockedNames数组里
			rlock.LockedNames[name] = expireAt
			return true
		}

		//以秒为单位，返回给定key的剩余生存时间
		ttl, _ := redis.Int64(rlock.Redis_conn.Do("TTL", redisKey))
		//ttl小于0 表示key上没有设置生存时间（key是不会不存在的，因为前面setnx会自动创建）
		//如果出现这种状况，那就是进程的某个实例setnx成功后 crash 导致紧跟着的expire没有被调用
		//这时可以直接设置expire并把锁纳为己用
		if ttl < 0 {
			rlock.Redis_conn.Do("EXPIRE", redisKey, expire)
			rlock.LockedNames[name] = expireAt
			return true
		}

		/*****循环请求锁部分*****/
		//如果没设置锁失败的等待时间 或者 已超过最大等待时间了，那就退出
		if timeout <= 0 || timeoutAt < time.Now().UnixNano()/1e6 {
			break
		}
		time.Sleep(time.Duration(waitIntervalUs))
	}
	return false
}

/**
* 解锁
* @param string name	锁的标识名
 */
func (rlock *RedisLock) Unlock(name string) bool {
	//先判断是否存在此锁
	if _, ok := rlock.LockedNames[name]; ok {
		//删除锁
		redisKey := "kx:" + name
		_, err := rlock.Redis_conn.Do("DEL", redisKey)
		if err != nil {
			return false
		}
		//清掉lockedNames里的锁标志
		delete(rlock.LockedNames, name)
		return true
	}
	return false
}

/**
* 释放当前所有获得的锁
* @return [type] [description]
 */
func (rlock *RedisLock) UnlockAll() bool {
	//此标志是用来标志是否释放所有锁成功
	allSuccess := true
	for name, _ := range rlock.LockedNames {
		if rlock.Unlock(name) == false {
			allSuccess = false
		}
	}
	return allSuccess
}

/**
* 给当前锁增加指定生存时间，必须大于0
* @param string name
* @param int expire
 */
func (rlock *RedisLock) Expire(name string, expire int) bool {
	if rlock.IsLocking(name) && expire > 0 {
		redisKey := "kx:" + name
		expire64 := int64(expire) + rlock.LockedNames[name]
		_, err := rlock.Redis_conn.Do("Expire", redisKey, expire64)
		if err != nil {
			return false
		}
		return true
	}
	return false
}

/**
* 判断当前是否拥有指定名字的锁
* @param string name
 */
func (rlock *RedisLock) IsLocking(name string) bool {
	if v, ok := rlock.LockedNames[name]; ok {
		redisKey := "kx:" + name
		rs, _ := redis.Int64(rlock.Redis_conn.Do("GET", redisKey))
		return v == rs
	}

	return false
}
