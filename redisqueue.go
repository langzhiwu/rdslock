package rdslock

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type RedisQueue struct {
	RLock *RedisLock
}

/**
* 入队一个 Task
* @param  [type]  $name	   队列名称
* @param  [type]  $id      任务id（或者其数组）
* @param  integer $timeout   入队超时时间(秒)
 */
func (rQueue *RedisQueue) Enqueue(name string, id []int, timeout int, afterInternal int) bool {
	if name == "" || len(id) == 0 || timeout <= 0 {
		return false
	}
	redisKey := "Queue:" + name
	//加锁
	if rQueue.RLock.Lock(redisKey, timeout, 15, 0) == false {
		return false
	}

	//入队时间戳作为score
	score := time.Now().Unix() + int64(afterInternal)
	//入队
	for _, v := range id {
		v1, err := rQueue.RLock.Redis_conn.Do("ZSCORE", redisKey, v)
		if err == nil && v1 != 0 {
			rQueue.RLock.Redis_conn.Do("ZADD", redisKey, score, v)
		}
	}

	//解锁
	rQueue.RLock.Unlock(redisKey)
	return true

}

/**
* 出队一个Task，需要指定$id 和 $score
* 如果score 与队列中的匹配则出队，否则认为该Task已被重新入队过，当前操作按失败处理
* @param  [type]  name    队列名称
* @param  [type]  id      任务标识
* param  [type]  score   任务对应score，从队列中获取任务时会返回一个score，只有score和队列中的值匹配时Task才会被出队
* @param  integer timeout 超时时间(秒)
* @return [type]           Task是否成功，返回false可能是redis操作失败，也有可能是score与队列中的值不匹配（这表示该Task自从获取到本地之后被其他线程入队过）
 */
func (rQueue *RedisQueue) Dequue(name string, id int, score int, timeout int) bool {
	if name == "" || id == 0 || score <= 0 {
		return false
	}
	redisKey := "Queue:" + name
	//加锁
	if rQueue.RLock.Lock(redisKey, timeout, 15, 0) == false {
		return false
	}

	//出队
	v, err := rQueue.RLock.Redis_conn.Do("ZSCORE", redisKey, id)
	if err != nil || v != score {
		return false
	}
	_, err1 := rQueue.RLock.Redis_conn.Do("ZREM", redisKey, id)
	if err1 != nil {
		return false
	}

	//解锁
	rQueue.RLock.Unlock(redisKey)
	return true
}

/**
* 获取队列顶部若干个Task 并将其出队
* param  [type]  $name    队列名称
* param  integer $count   数量
* param  integer $timeout 超时时间
* return [type]           返回数组[0=>['id'=> , 'score'=> ], 1=>['id'=> , 'score'=> ], 2=>['id'=> , 'score'=> ]]
 */
func (rQueue *RedisQueue) Pop(name string, count int, timeout int) map[string]string {
	if name == "" || count <= 0 {
		return make(map[string]string)
	}

	redisKey := "Queue:" + name
	//加锁
	if rQueue.RLock.Lock(redisKey, timeout, 15, 0) == false {
		return make(map[string]string)
	}

	user_map, err := redis.StringMap(rQueue.RLock.Redis_conn.Do("ZRANGE", redisKey, 0, count, "withscores"))
	if err != nil {
		return make(map[string]string)
	}

	//解锁
	rQueue.RLock.Unlock(redisKey)
	return user_map
}
