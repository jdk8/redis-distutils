# redis-distutils

Distribute lock and semaphore on redis cluster.

## Lock example

```python
from rediscluster import StrictRedisCluster

if __name__ == "__main__":
    startup_nodes = [{"host": "127.0.0.1", "port": "7001"},
                     {"host": "127.0.0.1", "port": "7002"},
                     {"host": "127.0.0.1", "port": "7003"},
                     {"host": "127.0.0.1", "port": "7004"},
                     {"host": "127.0.0.1", "port": "7005"}]
    rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    lock = rc.cluster_try_lock("lockid", 20000, 1000)
    print lock.is_lock_valid()
    rc.cluster_release_lock(lock)
```

cluster_try_lock takes 3 arguments:

- lockid: id of the lock
- lock_valid_time(in millisecond): lock will expire after lock_valid_time
- clock_drift(in millisecond): clock drift of cluster nodes

## Semaphore example

```python
from rediscluster import StrictRedisCluster

if __name__ == "__main__":
    startup_nodes = [{"host": "127.0.0.1", "port": "7001"},
                     {"host": "127.0.0.1", "port": "7002"},
                     {"host": "127.0.0.1", "port": "7003"},
                     {"host": "127.0.0.1", "port": "7004"},
                     {"host": "127.0.0.1", "port": "7005"}]
    rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    semaphore_id = rc.acquire_fair_semaphore("semaphore_name", 1000, 60)
    print semaphore_id
    semaphore_id and rc.release_fair_semaphore("semaphore_name", semaphore_id)
```

acquire_fair_semaphore takes 3 arguments:

- semaphore_name: name of the semaphore
- resources: semaphore resources
- timeout(in seconds): after timeout seconds, semaphore with semaphore_id will be released automatically