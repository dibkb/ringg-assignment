![System Architecture](system.png)

# Leaderboard Service Design

## 1. Data Structures

### PostgreSQL B-Tree Indexes

- Primary index: `(game_id, score DESC)` for Top-K queries
- Secondary index: `(user_id, game_id)` for user lookups

**Complexity Analysis:**

- Insert/Upsert: O(log N)
- Top-K Query: O(log N + K)
- User Score Lookup: O(log N)
- Rank Calculation: O(log N + R)

### Optional In-Memory Cache

- SortedList per game: `[(score, user_id)]`
- Operations: O(log M) for insert, O(K) for Top-K, O(log M) for rank lookup

## 2. Persistence & Recovery

### Kafka

- Replication Factor ≥ 3
- Producer acks=all
- Auto-commit enabled (1s interval)

### PostgreSQL

- WAL enabled
- Synchronous commits
- Connection pooling with asyncpg

### Recovery

- Idempotent upserts via ON CONFLICT
- Health monitoring
- Automated recovery on failures

## 3. Scaling Strategy

### Partitioning

- Kafka: Partition by game_id
- Database: Shard by game_id
- Global game_id → shard mapping

### Caching

- Pre-warm hot game caches
- LRU eviction policy
- Lazy loading for cold games

### Consistency

- Strong consistency for writes
- Eventual consistency for reads via replicas
- Cache invalidation on updates
