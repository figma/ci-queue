# Race Conditions in Master Setup/Heartbeat/Takeover

Analysis of potential race conditions in the master worker death detection and recovery mechanism.

## Status

| # | Race Condition | Severity | Status |
|---|----------------|----------|--------|
| 1 | TOCTOU in push() guard | Critical | **Fixed** (WATCH/MULTI) |
| 2 | Missing initial heartbeat window | High | **Fixed** |
| 3 | Takeover worker always runs setup | Critical | N/A (handled by #1 WATCH) |
| 4 | Inconsistent random seeds | Low | Open |
| 5 | push() guard skipped when master-worker-id is nil | Medium | Open |
| 6 | Heartbeat thread exceptions silently swallowed | Medium | Open |
| 7 | Network partition: master alive but isolated | High | Open (hard to fix) |
| 8 | Race in queue_initialized? caching | Low | OK (Lua rejects) |
| 9 | Takeover during store_chunk_metadata | Medium | Open |
| 10 | run_master_setup uses stale @index | Low | OK (tests don't change mid-run) |

---

## Detailed Analysis

### 1. TOCTOU in `push()` Guard - FIXED

**Problem:**
```ruby
current_master = redis.get(key('master-worker-id'))  # CHECK
if current_master && current_master != worker_id
  return
end
# <-- RACE WINDOW: takeover can happen here
redis.multi do |transaction|                          # USE
  transaction.lpush(key('queue'), tests)
```

**Scenario:**
1. Master A checks `master-worker-id` → sees "A" ✓
2. Worker B executes takeover Lua script, sets `master-worker-id` = "B"
3. Master A proceeds with `push()`, pushes tests
4. Worker B runs `run_master_setup()`, also pushes tests
5. **Result: Tests enqueued twice**

**Fix:** Use WATCH/MULTI to abort transaction if `master-worker-id` changes.

---

### 2. Missing Initial Heartbeat Window

**Problem:**
```ruby
# In acquire_master_role?:
@master = redis.setnx(key('master-status'), 'setup')
redis.set(key('master-worker-id'), worker_id)
# <-- No heartbeat set yet!

# Later in populate():
if acquire_master_role?
  with_master_setup_heartbeat do  # <-- First heartbeat sent HERE
```

**Scenario:**
1. Master A acquires role, sets `master-status` = "setup"
2. Before entering `with_master_setup_heartbeat`, Worker B checks heartbeat
3. No heartbeat exists → `master_setup_heartbeat_stale?` returns `true`
4. Worker B takes over even though Master A just started

**Proposed Fix:** Set initial heartbeat in `acquire_master_role?` immediately after acquiring the role.

```ruby
# In acquire_master_role?:
def acquire_master_role?
  return true if @master

  @master = redis.setnx(key('master-status'), 'setup')
  if @master
    begin
      redis.set(key('master-worker-id'), worker_id)
      redis.expire(key('master-worker-id'), config.redis_ttl)

      # NEW: Set initial heartbeat immediately to prevent premature takeover
      redis.set(key('master-setup-heartbeat'), CI::Queue.time_now.to_f)
      redis.expire(key('master-setup-heartbeat'), config.redis_ttl)

      warn "Worker #{worker_id} elected as master"
    rescue *CONNECTION_ERRORS
      warn("Failed to set master-worker-id: #{$!.message}")
    end
  end
  @master
rescue *CONNECTION_ERRORS
  @master = nil
  false
end
```

**Why this works:**
- Heartbeat is set atomically with master-worker-id (same begin/rescue block)
- No window exists where status="setup" but heartbeat is missing
- `with_master_setup_heartbeat` will continue refreshing the heartbeat afterward
- If setting heartbeat fails (connection error), we log but continue (same as master-worker-id)

---

### 3. Takeover Worker Always Runs Setup - NOT A REAL ISSUE

**Original Concern:**
```ruby
# In wait_for_master():
if attempt_master_takeover
  run_master_setup  # <-- Always called after takeover
  return true
end
```

**Scenario:**
1. Master A finishes `reorder_tests`, calls `push()`, passes WATCH check
2. Master A's heartbeat becomes stale during the `redis.multi` execution
3. Worker B takes over (status still "setup" until transaction commits)
4. Master A's transaction commits, sets status = "ready"
5. Worker B's `run_master_setup()` also pushes tests
6. **Result: Tests enqueued twice**

**Why this is NOT a real issue:**

The WATCH/MULTI fix for #1 already handles this. The takeover Lua script writes to `master-worker-id`, which is the key being watched in `push()`. If Worker B takes over:
- Lua script sets `master-worker-id = "B"`
- Master A's WATCH detects the change
- Master A's transaction returns `nil` and aborts

The only possible outcomes are:
1. Master A's MULTI completes before Lua → status="ready", Lua sees status≠"setup", takeover fails
2. Lua runs during WATCH window → WATCH trips, Master A aborts, only Worker B pushes

---

### 4. Inconsistent Random Seeds

**Problem:**
```ruby
# Original master (in populate):
def populate(tests, random: Random.new)  # Unseeded by default!
  executables = reorder_tests(tests, random: random)

# Takeover master (in run_master_setup):
random = Random.new(Digest::SHA256.hexdigest(config.seed).to_i(16))  # Seeded
```

**Impact:** If both masters somehow push (due to other races), test ordering would differ.

**Potential Fix:** Use seeded random in both places, or accept that ordering may differ (not critical if we prevent double-push).

---

### 5. `push()` Guard Skipped When `master-worker-id` is nil

**Problem:**
```ruby
current_master = redis.get(key('master-worker-id'))
if current_master && current_master != worker_id  # Skipped if nil!
```

**Scenario:**
1. Master A's `redis.set(key('master-worker-id'))` fails (connection error in `acquire_master_role?`)
2. Or `master-worker-id` TTL expires before push
3. `current_master` = nil, guard check is skipped
4. Push proceeds even though we may not be master

**Potential Fix:** Require `master-worker-id` to be non-nil and match, or fail the push.

---

### 6. Heartbeat Thread Exceptions Silently Swallowed

**Problem:**
```ruby
rescue StandardError => e
  warn("[master-setup-heartbeat] Failed to send heartbeat: #{e.message}")
  # <-- Thread continues, but heartbeat wasn't sent
end
```

**Scenario:**
1. Redis has temporary auth issue or connection problem
2. Heartbeat fails repeatedly but thread keeps running
3. Other workers see stale heartbeat, take over
4. Original master still thinks it's master (no indication heartbeats are failing)

**Potential Fix:** Track consecutive heartbeat failures. After N failures, set a flag or abort setup.

---

### 7. Network Partition: Master Alive but Isolated

**Scenario:**
1. Master A can't reach Redis (network partition)
2. Master A continues `reorder_tests` locally (no Redis needed for this step)
3. Workers see stale heartbeat, Worker B takes over
4. Partition heals
5. Master A tries `push()` - WATCH should detect the change and abort

**Status:** This is largely mitigated by the WATCH/MULTI fix, but network partitions are inherently difficult to handle perfectly in distributed systems.

---

### 8. Race in `queue_initialized?` Caching

**Problem:**
```ruby
def queue_initialized?
  @queue_initialized ||= begin
    status = master_status
    %w[ready finished].include?(status)
  end
end
```

**Scenario:**
1. Worker checks `queue_initialized?` → false (status = "setup")
2. Worker attempts takeover
3. Meanwhile, status changes to "ready"
4. Cached `@queue_initialized` remains false for this worker

**Status:** OK - The takeover Lua script checks status again atomically and will reject if status != "setup".

---

### 9. Takeover During `store_chunk_metadata`

**Scenario:**
1. Master A stores metadata for chunks 1-50
2. Master A's heartbeat becomes stale
3. Worker B takes over, runs full setup (stores metadata for all chunks)
4. Master A continues, stores metadata for chunks 51-100
5. Master A's `push()` aborts (WATCH), but chunk metadata is partially written

**Impact:** Chunk metadata might have entries from both masters. Since chunk IDs are deterministic, this may cause overwrites but not corruption.

**Potential Fix:** Could wrap chunk metadata storage in a check, but impact is low.

---

### 10. `run_master_setup` Uses Stale `@index`

**Problem:**
```ruby
def run_master_setup
  tests = @index.values  # From original populate() call
```

**Scenario:** If tests could theoretically change between `populate()` and takeover, the takeover worker would use stale data.

**Status:** OK - In a single CI run, the test list is fixed at startup and doesn't change.

---

## Recommended Fixes (Priority Order)

1. **Fix #3 (Critical):** Check `master-status` in `run_master_setup` before pushing
2. **Fix #2 (High):** Set initial heartbeat in `acquire_master_role?`
3. **Fix #5 (Medium):** Require non-nil `master-worker-id` in push guard
4. **Fix #6 (Medium):** Track consecutive heartbeat failures
