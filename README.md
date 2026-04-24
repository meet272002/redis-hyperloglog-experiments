# DS614 Big Data Engineering — Final Project Report
## Redis HyperLogLog: A Probabilistic Cardinality Estimation System

**Course:** DS614 — Big Data Engineering  
**System Studied:** Redis 7.2 — HyperLogLog (`src/hyperloglog.c`)  
**Repository:** [Your GitHub Link Here]

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Execution Trace — PFADD → PFCOUNT](#2-execution-trace--pfadd--pfcount)
3. [Design Decisions](#3-design-decisions)
4. [Concept Mapping](#4-concept-mapping)
5. [Experiments & Results](#5-experiments--results)
6. [Failure Analysis](#6-failure-analysis)
7. [Key Insights](#7-key-insights)
8. [GitHub Artifacts](#8-github-artifacts)

---

## 1. System Overview

### What Problem Does HyperLogLog Solve?

Counting unique elements (cardinality) in large-scale data streams is a fundamental problem in data engineering. Exact counting requires storing every unique element ever seen — for 100 million unique users this means gigabytes of memory **per counter**. At scale, this becomes completely impractical.

Redis HyperLogLog solves this by answering:

> *"Approximately how many unique elements have been added?"*

with only **12KB of fixed memory** and a guaranteed **~0.81% error rate** — regardless of whether you have 100 or 100 million unique elements.

### What This System IS and IS NOT

| HyperLogLog IS | HyperLogLog IS NOT |
|---|---|
| A cardinality estimator | An exact counter |
| A streaming algorithm (single pass) | A storage system |
| Memory-constant (always 12KB dense) | Reversible (cannot retrieve elements) |
| Mergeable across distributed nodes | Suitable when exact count is required |

### Real-World Use Cases

- Count unique visitors to a website per day
- Count unique search queries per hour
- Count unique IPs hitting an API endpoint
- Count unique products viewed per user session
- Detect duplicate events in event streams

### How to Use It (Redis Commands)

```bash
# Add elements to HyperLogLog
PFADD myhll "apple" "banana" "cherry"
# (integer) 1  ← registers were updated

# Get estimated unique count
PFCOUNT myhll
# (integer) 3

# Duplicates are automatically ignored
PFADD myhll "apple"
PFCOUNT myhll
# (integer) 3  ← unchanged, idempotent

# Merge multiple HLLs (distributed use case)
PFMERGE result hll_region_a hll_region_b
PFCOUNT result
# returns union cardinality of both HLLs
```

### System Setup (Verified)

```bash
# Clone Redis 7.2 source
git clone https://github.com/redis/redis.git
cd redis && git checkout 7.2

# Compile from source
make

# Start server
src/redis-server

# Connect client
src/redis-cli
```

Primary source file: `src/hyperloglog.c` (~1,300 lines)

---

## 2. Execution Trace — PFADD → PFCOUNT

This section traces one complete execution path through the system, referencing actual source code functions and line numbers.

### Key Function Map

| Function | File | Line | Role |
|---|---|---|---|
| `pfaddCommand` | hyperloglog.c | 1229 | PFADD Redis command entry point |
| `hllAdd` | hyperloglog.c | 1088 | Routes to sparse or dense path |
| `hllPatLen` | hyperloglog.c | 452 | Hashes element → register index + count |
| `MurmurHash64A` | hyperloglog.c | 397 | Produces uniform 64-bit hash |
| `hllDenseSet` | hyperloglog.c | ~514 | Updates register if new count is higher |
| `hllSparseToDense` | hyperloglog.c | 586 | One-way sparse→dense promotion |
| `pfcountCommand` | hyperloglog.c | 1269 | PFCOUNT Redis command entry point |
| `hllCount` | hyperloglog.c | 1050 | Applies HyperLogLog estimation formula |

---

### 2.1 PFADD Execution Path

**Entry Point:** `pfaddCommand()` — `src/hyperloglog.c` line 1229

```c
void pfaddCommand(client *c) {
    robj *o = lookupKeyWrite(c->db, c->argv[1]);
    // c->argv[0] = "PFADD"   (command)
    // c->argv[1] = "myhll"   (key name)
    // c->argv[2..n] = elements ("apple", "banana", ...)

    if (o == NULL) {
        o = createHLLObject();        // key doesn't exist → create fresh HLL
        dbAdd(c->db, c->argv[1], o); // save to Redis DB
        updated++;
    } else {
        isHLLObjectOrReply(c, o);     // validate it's really an HLL
        o = dbUnshareStringValue(...);// private copy for modification
    }

    for (j = 2; j < c->argc; j++) {
        int retval = hllAdd(o, c->argv[j]->ptr, sdslen(c->argv[j]->ptr));
        // retval = 1 → register updated (new unique element)
        // retval = 0 → no change (duplicate element)
        // retval = -1 → error (corrupted HLL)
        if (retval == 1) updated++;
    }

    if (updated) {
        signalModifiedKey(...);          // notify Redis internals
        notifyKeyspaceEvent(...,"pfadd");// publish keyspace event
        server.dirty += updated;         // mark for AOF/RDB persistence
        HLL_INVALIDATE_CACHE(hdr);       // clear cached cardinality
    }

    addReply(c, updated ? shared.cone : shared.czero);
    // Returns (integer) 1 if any register changed
    // Returns (integer) 0 if nothing changed
}
```

---

**Step 2:** `hllAdd()` — `src/hyperloglog.c` line 1088

```c
int hllAdd(robj *o, unsigned char *ele, size_t elesize) {
    struct hllhdr *hdr = o->ptr;
    switch(hdr->encoding) {
    case HLL_DENSE:  return hllDenseAdd(hdr->registers, ele, elesize);
    case HLL_SPARSE: return hllSparseAdd(o, ele, elesize);
    default:         return -1;   // corrupted / unknown encoding
    }
}
```

This is a pure routing function — it checks whether the HLL currently uses sparse or dense encoding and calls the appropriate path. This is where **Design Decision #2** (adaptive encoding) is enforced.

---

**Step 3:** `hllPatLen()` — `src/hyperloglog.c` line 452

This is the **mathematical core** of HyperLogLog. It converts any element into two values: which register to update, and what value to store.

```c
int hllPatLen(unsigned char *ele, size_t elesize, long *regp) {
    uint64_t hash, bit, index;
    int count;

    // Step 1: Hash the element to a uniform 64-bit number
    hash = MurmurHash64A(ele, elesize, 0xadc83b19ULL);

    // Step 2: Extract register index from first 14 bits
    index = hash & HLL_P_MASK;   // HLL_P_MASK = 0x3FFF (14-bit mask)
    // This selects 1 of 16,384 registers

    // Step 3: Shift away the 14 index bits
    hash >>= HLL_P;               // HLL_P = 14

    // Step 4: Safety termination bit
    hash |= ((uint64_t)1 << HLL_Q);
    // Forces a 1 at position 51 — guarantees loop terminates

    // Step 5: Count leading zeros + 1
    bit = 1;
    count = 1;
    while((hash & bit) == 0) {
        count++;
        bit <<= 1;
    }
    // count = 1  → hash starts with 1xxxxxxx (probability 1/2)
    // count = 2  → hash starts with 01xxxxxx (probability 1/4)
    // count = 10 → hash starts with 0000000001... (probability 1/1024)

    *regp = (int) index;
    return count;
}
```

**The Probability Insight:**

```
Leading zeros → Probability → Implication
count = 1     → 1/2         → very common,  few unique elements
count = 2     → 1/4         → common
count = 5     → 1/32        → less common
count = 10    → 1/1024      → rare,  ~1024 unique elements seen
count = 20    → 1/1048576   → very rare, ~1M unique elements seen
```

---

**Step 4:** `MurmurHash64A()` — `src/hyperloglog.c` line 397

```c
uint64_t MurmurHash64A(const void *key, size_t len, unsigned int seed) {
    const uint64_t m = 0xc6a4a7935bd1e995; // magic multiplier for bit mixing
    const int r = 47;                        // optimal shift for 64-bit

    uint64_t h = seed ^ (len * m);           // initialize with seed XOR length

    // Process 8 bytes at a time (fast!)
    while(data != end) {
        k *= m;
        k ^= k >> r;   // avalanche: shift then XOR
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    // Handle remaining bytes (if input length not divisible by 8)
    switch(len & 7) {
    case 7: h ^= (uint64_t)data[6] << 48;
    // ... cases 6 through 1
    }

    // Final avalanche mixing
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}
```

The function produces a **uniformly distributed** 64-bit output — every bit has equal 50/50 probability of being 0 or 1. This uniform distribution is what makes HyperLogLog's probability math work correctly.

---

### 2.2 Register Update

After `hllPatLen()` returns an index and count:

```c
int hllDenseSet(uint8_t *registers, long index, uint8_t count) {
    uint8_t oldcount;
    HLL_DENSE_GET_REGISTER(oldcount, registers, index);

    if (count > oldcount) {
        HLL_DENSE_SET_REGISTER(registers, index, count);
        return 1;   // register updated → PFADD returns 1
    } else {
        return 0;   // no change → duplicate detected
    }
}
```

**Why duplicates don't affect count:** The same element always hashes to the same index with the same count. Since `count > oldcount` is never true for duplicates, the register is never updated. This is the source of HyperLogLog's idempotency.

---

### 2.3 PFCOUNT Execution Path

**Entry Point:** `pfcountCommand()` — `src/hyperloglog.c` line 1269

```c
void pfcountCommand(client *c) {

    // Case 1: Multiple keys → compute union cardinality
    if (c->argc > 2) {
        uint8_t max[HLL_HDR_SIZE + HLL_REGISTERS];
        memset(max, 0, sizeof(max));
        hdr->encoding = HLL_RAW;         // special internal encoding

        for (j = 1; j < c->argc; j++) {
            hllMerge(registers, o);      // MAX(register[i]) across all HLLs
        }
        addReplyLongLong(c, hllCount(hdr, NULL));
        return;
    }

    // Case 2: Single key → use cache or recompute
    o = lookupKeyRead(c->db, c->argv[1]);

    if (HLL_VALID_CACHE(hdr)) {
        // Cache valid → return instantly (O(1))
        card  = (uint64_t)hdr->card[0];
        card |= (uint64_t)hdr->card[1] << 8;
        // ... (reads 8-byte little-endian value from header)
        card |= (uint64_t)hdr->card[7] << 56;
    } else {
        // Cache invalid → recompute and cache result
        card = hllCount(hdr, &invalid);
        // save card back into hdr->card for next call
    }

    addReplyLongLong(c, card);
}
```

**Cache Lifecycle:**

```
PFADD myhll "apple"
      ↓ HLL_INVALIDATE_CACHE()    ← cache marked dirty

PFCOUNT myhll                     ← cache invalid
      ↓ hllCount()                ← expensive: reads 16,384 registers
      ↓ save result to hdr->card  ← cache now valid

PFCOUNT myhll (again)             ← cache valid
      ↓ return hdr->card instantly ← O(1), no recalculation ⚡

PFCOUNT myhll (1000 more times)   ← all return from cache instantly
```

---

### 2.4 The Estimation Formula — `hllCount()`

**Function:** `hllCount()` — `src/hyperloglog.c` line 1050

```c
uint64_t hllCount(struct hllhdr *hdr, int *invalid) {
    double m = HLL_REGISTERS;   // m = 16,384
    int reghisto[64] = {0};

    // Step 1: Build register histogram
    // reghisto[i] = number of registers with value i
    if (hdr->encoding == HLL_DENSE)
        hllDenseRegHisto(hdr->registers, reghisto);
    else if (hdr->encoding == HLL_SPARSE)
        hllSparseRegHisto(..., reghisto);
    else if (hdr->encoding == HLL_RAW)
        hllRawRegHisto(hdr->registers, reghisto);

    // Step 2: Apply improved HyperLogLog formula
    // Based on: Ertl (2017) arXiv:1702.01284
    double z = m * hllTau((m - reghisto[HLL_Q+1]) / (double)m);
    // hllTau() → correction for large cardinality (saturated registers)

    for (j = HLL_Q; j >= 1; --j) {
        z += reghisto[j];
        z *= 0.5;
    }
    // Core harmonic mean loop — weights each register by 2^(-value)

    z += m * hllSigma(reghisto[0] / (double)m);
    // hllSigma() → correction for small cardinality (many zero registers)

    // Step 3: Apply bias correction and return
    E = llroundl(HLL_ALPHA_INF * m * m / z);
    // HLL_ALPHA_INF = 0.7213475... (mathematically derived constant)
    // Final formula: E = α × m² / z
    return (uint64_t) E;
}
```

---

### 2.5 Complete End-to-End Flow Diagram

```
User types: PFADD myhll "apple"
                    │
                    ▼
         pfaddCommand()                    line 1229
         ├─ lookupKeyWrite()               find "myhll" in Redis DB
         ├─ createHLLObject()              if new → sparse HLL (18 bytes)
         └─ for each element:
                    │
                    ▼
              hllAdd()                     line 1088
              ├─ encoding == SPARSE? ──→  hllSparseAdd()
              └─ encoding == DENSE?  ──→  hllDenseAdd()
                                               │
                                               ▼
                                         hllPatLen()             line 452
                                         ├─ MurmurHash64A()      line 397
                                         │   └─ "apple" → 64-bit hash
                                         ├─ index = hash & HLL_P_MASK
                                         │   → first 14 bits → register #XXXX
                                         └─ count = leading zeros + 1
                                               │
                                               ▼
                                         hllDenseSet()
                                         ├─ count > oldcount? → update register
                                         │                     → return 1
                                         └─ count ≤ oldcount? → no change
                                                               → return 0
                    │
         HLL_INVALIDATE_CACHE()           mark cached count dirty
         addReply(1 or 0)                 send result to client

User types: PFCOUNT myhll
                    │
                    ▼
         pfcountCommand()                  line 1269
         ├─ argc > 2? ──→ hllMerge() all keys → hllCount()
         └─ single key:
                    │
                    ▼
              HLL_VALID_CACHE?
              ├─ YES → read hdr->card → return instantly ⚡
              └─ NO  → hllCount()                          line 1050
                            ├─ hllDenseRegHisto()          build histogram
                            ├─ hllTau()                    large cardinality fix
                            ├─ harmonic mean loop          core formula
                            ├─ hllSigma()                  small cardinality fix
                            └─ E = HLL_ALPHA_INF × m² / z → return estimate
                            │
                            └─ save result to hdr->card    update cache
```

---

## 3. Design Decisions

### Decision 1: MurmurHash64A as the Hash Function

**File:** `src/hyperloglog.c` line 397  
**Used at:** line 467 — `hash = MurmurHash64A(ele, elesize, 0xadc83b19ULL)`

**What problem it solves:**

HyperLogLog's mathematical accuracy depends entirely on uniform bit distribution across the 64-bit hash output. If certain elements cluster to the same register indices, the harmonic mean formula produces wildly inaccurate estimates. Every element must have an equal probability of mapping to any of the 16,384 registers.

**How it works:**

MurmurHash64A processes input 8 bytes at a time, applying a sequence of multiply-XOR-shift operations (the "avalanche effect") that ensures every input bit influences every output bit. The fixed seed `0xadc83b19ULL` ensures identical elements always hash identically across all Redis instances.

**Tradeoff introduced:**

MurmurHash64A is non-cryptographic — optimized for speed over security. It has a tiny theoretical collision probability. A cryptographic hash (SHA-256) would eliminate collisions entirely but run approximately 10x slower on every PFADD call. For cardinality estimation where 0.81% error is already inherent to the algorithm, the negligible collision risk is completely acceptable in exchange for the performance gain.

---

### Decision 2: Sparse → Dense Adaptive Encoding

**Files:** `src/hyperloglog.c` lines 586, 675 | `redis.conf` line 1992 | `src/config.c` line 3227

**The HLL Header Structure** (both encodings share this 16-byte header):

```
+------+---+-----+----------+
| HYLL | E | N/U | Cardin.  |
+------+---+-----+----------+
  4B    1B   3B      8B

HYLL    → magic bytes identifying this as an HLL object
E       → encoding: HLL_DENSE(0) or HLL_SPARSE(1)
N/U     → 3 unused/reserved bytes
Cardin. → 64-bit cached cardinality (MSB set = cache invalid)
```

**Sparse Encoding — Three Opcodes** (lines 365–386):

```
Bit pattern   Name    Size    Meaning
00xxxxxx  →  ZERO    1 byte  Next N registers are zero (N ≤ 64)
01xxxxxx  →  XZERO   2 bytes Next N registers are zero (N ≤ 16,384)
1vvvvvxx  →  VAL     1 byte  Next N registers all have value V (V ≤ 32, N ≤ 4)
```

Memory comparison:
```
Fresh HLL (0 elements):   [header 16B] + [XZERO:16384 = 2B] = 18 bytes total
After 3 elements:         [header 16B] + [XZERO][VAL][XZERO][VAL][XZERO] ≈ 26 bytes
Dense (always):           16B header + 16,384 × 6 bits = 12,304 bytes (≈ 12KB)
```

**Two Promotion Triggers** (one-way, irreversible):

```c
// Trigger 1: value too large for sparse (line 675)
if (count > HLL_SPARSE_VAL_MAX_VALUE) goto promote;
// VAL opcode only stores 5 bits → max value = 32
// Probability of this trigger: 1/2^32 (extremely rare)

// Trigger 2: size exceeds configured threshold
// server.hll_sparse_max_bytes = 3000 (default)
// Configured in redis.conf line 1992
// Runtime-changeable: CONFIG SET hll-sparse-max-bytes <value>
// Registered in config.c line 3227 with MODIFIABLE_CONFIG flag
```

**Promotion function:** `hllSparseToDense()` line 586 — allocates a fresh 12KB dense string, walks all sparse opcodes, sets non-zero registers in the dense array, then frees the old sparse data.

**What problem it solves:**

A system tracking unique visitors for 10 million web pages needs 10 million HLL counters. In sparse encoding, counters with few unique visitors use only 18–100 bytes each. Forcing 12KB dense for every HLL would consume 120GB for the same workload.

**Tradeoff introduced:**

Two completely separate code paths (sparse and dense) must be maintained throughout the codebase. Promotion is one-way — a dense HLL never returns to sparse even if usage drops. The sparse path also has higher per-operation CPU cost due to opcode decoding and potential buffer reallocation.

---

### Decision 3: Cardinality Result Cache in Header

**File:** `src/hyperloglog.c` — `pfaddCommand()` line 1229, `pfcountCommand()` line 1269

**Where implemented:**

```c
// The 8-byte Cardin. field in the header stores the cached result.
// MSB of the last byte set = cache invalid, clear = cache valid.

// Invalidated on every write — pfaddCommand() line ~1258:
HLL_INVALIDATE_CACHE(hdr);

// Checked on every read — pfcountCommand() line ~1317:
if (HLL_VALID_CACHE(hdr)) {
    card  = (uint64_t)hdr->card[0];
    card |= (uint64_t)hdr->card[1] << 8;
    card |= (uint64_t)hdr->card[2] << 16;
    card |= (uint64_t)hdr->card[3] << 24;
    card |= (uint64_t)hdr->card[4] << 32;
    card |= (uint64_t)hdr->card[5] << 40;
    card |= (uint64_t)hdr->card[6] << 48;
    card |= (uint64_t)hdr->card[7] << 56;
    // Return cached value instantly — no register scan needed
}
```

**What problem it solves:**

Computing `hllCount()` requires reading all 16,384 registers and applying the harmonic mean formula — a non-trivial operation at high call frequency. In production workloads such as a dashboard showing unique daily visitors, `PFCOUNT` may be called thousands of times per second while `PFADD` happens far less frequently. The cache converts repeated `PFCOUNT` calls from O(m) — where m=16,384 — to O(1).

**Tradeoff introduced:**

The cache is invalidated on every single `PFADD`, even if the element is a duplicate and no register actually changed. If `PFADD` and `PFCOUNT` strictly alternate at high frequency, the cache never provides benefit. The design is optimized specifically for read-heavy workloads and penalizes mixed write-read patterns.

---

## 4. Concept Mapping

### Concept 1: Storage — LSM Tree Analogy

| LSM Tree Concept | HyperLogLog Equivalent | Code Location |
|---|---|---|
| MemTable (small write buffer) | Sparse encoding (tiny, efficient) | lines 365–386 |
| SSTable (large, structured) | Dense encoding (fixed 12KB) | `HLL_DENSE_SIZE` |
| Compaction threshold | `hll-sparse-max-bytes = 3000` | redis.conf:1992 |
| One-way flush | `hllSparseToDense()` (irreversible) | line 586 |
| Bloom filter (approximate) | HLL estimate (probabilistic) | `hllCount()`:1050 |

Just as an LSM tree buffers writes in a small MemTable before flushing to a structured SSTable, HyperLogLog buffers small cardinalities in compact sparse encoding before promoting to the fixed-layout dense encoding. Both transitions are one-way and triggered by a configurable size threshold.

---

### Concept 2: Streaming / Ingestion

HyperLogLog is a textbook streaming algorithm satisfying all streaming constraints:

```
Streaming requirement          HyperLogLog behavior
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Single pass over data    →    hllAdd() processes each element once
Cannot store all data    →    element hashed → register updated → element discarded
O(1) memory              →    fixed 12KB regardless of stream length
Handles unbounded input  →    registers never overflow (max value = 63 in dense)
Supports merging         →    PFMERGE combines distributed stream counts
```

**Code evidence:** `hllAdd()` line 1088 — the element pointer is passed in, processed by `hllPatLen()`, and the element itself is never stored anywhere. Only the register value is updated. `MurmurHash64A()` at line 397 converts the element to a hash that is immediately used and discarded.

---

### Concept 3: Hash Partitioning

The 16,384 registers in HyperLogLog are functionally identical to hash partitions in a distributed system:

```
Hash Partitioning:                   HyperLogLog:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
partition_id = hash(key) % N    →    index = hash & HLL_P_MASK
N partitions                    →    16,384 registers
Each partition independent      →    each register tracks its own max
Balanced load by design         →    uniform hash ensures even distribution
```

**Code evidence** — `hllPatLen()` line 464:
```c
index = hash & HLL_P_MASK;
// HLL_P_MASK = 0x3FFF = binary 0000...0011111111111111
// Extracts first 14 bits → selects 1 of 2^14 = 16,384 registers
// HLL_P = 14 → log₂(16,384) = 14
```

The choice of 16,384 registers (2^14) is deliberate — it gives a standard error of `1.04 / √16384 = 0.81%`. More registers means lower error but higher memory cost.

---

### Concept 4: Fault Tolerance and Distributed Aggregation

PFMERGE enables a pattern of distributed fault-tolerant cardinality counting:

```
Architecture:
  Redis Node A (region East) → PFADD hll_east [user events]
  Redis Node B (region West) → PFADD hll_west [user events]
                   ↓
  PFMERGE total_users hll_east hll_west
                   ↓
  PFCOUNT total_users → accurate global unique user count
```

**Merge correctness** — `hllMerge()` line 1106:
```c
// For each of the 16,384 registers:
if (val > max[i]) max[i] = val;
// Taking MAX per register preserves the probabilistic property.
// MAX(register_A[i], register_B[i]) = the rarest element
// seen by EITHER node for that register partition.
// This is mathematically equivalent to having seen all elements
// on a single node — no accuracy is lost in the merge.
```

If one node fails and is rebuilt from scratch, its HLL simply contributes zeros to the merge — the surviving node's data is not corrupted. This makes the system resilient to partial node failure.

---

## 5. Experiments & Results

All experiments were run against Redis 7.2 compiled from source on Ubuntu 24, connecting via the Python `redis` client library.

### Experiment 1: Error Rate vs Cardinality

**Goal:** Verify the ~0.81% error guarantee holds across cardinalities from 10 to 100,000.

**Script:** `experiments/experiment1_error_rate.py`

**Results:**

| N (Actual) | HLL Estimate | Error % |
|---|---|---|
| 10 | 10 | 0.00% |
| 100 | 99 | 1.00% |
| 500 | 503 | 0.60% |
| 1,000 | 1,001 | 0.10% |
| 5,000 | 5,012 | 0.24% |
| 10,000 | 9,986 | 0.14% |
| 50,000 | 49,780 | 0.44% |
| 100,000 | 100,600 | 0.60% |

**Analysis:**

Maximum observed error = **1.00%** (at N=100, within normal statistical variance). All results remain near or below the theoretical 0.81% guarantee. Critically, error does **not increase** as N grows from 10 to 100,000 — confirming the O(1) accuracy property. At N=100,000 the error is still only 0.60%.

The small spike at N=100 is normal — with fewer unique elements, there is higher relative variance in which registers get populated. At larger N, the law of large numbers smooths out this variance across all 16,384 registers.

**Code connection:** Bias corrections applied in `hllCount()` via `hllSigma()` (small cardinality) and `hllTau()` (large cardinality) at lines 1075–1079 are responsible for keeping error bounded at both extremes.

---

### Experiment 2: Memory — HyperLogLog vs Exact SET

**Goal:** Quantify memory savings of HLL versus Redis SET (exact counting) at different cardinalities.

**Script:** `experiments/experiment2_memory.py`

**Results:**

| N Elements | HLL Memory | SET Memory | Ratio |
|---|---|---|---|
| 10 | 152 B | 184 B | 1.2x |
| 100 | 440 B | 1,336 B | 3.0x |
| 1,000 | 2,616 B | 48,304 B | 18.5x |
| 5,000 | 14,392 B | 298,416 B | 20.7x |
| 10,000 | 14,392 B | 596,720 B | 41.5x |
| 50,000 | 14,392 B | 2,786,544 B | **193.6x** |

**Analysis:**

Three distinct phases are visible in the HLL memory column:

**Phase 1 — Sparse (N=10 to N=1,000):** Memory grows from 152 to 2,616 bytes as more registers receive non-zero values and the sparse opcode sequence expands. Still far smaller than SET.

**Phase 2 — Promotion (between N=1,000 and N=5,000):** Sparse encoding exceeded `hll-sparse-max-bytes = 3000` (redis.conf line 1992), triggering `hllSparseToDense()` at line 586. Memory jumped to the fixed 14,392 bytes (12KB dense + Redis object overhead).

**Phase 3 — Dense plateau (N=5,000 to N=50,000):** HLL memory stays exactly 14,392 bytes regardless of how many more elements are added. This is the constant-memory guarantee. Meanwhile, SET memory continues growing linearly — reaching 2.7MB at N=50,000.

**At N=50,000:** HLL uses **193.6x less memory** than exact SET while maintaining 0.44% accuracy.

---

### Experiment 3: Skew and Duplicate Input Handling

**Goal:** Verify HLL handles duplicate-heavy and skewed input correctly (idempotency).

**Script:** `experiments/experiment3_skew.py`

**Results:**

```
Test 1: 'same_element' added 10,000 times
Expected: 1  |  HLL says: 1  ✅

Test 2: 10 unique elements, each repeated 1,000 times
Expected: 10  |  HLL says: 10  ✅

Test 3: Sequential unique additions
Added    PFCOUNT    PFADD returned
1        1          1
2        2          1
3        3          1
...
10       10         1
```

**Analysis:**

HyperLogLog is **perfectly idempotent** — adding the same element any number of times produces the same result as adding it once. This holds even under extreme skew (10,000 repetitions of one element).

The reason is rooted in `hllDenseSet()`: `MurmurHash64A("same_element")` always produces the same 64-bit hash → same register index → same leading zero count. Since `count > oldcount` is never satisfied for a duplicate, the register is never updated and `PFADD` returns 0.

**Real-world implication:** HyperLogLog is safe for event streams that may contain retransmissions, retries, or duplicate events — a common condition in at-least-once delivery systems.

---

### Experiment 4: Sparse Threshold Modification

**Goal:** Observe the effect of modifying `hll-sparse-max-bytes` at runtime on memory behavior.

**Script:** `experiments/experiment4_sparse_threshold.py`

**Method:** Changed threshold via `CONFIG SET hll-sparse-max-bytes <value>` (no Redis restart), then added 200 random elements and measured memory.

**Results:**

| Threshold (bytes) | Memory Used | Estimated Count | Encoding |
|---|---|---|---|
| 30 | 14,392 B | 201 | **Dense** |
| 100 | 14,392 B | 199 | **Dense** |
| 500 | 14,392 B | 200 | **Dense** |
| 1,000 | 824 B | 200 | **Sparse** |
| 3,000 | 824 B | 201 | **Sparse** |

**Analysis:**

A clear phase transition occurs between threshold=500 and threshold=1,000. For 200 random elements, the sparse representation requires approximately **824 bytes**. Any threshold below this forces immediate promotion to dense (12KB).

**Key findings:**
- **Encoding does not affect accuracy** — all thresholds produce ~200 count, confirming encoding is a pure storage optimization.
- **The runtime-configurable threshold** (registered with `MODIFIABLE_CONFIG` flag in config.c line 3227) allows operators to tune the memory/performance tradeoff without restarting Redis.
- **Threshold selection matters:** Setting threshold too low (e.g., 30) wastes ~17x more memory for small HLLs. Setting it high allows sparse encoding to persist longer and save significant memory in workloads with many small-cardinality HLLs.

---

## 6. Failure Analysis

### Question 1: What happens when data size increases significantly?

From Experiment 1, error remains ≤1.00% even at N=100,000. From Experiment 2, memory plateaus at exactly 14,392 bytes (fixed dense encoding) regardless of how many additional elements are added beyond the sparse threshold.

The system does **not degrade** under large input because:

- Dense encoding is a fixed-size structure: `HLL_DENSE_SIZE = 12,288 bytes` — it does not grow with cardinality.
- The estimation formula accuracy is mathematically bounded at ~0.81% standard error for any cardinality.
- `hllCount()` always reads exactly 16,384 registers regardless of how many unique elements were added.

**Code evidence:**
```c
#define HLL_REGISTERS (1<<HLL_P)   // 2^14 = 16,384, constant — never grows
#define HLL_DENSE_SIZE ...         // fixed 12,288 bytes always
```

**Potential concern at extreme scale (>10^15 elements):** The leading zero count is bounded at HLL_Q+1 = 51 bits. Elements that hash to 50+ leading zeros (probability ~1/2^50) would saturate registers permanently. The `hllTau()` correction handles a significant number of saturated registers, but at extreme astronomical scale, accuracy could degrade. This is not a practical concern for real-world workloads.

---

### Question 2: What assumptions does this system rely on?

**Assumption 1: Hash function produces uniform distribution**

The entire mathematical foundation of HyperLogLog relies on `MurmurHash64A` distributing elements uniformly across all 64 output bits. If the input data has patterns that correlate with the hash function's internal structure, register assignment could become skewed — certain registers would be updated disproportionately while others remain at zero, breaking the harmonic mean calculation.

*Evidence in code:* Fixed seed `0xadc83b19ULL` (line 467) was specifically chosen for its distribution properties. The multiply-XOR-shift avalanche mixing (lines 408–433) is designed to ensure high-entropy output even for low-entropy inputs like sequential integers.

**Assumption 2: Element independence**

The probabilistic model assumes each element's register assignment is independent of all others. Highly correlated inputs — such as elements that differ only in the last character — should still hash independently due to MurmurHash's avalanche effect. However, pathological inputs specifically crafted to exploit MurmurHash's structure could theoretically bias estimates.

**Assumption 3: Sparse register values stay below 32**

If the leading zero count for any element exceeds `HLL_SPARSE_VAL_MAX_VALUE = 32`, the sparse encoding cannot represent that value and forces an immediate promotion to dense (line 675). The probability of this occurring for any single element is 1/2^32 ≈ 0.00000002% — effectively impossible in practice, but the code handles it correctly.

**Assumption 4: The cardinality cache is correctly invalidated**

`HLL_INVALIDATE_CACHE()` must be called on every write that modifies an HLL. If any code path modifies registers without invalidating the cache, subsequent `PFCOUNT` calls would return stale results silently. A review of the codebase shows all modification paths (`pfaddCommand`, `pfmergeCommand`) correctly call `HLL_INVALIDATE_CACHE()`.

---

## 7. Key Insights

**Insight 1: Memory vs Accuracy is a tunable tradeoff**

The number of registers (16,384) determines both memory use and accuracy. The formula `error = 1.04 / √m` shows that doubling the registers halves the error but doubles the memory. Redis chose 16,384 as the engineering sweet spot: 12KB for 0.81% error — acceptable for virtually all cardinality estimation use cases.

**Insight 2: Encoding is completely transparent to the user**

`PFADD` and `PFCOUNT` produce identical results whether the internal encoding is sparse or dense. The promotion from sparse to dense happens automatically, invisibly, and irreversibly. Users never need to know which encoding is active — this is a clean abstraction boundary in the implementation.

**Insight 3: The cardinality cache is critical for production performance**

Without the `hdr->card` caching in the 16-byte header, every `PFCOUNT` call would scan all 16,384 registers and apply the harmonic mean formula. In a dashboard querying unique daily visitors once per second, the cache converts this from O(16,384 register reads) to O(8 byte reads) — a ~2,000x operation reduction.

**Insight 4: PFMERGE enables mathematically correct distributed architectures**

Taking `MAX(register_A[i], register_B[i])` across all registers during merge preserves the probabilistic guarantee. This means multiple Redis nodes can independently count cardinalities on different data subsets and merge results without any accuracy loss — a powerful property for distributed data pipelines.

**Insight 5: HyperLogLog is write-only and lossy by design**

Once elements are added, they cannot be retrieved. The structure stores only statistical summaries (register maximums), not data. This is not a limitation — it is the fundamental property that enables constant 12KB memory. Applications that need both counting and retrieval must maintain separate data structures.

**When to use HyperLogLog vs exact SET:**

| Scenario | Use HyperLogLog | Use SET |
|---|---|---|
| Need ~0.81% error acceptable | ✅ Yes | ❌ No |
| Need exact count | ❌ No | ✅ Yes |
| N > 10,000 unique elements | ✅ Yes (193x memory saving) | ⚠️ Memory intensive |
| N < 1,000 unique elements | ⚠️ Similar memory | ✅ Exact + retrievable |
| Need to retrieve elements | ❌ No | ✅ Yes |
| Distributed counting (merge) | ✅ PFMERGE | ❌ Complex |

---

## 8. GitHub Artifacts

```
BDE_Project/
├── README.md                          ← this report
├── redis/                             ← Redis 7.2 source (cloned)
│   └── src/hyperloglog.c              ← primary file studied
└── experiments/
    ├── experiment1_error_rate.py      ← error rate vs cardinality
    ├── experiment2_memory.py          ← HLL vs SET memory
    ├── experiment3_skew.py            ← duplicate/skew handling
    └── experiment4_sparse_threshold.py ← threshold modification
```

### Quick Reproduction

```bash
# Clone and build Redis
git clone https://github.com/redis/redis.git
cd redis && git checkout 7.2 && make

# Start server
src/redis-server &

# Install Python client
pip3 install redis

# Run all experiments
cd experiments
python3 experiment1_error_rate.py
python3 experiment2_memory.py
python3 experiment3_skew.py
python3 experiment4_sparse_threshold.py
```

---

## References

1. Flajolet, P., Fusy, É., Gandouet, O., & Meunier, F. (2007). *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm.* DMTCS Proceedings.

2. Heule, S., Nunkesser, M., & Hall, A. (2013). *HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm.* EDBT 2013.

3. Ertl, O. (2017). *New cardinality estimation algorithms for HyperLogLog sketches.* arXiv:1702.01284. *(Implemented in hllCount() at line 1050)*

4. Redis 7.2 Source Code — `src/hyperloglog.c` — https://github.com/redis/redis

5. Redis Configuration Reference — `redis.conf` line 1992 — `hll-sparse-max-bytes`
