import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print("=" * 55)
print("TEST: HLL behaviour with duplicate/skewed input")
print("=" * 55)

# Test 1: All duplicates
r.delete("hll_skew")
for _ in range(10000):
    r.pfadd("hll_skew", "same_element")

result = r.pfcount("hll_skew")
print(f"\nTest 1: Added 'same_element' 10,000 times")
print(f"Expected: 1 | HLL says: {result}")

# Test 2: Skewed (10 unique, each repeated 1000 times)
r.delete("hll_skew2")
unique = [f"element_{i}" for i in range(10)]
for _ in range(1000):
    for e in unique:
        r.pfadd("hll_skew2", e)

result2 = r.pfcount("hll_skew2")
print(f"\nTest 2: 10 unique elements, each repeated 1000x")
print(f"Expected: 10 | HLL says: {result2}")

# Test 3: Gradual unique additions
r.delete("hll_skew3")
print(f"\nTest 3: Adding unique elements one by one")
print(f"{'Added':<10}{'PFCOUNT':<10}{'PFADD returned':<15}")
for i in range(1, 11):
    ret = r.pfadd("hll_skew3", f"item_{i}")
    cnt = r.pfcount("hll_skew3")
    print(f"{i:<10}{cnt:<10}{ret:<15}")

print("\nConclusion: HLL is idempotent — duplicates never")
print("change the count. Only new unique elements matter.")
