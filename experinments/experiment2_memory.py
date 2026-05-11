import redis
import random
import string

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

print("=" * 65)
print(f"{'N Elements':<15}{'HLL Memory':<20}{'SET Memory':<20}{'Ratio':<10}")
print("=" * 65)

sizes = [10, 100, 1000, 5000, 10000, 50000]

for n in sizes:
    r.delete("hll_mem")
    r.delete("set_mem")

    elements = [random_string() for _ in range(n)]

    # Add to HLL
    for e in elements:
        r.pfadd("hll_mem", e)

    # Add to SET
    for e in elements:
        r.sadd("set_mem", e)

    hll_mem = r.memory_usage("hll_mem")
    set_mem = r.memory_usage("set_mem")
    ratio   = set_mem / hll_mem

    print(f"{n:<15}{hll_mem:<20}{set_mem:<20}{ratio:.1f}x")

print("=" * 65)
print("HLL uses fixed ~12KB regardless of input size")
