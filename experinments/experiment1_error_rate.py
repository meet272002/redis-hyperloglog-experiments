import redis
import random
import string

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

print("=" * 55)
print(f"{'N (Actual)':<15}{'HLL Estimate':<15}{'Error %':<15}")
print("=" * 55)

sizes = [10, 100, 500, 1000, 5000, 10000, 50000, 100000]

for n in sizes:
    r.delete("hll_exp1")
    actual = set()

    for _ in range(n):
        val = random_string()
        r.pfadd("hll_exp1", val)
        actual.add(val)

    estimated = r.pfcount("hll_exp1")
    true_count = len(actual)
    error = abs(estimated - true_count) / true_count * 100

    print(f"{true_count:<15}{estimated:<15}{error:.2f}%")

print("=" * 55)
print("Expected error rate: ~0.81% (HyperLogLog guarantee)")
