import redis
import random
import string

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters, k=length))

print("=" * 65)
print("TEST: Effect of hll-sparse-max-bytes on memory")
print("=" * 65)

thresholds = [30, 100, 500, 1000, 3000]

for threshold in thresholds:
    # Set threshold at runtime (no restart needed!)
    r.config_set("hll-sparse-max-bytes", threshold)

    r.delete("hll_thresh")

    # Add 200 random elements
    for _ in range(200):
        r.pfadd("hll_thresh", random_string())

    mem   = r.memory_usage("hll_thresh")
    count = r.pfcount("hll_thresh")

    print(f"Threshold={threshold:<6} | "
          f"Memory={mem:<8} bytes | "
          f"Estimated count={count}")

# Reset to default
r.config_set("hll-sparse-max-bytes", 3000)
print("\nReset hll-sparse-max-bytes back to 3000 (default)")
print("Low threshold → forces dense sooner → more memory used")
print("High threshold → stays sparse longer → less memory used")
