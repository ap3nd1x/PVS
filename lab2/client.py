import hazelcast
import threading
import time

ITERATIONS = 2000
THREADS = 10

hz = hazelcast.HazelcastClient(
    cluster_name="lab-cluster",
    cluster_members=["localhost:5701", "localhost:5702", "localhost:5703"],
)

dist_map = hz.get_map("counter_map").blocking()
atomic = hz.cp_subsystem.get_atomic_long("counter_long").blocking()

def run_test(name, setup, worker):
    setup()

    threads = []
    start = time.time()

    for _ in range(THREADS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    duration = time.time() - start
    print(f"{name} RESULT:", dist_map.get("cnt") if "cnt" in dist_map.key_set() else atomic.get())
    print(f"{name} TIME:", duration)
    print("-" * 50)

def test_no_lock():
    def setup():
        dist_map.put("cnt", 0)

    def worker():
        for i in range(ITERATIONS):
            val = dist_map.get("cnt")
            dist_map.put("cnt", val + 1)

    run_test("NO LOCK", setup, worker)

def test_pessimistic():
    def setup():
        dist_map.put("cnt", 0)

    def worker():
        for i in range(ITERATIONS):
            dist_map.lock("cnt")
            val = dist_map.get("cnt")
            dist_map.put("cnt", val + 1)
            dist_map.unlock("cnt")

    run_test("PESSIMISTIC", setup, worker)

def test_optimistic():
    def setup():
        dist_map.put("cnt", 0)

    def worker():
        for _ in range(ITERATIONS):
            while True:
                old = dist_map.get("cnt")
                if dist_map.replace_if_same("cnt", old, old + 1):
                    break

    run_test("OPTIMISTIC", setup, worker)

def test_atomic_long():
    def setup():
        atomic.set(0)

    def worker():
        for _ in range(ITERATIONS):
            atomic.increment_and_get()

    threads = []
    start = time.time()

    for _ in range(THREADS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    duration = time.time() - start
    print("IATOMICLONG RESULT:", atomic.get())
    print("IATOMICLONG TIME:", duration)
    print("-" * 50)


test_no_lock()
test_pessimistic()
test_optimistic()
test_atomic_long()

hz.shutdown()
