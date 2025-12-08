import requests
import time
from concurrent.futures import ThreadPoolExecutor

SERVER = "http://localhost:8080"

def send_inc(n):
    for _ in range(n):
        requests.get(f"{SERVER}/inc")

def run_test(clients, calls_per_client=10000):
    print(f"Running test: {clients} clients × {calls_per_client} calls")

    start = time.time()

    with ThreadPoolExecutor(max_workers=clients) as pool:
        for _ in range(clients):
            pool.submit(send_inc, calls_per_client)

    end = time.time()
    total_time = end - start

    resp = requests.get(f"{SERVER}/count").json()["counter"]

    print(f"Final count: {resp}")
    print(f"Total time: {total_time:.3f} seconds")
    print(f"Requests/sec: {resp / total_time:.2f}")
    print("-" * 50)
