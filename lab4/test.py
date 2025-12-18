from pymongo import MongoClient
import threading, time

CONN_STR = "mongodb://root:password@localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0&authSource=admin"

client = MongoClient(CONN_STR)
db = client["lab4"]
col = db["counter"]

N_THREADS = 10
ITER = 10000

def worker(write_concern):
    local_client = MongoClient(CONN_STR, w=write_concern)
    c = local_client["lab4"]["counter"]
    for _ in range(ITER):
        c.find_one_and_update(
            {"_id": 1},
            {"$inc": {"value": 1}}
        )

def reset_counter():
    col.update_one({"_id": 1}, {"$set": {"value": 0}}, upsert=True)

def run_test(wc):
    reset_counter()

    threads = []
    start = time.time()

    for _ in range(N_THREADS):
        t = threading.Thread(target=worker, args=(wc,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    end = time.time()

    print(f"\n--- writeConcern={wc} ---")
    print("Time:", end - start)
    print("Final value:", col.find_one({"_id": 1})["value"])
    print("--------------------------------")

run_test(1)
run_test("majority")
