import threading
import time
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import uuid

# ID нашего "поста"
post_id = uuid.UUID('550e8400-e29b-41d4-a716-446655440000')

def increment_task(consistency):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('counter_ks')
    
    query = f"UPDATE likes SET count_value = count_value + 1 WHERE post_id = {post_id}"
    statement = SimpleStatement(query, consistency_level=consistency)
    
    for _ in range(10000):
        session.execute(statement)
    cluster.shutdown()

def run_experiment(consistency_level, name):
    print(f"--- Тест: {name} ---")
    threads = []
    start_time = time.time()
    
    for i in range(10):
        t = threading.Thread(target=increment_task, args=(consistency_level,))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
    
    end_time = time.time()
    print(f"Время выполнения: {end_time - start_time:.2f} сек")


#run_experiment(ConsistencyLevel.ONE, "Consistency ONE")

run_experiment(ConsistencyLevel.QUORUM, "Consistency QUORUM")
