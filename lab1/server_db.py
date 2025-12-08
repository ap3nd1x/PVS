from fastapi import FastAPI
import uvicorn
import psycopg2
import threading

app = FastAPI()
lock = threading.Lock()

conn = psycopg2.connect(
    dbname="counterdb",
    user="counteruser",
    password="pass123",
    host="localhost",
    port=5432
)
conn.autocommit = True

@app.get("/inc")
def inc():
    with lock:
        cur = conn.cursor()
        cur.execute("UPDATE counter SET value = value + 1 WHERE id = 1;")
        cur.close()
    return {"status": "ok"}

@app.get("/count")
def get_count():
    cur = conn.cursor()
    cur.execute("SELECT value FROM counter WHERE id = 1;")
    result = cur.fetchone()[0]
    cur.close()
    return {"counter": result}

if __name__ == "__main__":
    uvicorn.run("server_db:app", host="0.0.0.0", port=8080, workers=4)
