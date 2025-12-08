from fastapi import FastAPI
import uvicorn
import threading

app = FastAPI()

counter = 0
lock = threading.Lock()

@app.get("/inc")
def inc():
    global counter
    with lock:
        counter += 1
        return {"status": "ok", "counter": counter}

@app.get("/count")
def get_count():
    return {"counter": counter}

if __name__ == "__main__":
    uvicorn.run("server_memory:app", host="0.0.0.0", port=8080, workers=4)
