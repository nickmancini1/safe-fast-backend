from fastapi import FastAPI

app = FastAPI(title="SAFE-FAST Backend", version="0.1.0")


@app.get("/")
def root():
    return {"status": "ok", "service": "safe-fast-backend"}


@app.get("/health")
def health():
    return {"ok": True}
