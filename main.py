import os
from fastapi import FastAPI, Depends, HTTPException, Header
from pydantic import BaseModel
from typing import Optional

from database import get_conn, init_db

app = FastAPI()

def verify_key(x_api_key: Optional[str] = Header(None)):
    expected_key = os.getenv("API_KEY")
    if expected_key and x_api_key != expected_key:
        raise HTTPException(status_code=403, detail="Fel API-nyckel")

@app.on_event("startup")
def startup_event():
    init_db()

class NyVader(BaseModel):
    datum: str
    stad: str
    temperatur: float

@app.get("/")
def root():
    return {"message": "API is running"}

@app.get("/api/vader")
def get_vader():
    con = get_conn()
    rows = con.execute("SELECT datum, stad, temperatur FROM lake.vader ORDER BY datum").fetchall()
    con.close()
    return [{"datum": str(r[0]), "stad": r[1], "temperatur": r[2]} for r in rows]

@app.post("/api/vader", status_code=201, dependencies=[Depends(verify_key)])
def ny_vader(v: NyVader):
    con = get_conn()
    con.execute("INSERT INTO lake.vader VALUES (?, ?, ?)", [v.datum, v.stad, v.temperatur])
    con.close()
    return v

@app.delete("/api/vader/{datum}", dependencies=[Depends(verify_key)])
def radera_vader(datum: str):
    con = get_conn()
    con.execute("DELETE FROM lake.vader WHERE datum = ?", [datum])
    con.close()
    return {"deleted": datum}