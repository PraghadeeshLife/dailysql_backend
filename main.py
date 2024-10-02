from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncpg
import os

app = FastAPI()

# CORS Configuration
origins = [
    "http://localhost:8080",  # Adjust this to your Flutter web URL
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# PostgreSQL database connection parameters
DATABASE_URL = os.getenv("DATABASE_URL")

# Initialize a connection pool (async)
async def connect_to_db():
    try:
        return await asyncpg.create_pool(DATABASE_URL)
    except Exception as e:
        print("Error connecting to the database", e)
        return None

# SQL Query Input Schema
class QueryRequest(BaseModel):
    query: str

# Establish connection pool on startup
@app.on_event("startup")
async def startup():
    app.state.pool = await connect_to_db()

# Close connection pool on shutdown
@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()

# Route to handle SQL query execution
@app.post("/execute-query")
async def execute_query(request: QueryRequest):
    pool = app.state.pool
    query = request.query
    
    try:
        async with pool.acquire() as connection:
            result = await connection.fetch(query)
            return {"status": "success", "result": [dict(record) for record in result]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
