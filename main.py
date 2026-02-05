from fastapi import FastAPI, HTTPException, Query
import psycopg2
import os
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any

# Load environment variables
load_dotenv()

app = FastAPI()

# Database configuration
DB_HOST = os.getenv("DB_HOST", "172.16.10.11")
DB_NAME = os.getenv("DB_NAME", "n8n")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

@app.get("/top_songs", response_model=List[Dict[str, Any]])
def get_top_songs(limit: int = 200000):
    query = """
    SELECT 
        t.metadata ->> 'songId' AS song_id,
        t.metadata ->> 'songName' AS ten_bai_hat,
        COUNT(*) AS so_luong_chon
    FROM 
        public.tracking AS t
    WHERE 
        t."event" = 'event_play_song'
        AND (t.metadata ->> 'songId') !~ '^[0-9]+$' -- Chỉ lấy ID chứa chữ/ký tự, bỏ ID toàn số
    GROUP BY 
        t.metadata ->> 'songId', 
        t.metadata ->> 'songName'
    ORDER BY 
        so_luong_chon DESC
    LIMIT %s;
    """
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        
        # Convert to list of dicts
        result = []
        for row in rows:
            result.append({
                "song_id": row[0],
                "ten_bai_hat": row[1],
                "so_luong_chon": row[2]
            })
            
        return result
        
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
