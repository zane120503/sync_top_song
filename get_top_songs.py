import os
import psycopg2
import csv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST", "172.16.10.11")
DB_NAME = os.getenv("DB_NAME", "n8n")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_top_songs():
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
    LIMIT 200000;
    """

    conn = None
    try:
        # Connect to the database
        print(f"Connecting to database {DB_NAME} at {DB_HOST}...")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        
        cur = conn.cursor()
        print("Executing query...")
        cur.execute(query)
        
        rows = cur.fetchall()
        print(f"Retrieved {len(rows)} rows.")

        # Write to CSV
        output_file = 'top_songs.csv'
        print(f"Writing results to {output_file}...")
        
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['song_id', 'ten_bai_hat', 'so_luong_chon'])
            # Write data
            writer.writerows(rows)
            
        print("Done!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    get_top_songs()
