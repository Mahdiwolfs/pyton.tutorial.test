import duckdb
import os

# Miljövariabler för Postgres
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "ducklake")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "duck")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Miljövariabler för S3/MinIO
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_KEY_ID   = os.getenv("S3_KEY_ID",   "minioadmin")
S3_SECRET   = os.getenv("S3_SECRET",   "minioadmin")
S3_BUCKET   = os.getenv("S3_BUCKET",   "ducklake")

def get_conn():
    """Skapar en koppling till DuckDB och ansluter till Postgres/S3."""
    con = duckdb.connect()
    
    # Ladda nödvändiga tillägg
    con.execute("INSTALL postgres; LOAD postgres;")
    # Om 'ducklake' är ett eget tillägg krävs det att det är installerat
    try:
        con.execute("LOAD ducklake")
    except Exception:
        print("Varning: Kunde inte ladda ducklake-tillägget.")

    # Skapa SECRET för Postgres
    con.execute(f"""
        CREATE OR REPLACE SECRET (
            TYPE postgres,
            HOST '{POSTGRES_HOST}',
            PORT 5432,
            DATABASE '{POSTGRES_DB}',
            USER '{POSTGRES_USER}',
            PASSWORD '{POSTGRES_PASSWORD}'
        )
    """)

    # Hantera S3-koppling om endpoint finns
    if S3_ENDPOINT:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            CREATE OR REPLACE SECRET (
                TYPE s3,
                KEY_ID '{S3_KEY_ID}',
                SECRET '{S3_SECRET}',
                ENDPOINT '{S3_ENDPOINT}',
                URL_STYLE 'path',
                USE_SSL false
            )
        """)
        data_path = f"s3://{S3_BUCKET}/"
    else:
        data_path = "./data/lake/"

    # Anslut till Postgres-databasen som ett schema i DuckDB
    con.execute(f"""
        ATTACH 'dbname={POSTGRES_DB}'
        AS lake (TYPE postgres, DATA_PATH '{data_path}')
    """)
    return con

# VIKTIGT: init_db måste ligga längst ut till vänster (utanför get_conn)
# ... (kod ovanför är bra fram till ATTACH)

    # Anslut till Postgres-databasen. 
    # Notera: Vi tar bort DATA_PATH härifrån eftersom det inte används för Postgres.
    con.execute(f"ATTACH 'dbname={POSTGRES_DB}' AS lake (TYPE POSTGRES)")

    # Om du vill använda din S3-path för att t.ex. exportera data senare, 
    # kan du spara den i en variabel eller sätta en DuckDB-inställning, 
    # men den ska inte vara med i ATTACH-kommandot ovan.
    
    return con