from sqlalchemy import create_engine, text
import pandas as pd

# Connect to PostgreSQL running in Docker
engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")

with engine.connect() as conn:
    # Use `text()` for raw SQL queries
    conn.execute(text("CREATE TABLE IF NOT EXISTS hello (id SERIAL PRIMARY KEY, name TEXT);"))
    conn.execute(text("INSERT INTO hello (name) VALUES ('Pikachu'), ('Bulbasaur');"))

    # Pandas can still read SQL directly
    result = pd.read_sql("SELECT * FROM hello", conn)
    print(result)
