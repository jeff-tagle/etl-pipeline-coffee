from dagster import asset
from .extract_transform import fetch_pokemon_data
from sqlalchemy import create_engine

@asset
def pokemon_data():
    df = fetch_pokemon_data()

    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")
    df.to_sql("pokemon", engine, if_exists="replace", index=False)
    
    print(f"✅ Loaded {len(df)} Pokémon to PostgreSQL!")
    return df