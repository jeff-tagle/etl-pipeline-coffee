from sqlalchemy import create_engine
from extract_transform import fetch_pokemon_data

def load_to_postgres():
    print("Fetching Pokémon data...")
    df = fetch_pokemon_data()
    print(f"Fetched {len(df)} Pokémon")

    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")
    print("Loading to PostgreSQL...")
    df.to_sql("pokemon", engine, if_exists="replace", index=False)
    print(f"✅ Loaded {len(df)} Pokémon to PostgreSQL!")

if __name__ == "__main__":
    load_to_postgres()