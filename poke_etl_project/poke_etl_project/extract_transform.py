from sqlalchemy import create_engine
import requests

def get_all_pokemon_urls():
    all_urls = []
    offset = 0
    limit = 100  # PokéAPI paginates with ?offset=...&limit=...

    while True:
        url = f"https://pokeapi.co/api/v2/pokemon?offset={offset}&limit={limit}"
        res = requests.get(url).json()
        results = res.get("results", [])
        if not results:
            break
        all_urls.extend([r["url"] for r in results])
        if not res.get("next"):
            break
        offset += limit

    return all_urls

def get_pg_engine():
    return create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")

def write_df_to_postgres(df, table_name):
    engine = get_pg_engine()
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"✅ Loaded {len(df)} rows to table '{table_name}'")