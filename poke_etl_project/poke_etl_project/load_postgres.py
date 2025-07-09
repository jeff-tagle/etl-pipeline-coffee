from extract_transform import fetch_pokemon_data, write_df_to_postgres

def load_to_postgres():
    print("Fetching Pokémon data...")
    df = fetch_pokemon_data()
    print(f"Fetched {len(df)} Pokémon")

    print("Loading to PostgreSQL...")
    write_df_to_postgres(df, "pokemon")

if __name__ == "__main__":
    load_to_postgres()
