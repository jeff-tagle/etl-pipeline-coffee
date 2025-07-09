from dagster import asset
from tqdm import tqdm
import requests
import pandas as pd
from .extract_transform import write_df_to_postgres, get_all_pokemon_urls

@asset
def pokemon_species():
    records = []
    urls = get_all_pokemon_urls()

    for url in urls:
        res = requests.get(url).json()
        types = [t["type"]["name"] for t in res["types"]]
        stats = {s["stat"]["name"]: s["base_stat"] for s in res["stats"]}

        records.append({
            "pokemon_id": res["id"],
            "name": res["name"],
            "types": ",".join(types),
            "base_experience": res["base_experience"],
            "height": res["height"],
            "weight": res["weight"],
            **stats
        })

    df = pd.DataFrame(records)
    write_df_to_postgres(df, "pokemon_species")
    return df

@asset
def pokemon_moves():
    move_rows = []
    urls = get_all_pokemon_urls()
    for url in urls:
        res = requests.get(url).json()
        for move in res["moves"]:
            move_name = move["move"]["name"]
            for detail in move["version_group_details"]:
                move_rows.append({
                    "pokemon_id": res["id"],
                    "name": res["name"],
                    "move_name": move_name,
                    "version_group": detail["version_group"]["name"],
                    "learn_method": detail["move_learn_method"]["name"],
                    "level_learned_at": detail["level_learned_at"]
                })
    df = pd.DataFrame(move_rows)
    write_df_to_postgres(df, "pokemon_moves")
    return df

@asset
def pokemon_game_indices():
    rows = []
    urls = get_all_pokemon_urls()
    for url in urls:
        res = requests.get(url).json()
        for gi in res["game_indices"]:
            rows.append({
                "pokemon_id": res["id"],
                "name": res["name"],
                "version": gi["version"]["name"],
                "game_index": gi["game_index"]
            })
    df = pd.DataFrame(rows)
    write_df_to_postgres(df, "pokemon_game_indices")
    return df

@asset
def pokemon_held_items():
    rows = []
    urls = get_all_pokemon_urls()
    for url in urls:
        res = requests.get(url).json()
        for item in res.get("held_items", []):
            item_name = item["item"]["name"]
            for version_detail in item["version_details"]:
                rows.append({
                    "pokemon_id": res["id"],
                    "name": res["name"],
                    "item_name": item_name,
                    "version": version_detail["version"]["name"]
                })
    df = pd.DataFrame(rows)
    write_df_to_postgres(df, "pokemon_held_items")
    return df

@asset
def fire_type_pokemon(pokemon_species):  # Still depends on species asset
    df = pokemon_species
    fire_df = df[df["types"].str.contains("fire", case=False, na=False)].copy()
    write_df_to_postgres(fire_df, "fire_type_pokemon")
    return fire_df