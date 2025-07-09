import requests
import pandas as pd

def fetch_pokemon_data(limit=151):
    all_data = []

    for i in range(1, limit + 1):
        response = requests.get(f"https://pokeapi.co/api/v2/pokemon/{i}")
        if response.status_code != 200:
            print(f"Failed to fetch Pokémon {i}")
            continue

        data = response.json()
        pokemon = {
            "id": data["id"],
            "name": data["name"],
            "height": data["height"],
            "weight": data["weight"],
            "base_experience": data["base_experience"],
            "types": ", ".join([t["type"]["name"] for t in data["types"]]),
            "hp": next(stat["base_stat"] for stat in data["stats"] if stat["stat"]["name"] == "hp"),
            "attack": next(stat["base_stat"] for stat in data["stats"] if stat["stat"]["name"] == "attack"),
            "defense": next(stat["base_stat"] for stat in data["stats"] if stat["stat"]["name"] == "defense"),
        }
        all_data.append(pokemon)

    df = pd.DataFrame(all_data)
    return df
