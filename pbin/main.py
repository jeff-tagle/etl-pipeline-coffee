import requests

url = "https://api.sampleapis.com/beers/ale"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    for beer in data:
        rating = beer.get('rating')
        if isinstance(rating, dict) and 'average' in rating:
            average = rating['average']
        else:
            average = 'N/A'  # or None, or any placeholder you like
        print(f"{beer['name']} - {beer['price']} - {average}")
else:
    print(f"Error: {response.text}")