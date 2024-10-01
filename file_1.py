import requests
import time
import pandas as pd
import json
#had a hard time getting my API key, so I've replaced it with 'YOUR_KEY'
# Query NYT API
query_url = "https://api.nytimes.com/svc/movies/v2/reviews/search.json?api-key=YOUR_KEY"
reviews_list = []

for page in range(20):
    try:
        full_url = f"{query_url}&page={page}"
        response = requests.get(full_url)
        reviews = response.json()
        
        for review in reviews["response"]["docs"]:
            reviews_list.append(review)
        
        print(f"Page {page} completed.")
        time.sleep(12)  # 12-second interval between queries
        
    except Exception as e:
        print(f"Error on page {page}: {e}")
        break

# Preview first 5 results
print(json.dumps(reviews_list[:5], indent=4))

# Convert to DataFrame
df = pd.json_normalize(reviews_list)
df["title"] = df["headline.main"]

# Function to extract keywords
def extract_keywords(keywords):
    return ', '.join([keyword['value'] for keyword in keywords]) if keywords else ''

df["keywords"] = df["keywords"].apply(extract_keywords)
titles = df["title"].to_list()

# Access TMDB API
tmdb_movies_list = []
request_counter = 1

for title in titles:
    try:
        # Make the API request to TMDB (replace with actual API request)
        request_counter += 1
        if request_counter % 50 == 0:
            time.sleep(1)
        
        tmdb_data = requests.get(f"https://api.themoviedb.org/3/search/movie?query={title}&api_key=YOUR_KEY").json()
        movie_id = tmdb_data['results'][0]['id']
        
        movie_details = requests.get(f"https://api.themoviedb.org/3/movie/{movie_id}?api_key=YOUR_KEY").json()
        
        genres = [g['name'] for g in movie_details['genres']]
        spoken_languages = [lang['english_name'] for lang in movie_details['spoken_languages']]
        production_countries = [country['name'] for country in movie_details['production_countries']]
        
        movie_info = {
            'title': movie_details['title'],
            'genres': genres,
            'spoken_languages': spoken_languages,
            'production_countries': production_countries,
            # Add other required fields here
        }
        
        tmdb_movies_list.append(movie_info)
        print(f"Movie {movie_details['title']} found.")

    except Exception as e:
        print(f"Movie not found for title: {title}")

# Preview TMDB results
print(json.dumps(tmdb_movies_list[:5], indent=4))

# Convert to DataFrame
tmdb_df = pd.DataFrame(tmdb_movies_list)

# Part 3: Merge and Clean Data
merged_df = pd.merge(df, tmdb_df, on='title')

columns_to_fix = ['genres', 'spoken_languages', 'production_countries']
characters_to_remove = ['[', ']', "'"]

for column in columns_to_fix:
    merged_df[column] = merged_df[column].astype(str)
    for char in characters_to_remove:
        merged_df[column] = merged_df[column].str.replace(char, "")

print(merged_df.head())

# Drop unnecessary columns and duplicates
merged_df.drop(columns=['byline.person'], inplace=True)
merged_df.drop_duplicates(inplace=True)
merged_df.reset_index(drop=True, inplace=True)

# Export to CSV
merged_df.to_csv('merged_movie_data.csv', index=False)