import os
import time
import re
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ------------------ LOAD ENV ------------------ #
load_dotenv()
API_KEY = os.getenv("OMDB_API_KEY")
DB_USER = os.getenv("MYSQL_USER")
DB_PASS = os.getenv("MYSQL_PASSWORD")
DB_HOST = os.getenv("MYSQL_HOST")
DB_NAME = os.getenv("MYSQL_DB")

# LIMIT CONFIG
MOVIE_LIMIT = 900   # how many movies to insert into DB
API_SLEEP = 0.15     # delay between API calls 


engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

with engine.connect() as conn:
    print("✅ Database Connected:", conn.execute(text("SELECT 1")).fetchone())

# ------------------ HELPERS ---------------------

def extract_year(title):
    match = re.search(r"\((\d{4})\)$", title)
    return int(match.group(1)) if match else None

def clean_title(title):
    return re.sub(r"\s\(\d{4}\)$", "", title)

def fetch_omdb(title):
    try:
        url = f"http://www.omdbapi.com/?apikey={API_KEY}&t={title}"
        r = requests.get(url, timeout=5).json()
        if r.get("Response") == "True":
            return r.get("Director"), r.get("Plot"), r.get("BoxOffice")
    except:
        pass
    return None, None, None

# EXTRACT

def extract():
    movies  = pd.read_csv("Data/movies.csv")
    ratings = pd.read_csv("Data/ratings.csv")
    print("✅ CSV Loaded")
    return movies, ratings

# TRANSFORM 

def transform(movies, ratings):
    print("🔧 Transforming data...")

    movies = movies.head(MOVIE_LIMIT).copy()
    movies["release_year"] = movies["title"].apply(extract_year)
    movies["clean_title"]  = movies["title"].apply(clean_title)

    # Fetch OMDB details
    dirs, plots, boxes = [], [], []
    for title in movies["clean_title"]:
        d, p, b = fetch_omdb(title)
        dirs.append(d)
        plots.append(p)
        boxes.append(b)
        time.sleep(API_SLEEP)

    movies["director"]  = dirs
    movies["plot"]      = plots
    movies["box_office"] = boxes

    # Genre processing
    movies["genres"] = movies["genres"].str.split("|")

    # Ratings timestamp
    ratings["rating_timestamp"] = pd.to_datetime(ratings["timestamp"], unit="s")

    return movies, ratings

# LOAD

def load(movies, ratings):
    print("Loading into MySQL...")

    # Insert movies
    movie_df = movies[["movieId", "clean_title", "release_year", "director", "plot", "box_office"]]
    movie_df.columns = ["movie_id","title","release_year","director","plot","box_office"]

    with engine.begin() as conn:
        for _, row in movie_df.iterrows():
            conn.execute(text("""
                INSERT IGNORE INTO movies (movie_id, title, release_year, director, plot, box_office)
                VALUES (:movie_id, :title, :release_year, :director, :plot, :box_office)
            """), row.to_dict())

    # Insert genres
    all_genres = sorted(set(g for sub in movies["genres"] for g in sub))
    with engine.begin() as conn:
        for g in all_genres:
            conn.execute(text("INSERT IGNORE INTO genres (genre_name) VALUES (:g)"), {"g": g})

    # Map movie_genres
    with engine.begin() as conn:
        for _, row in movies.iterrows():
            mid = row["movieId"]
            for g in row["genres"]:
                conn.execute(text("""
                    INSERT IGNORE INTO movie_genres(movie_id, genre_id)
                    SELECT :m, genre_id FROM genres WHERE genre_name = :g
                """), {"m": mid, "g": g})

    # Insert ratings
    ratings_df = ratings[["userId", "movieId", "rating", "rating_timestamp"]]
    ratings_df.columns = ["user_id","movie_id","rating","rating_timestamp"]

    with engine.begin() as conn:
        for _, row in ratings_df.iterrows():
            conn.execute(text("""
                INSERT IGNORE INTO ratings(user_id, movie_id, rating, rating_timestamp)
                VALUES (:user_id, :movie_id, :rating, :rating_timestamp)
            """), row.to_dict())

    print("✅ ETL LOAD COMPLETE!")

# MAIN

def main():
    m, r = extract()
    m, r = transform(m, r)
    load(m, r)

if __name__ == "__main__":
    main()