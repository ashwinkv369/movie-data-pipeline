import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Config
load_dotenv()
DB_USER = os.getenv("MYSQL_USER")
DB_PASS = os.getenv("MYSQL_PASSWORD")
DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_NAME = os.getenv("MYSQL_DB")

DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

# Create engine 
engine = create_engine(DB_URI)

# ----------------- Helpers -----------------
@st.cache_data(ttl=300)
def load_movies():
    q = """
    SELECT m.movie_id, m.title, m.release_year, m.director, m.plot, m.box_office,
           GROUP_CONCAT(g.genre_name SEPARATOR ', ') AS genres
    FROM movies m
    LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
    LEFT JOIN genres g ON mg.genre_id = g.genre_id
    GROUP BY m.movie_id, m.title, m.release_year, m.director, m.plot, m.box_office
    """
    return pd.read_sql(text(q), engine)

@st.cache_data(ttl=300)
def load_ratings():
    q = "SELECT user_id, movie_id, rating, rating_timestamp FROM ratings"
    return pd.read_sql(text(q), engine)

@st.cache_data(ttl=300)
def top_movies(n=10):
    q = """
    SELECT m.movie_id, m.title, ROUND(AVG(r.rating),2) AS avg_rating, COUNT(r.rating) AS num_ratings
    FROM ratings r
    JOIN movies m ON r.movie_id = m.movie_id
    GROUP BY m.movie_id, m.title
    HAVING num_ratings >= 5
    ORDER BY avg_rating DESC, num_ratings DESC
    LIMIT :limit
    """
    return pd.read_sql(text(q), engine, params={"limit": n})

@st.cache_data(ttl=300)
def top_genres(n=10):
    q = """
    SELECT g.genre_name, ROUND(AVG(r.rating),2) AS avg_rating, COUNT(r.rating) AS num_ratings
    FROM ratings r
    JOIN movie_genres mg ON r.movie_id = mg.movie_id
    JOIN genres g ON mg.genre_id = g.genre_id
    GROUP BY g.genre_name
    HAVING num_ratings >= 20
    ORDER BY avg_rating DESC
    LIMIT :limit
    """
    return pd.read_sql(text(q), engine, params={"limit": n})

# ----------------- Streamlit Layout -----------------
st.set_page_config(page_title="Movie Data Dashboard", layout="wide")
st.title("🎬 Movie Data Pipeline — Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
movies_df = load_movies()
ratings_df = load_ratings()

# unique genres (split the comma string)
all_genres = sorted({g.strip() for sub in movies_df["genres"].fillna("").str.split(",") for g in sub if g})
selected_genre = st.sidebar.selectbox("Genre (all)", options=["All"] + all_genres)

years = sorted(movies_df["release_year"].dropna().unique().tolist())
years_str = ["All"] + [str(int(y)) for y in years]
selected_year = st.sidebar.selectbox("Release year", options=years_str)

search_text = st.sidebar.text_input("Search title")

top_n = st.sidebar.slider("Top N movies", min_value=5, max_value=50, value=10)

# Apply filters
filtered = movies_df.copy()
if selected_genre != "All":
    filtered = filtered[filtered["genres"].str.contains(selected_genre, na=False)]
if selected_year != "All":
    filtered = filtered[filtered["release_year"] == int(selected_year)]
if search_text:
    filtered = filtered[filtered["title"].str.contains(search_text, case=False, na=False)]

# Main columns
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Top movies (by avg rating)")
    top_movies_df = top_movies(top_n)
    st.dataframe(top_movies_df, use_container_width=True)

    st.markdown("---")
    st.subheader("Filtered movies")
    st.dataframe(filtered[["movie_id", "title", "release_year", "director", "genres"]].reset_index(drop=True), height=400)

with col2:
    st.subheader("Top genres")
    tg = top_genres(10)
    st.table(tg)

    st.markdown("### Movie details")
    # Select a movie to view full details
    movie_choice = st.selectbox("Pick a movie id", options=filtered["movie_id"].head(200).tolist())
    if movie_choice:
        mrow = movies_df[movies_df["movie_id"] == movie_choice].iloc[0]
        st.markdown(f"**{mrow['title']}** ({int(mrow['release_year']) if pd.notna(mrow['release_year']) else ''})")
        st.markdown(f"**Director:** {mrow.get('director','')}")
        st.markdown(f"**Genres:** {mrow.get('genres','')}")
        st.markdown("**Plot:**")
        st.write(mrow.get("plot",""))

# Footer - show counts
st.markdown("---")
c1, c2, c3 = st.columns(3)
c1.metric("Movies in DB", int(movies_df.shape[0]))
c2.metric("Ratings rows", int(ratings_df.shape[0]))
c3.metric("Genres", len(all_genres))

st.write("Tip: Use the sidebar filters to narrow results.")
