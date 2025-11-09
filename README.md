# 🎬 Movie Data ETL Pipeline

A simple ETL pipeline that loads MovieLens data into **MySQL**, enriches movie info using the **OMDb API**, and keeps the database clean and duplicate-free. The database schema consists of 4 tables - movies, ratings, genres and movie_genres to avoid duplicate/ repeating entries. A steamlit dashboard has been created for better visualization and enabling interactivity.

---

## 🛠 Tech Used
- Python (Pandas, Requests, SQLAlchemy)
- MySQL
- OMDb API
- SQL
- Streamlit (Interactive dashboard)

---

## 🗂 Database Tables
- `movies`
- `genres`
- `movie_genres`
- `ratings`

---

##  Setup & Run

### 1️⃣ Clone the repo
```sh
git clone https://github.com/ashwinkv369/movie-data-pipeline.git
cd movie-data-pipeline
```
### 2️⃣ Install dependencies
```sh
pip install -r requirements.txt
```
(If requirements.txt does not exist, install manually)
```sh
pip install pandas requests sqlalchemy pymysql python-dotenv streamlit
```

### 3️⃣ Create .env file in project root
```sh
OMDB_API_KEY=your_api_key_here
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_HOST=localhost
MYSQL_DB=movies_db
```
### 4️⃣ Set up MySQL Database

Run in MySQL Workbench or SQLTools:
```sh
CREATE DATABASE IF NOT EXISTS movies_db;
USE movies_db;
source schema.sql;
```
### 5️⃣ Run the ETL Pipeline
```sh
python etl.py
```

Run the analytics queries
```sh
source queries.sql;
```

### 6️⃣ Launch the Streamlit Dashboard
```sh
streamlit run streamlit_app.py
```

### ✅ Pipeline Summary

- Loads MovieLens CSV data
- Fetches extra movie info from OMDb (director, plot, box office)
- Cleans & transforms the data
- Normalizes genres into a separate table
- Stores in MySQL with no duplicates
- Can be safely re-run (idempotent)
- Visualizes insights in a Streamlit dashboard

## Design Choices & Assumptions

| Decision | Reason |
|---|---|
| Normalized schema (genres split into mapping table) | Avoids duplication and supports scalable queries |
| Idempotent inserts & safe re-runs | Ensures ETL can run repeatedly without bad data |
| Environment variables via `.env` | Protects credentials and API key |
| Streamlit dashboard added | Helps validate pipeline output and enhances storytelling |
| Batch API fetching with limits | Prevents rate limits and long run times |

## Challenges & How They Were Solved

| Challenge | Solution |
|---|---|
| OMDb title mismatches | Cleaned titles by removing years before API lookup |
| API failures / missing movies | Handled gracefully with fallback null values |
| Slow API calls | Limited requests, added delays, and made enrichment optional |
| Duplicate genre entries | Split into `genres` + `movie_genres` mapping table |
| Streamlit MySQL caching errors | Removed caching on DB engine, cached only query results |
| Git push conflicts | Used `git pull --rebase` to sync local and remote history |


### Author
Ashwin K V


