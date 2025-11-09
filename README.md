# 🎬 Movie Data ETL Pipeline

A simple ETL pipeline that loads MovieLens data into **MySQL**, enriches movie info using the **OMDb API**, and keeps the database clean and duplicate-free.

---

## 🛠 Tech Used
- Python (Pandas, Requests, SQLAlchemy)
- MySQL
- OMDb API
- SQL

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
pip install pandas requests sqlalchemy pymysql python-dotenv
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

### ✅ Pipeline Summary

- Loads MovieLens CSV data
- Fetches extra movie info from OMDb (director, plot, box office)
- Cleans & transforms the data
- Stores in MySQL with no duplicates
- Can be safely re-run (idempotent)

### Author
Ashwin K V


