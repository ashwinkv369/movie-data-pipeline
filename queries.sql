-- 1. Which movie has the highest average rating?
SELECT m.title, ROUND(AVG(r.rating), 2) AS avg_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
GROUP BY r.movie_id, m.title
ORDER BY avg_rating DESC
LIMIT 1;


-- 2. Top 5 movie genres with the highest average rating
SELECT g.genre_name, ROUND(AVG(r.rating), 2) AS avg_rating
FROM ratings r
JOIN movie_genres mg ON r.movie_id = mg.movie_id
JOIN genres g ON mg.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC
LIMIT 5;


-- 3. Director with the most movies in this dataset
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND director != 'N/A'
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;


-- 4. Average rating of movies released each year
SELECT m.release_year, ROUND(AVG(r.rating), 2) AS avg_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year ASC;