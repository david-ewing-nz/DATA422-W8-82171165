# conditionally install packages
if(!require(RPostgres)) {
  install.packages("RPostgres")
}


# packages
library(DBI)
library(RPostgres)

#
#-------------------------------------------------------------------------------
# DELIVERABLE 0: Database Connection
#  environment variables (CONFIRM in environment values)
#
dbname <- Sys.getenv("PG_DBNAME", "Jeff")
host <- Sys.getenv("PG_HOST", "mathmads.canterbury.ac.nz")
port <- Sys.getenv("PG_PORT", "8909")
user <- Sys.getenv("PG_USR", "student_data422")
password <- Sys.getenv("PG_PASS") ## NO DEFAULT for security purposes. 


#
# connection object via environment variables (.Fenviron)
#
con <- dbConnect(
  Postgres(),
  dbname = dbname,
  host = host,
  port = port,
  user = user,
  password = password
)

#
# confirm connection status
#
if (!dbIsValid(con)) {
  stop("FAILED connection to PostgreSQL database.")
} else {
  print("SUCCESSFUL connection to PostgreSQL database.")
}

#
#-------------------------------------------------------------------------------
# DELIVERABLE 1.1: List all tables in the connected database
#
tables <- dbListTables(con)
print(tables)

#
#-------------------------------------------------------------------------------
# DELIVERABLE 1.2: List all the fields in a table
#
fields <- dbListFields(con, "rental")
print(fields)

#
#-------------------------------------------------------------------------------
# DELIVERABLE 1.3: Pull 10 Rows from the 'rental' table as an example
# SELECT:      SQL to 'pull some data' 
# rental_id:   Unique ID.
# rental_date: The date of  rental occurred.
#inventory_id: ID of the rented item.
#customer_id:  ID of the customer who rented the item.
#LIMIT 10:     clause to limit data
#
query <- "SELECT rental_id, rental_date, inventory_id, customer_id FROM rental LIMIT 10"
data <- dbGetQuery(con, query)
print(data)

#
#-------------------------------------------------------------------------------
# DELIVERABLE 1.4: Pull the total number of rentals
#
query <- "SELECT COUNT(*) FROM rental"
dbGetQuery(con, query)
count <- dbGetQuery(con, query)
print(count)


#-------------------------------------------------------------------------------
# DELIVERABLE 2: Perform a JOIN between 'rental' and 'customer'  to get customer names
# INNER JOIN (SQL default):
# JOIN customer ON rental.customer_id = customer.customer_id 
join_query <- "
  SELECT rental.rental_id, rental.rental_date, customer.first_name, customer.last_name
  FROM rental
  JOIN customer ON rental.customer_id = customer.customer_id
  LIMIT 10
"
joined_data <- dbGetQuery(con, join_query)
print(joined_data)

#
#-------------------------------------------------------------------------------
# DELIVERABLE 3: SQL Query and Line Graph of Daily Income
#
con <- dbConnect(
  Postgres(),
  dbname = dbname,
  host = host,
  port = port,
  user = user,
  password = password
)


# SQL query to get total daily revenue
query <- "
SELECT 
    DATE(rental.rental_date) AS rental_date, 
    SUM(payment.amount) AS total_revenue
FROM rental
JOIN payment ON rental.rental_id = payment.rental_id
GROUP BY DATE(rental.rental_date)
ORDER BY rental_date;
"

# Execute the query
revenue_data <- dbGetQuery(con, query)

# Print the retrieved data
print(revenue_data)

# Disconnect from the database
dbDisconnect(con)

# Install ggplot2 if not installed
if(!require(ggplot2)) {
  install.packages("ggplot2") }
# Load ggplot2
library(ggplot2)

# Visualize the result using ggplot2
ggplot(revenue_data, aes(x = rental_date, y = total_revenue)) +
  geom_line(color = "blue") +
  labs(title = "Total Daily Revenue from Rentals", x = "Date", y = "Total Revenue") +
  theme_minimal()


#
#-------------------------------------------------------------------------------
# DELIVERABLE 4: SQL query and Bar Graph for Inventory Stock Take
# Requirements for the Inventory Stock Take:
#  1. Aliasing of variable names: Use SQL aliases to simplify table and column references.
#  2. Common Table Expression (CTE): Use a CTE for organising part of the query.
#  3. Five Joins: Join at least five tables in the query.
#  4. Where statement: Filter the data (in this case, to focus on R-rated movies).
#  5. Group by: Group the data (by store and movie category).
#  6. Aggregating function: Use an aggregation function (such as COUNT()) to count DVDs.
#
con <- dbConnect(
  Postgres(),
  dbname = dbname,
  host = host,
  port = port,
  user = user,
  password = password
)

query <- "
WITH r_rated_movies AS (
    SELECT 
        film.film_id, 
        film.title, 
        film.rating, 
        film_category.category_id, 
        inventory.store_id
    FROM film
    JOIN film_category ON film.film_id = film_category.film_id
    JOIN inventory ON film.film_id = inventory.film_id
    WHERE film.rating = 'R'
)
SELECT 
    store.store_id AS store, 
    category.name AS category, 
    COUNT(r_rated_movies.film_id) AS dvd_count
FROM r_rated_movies
JOIN store ON r_rated_movies.store_id = store.store_id
JOIN category ON r_rated_movies.category_id = category.category_id
GROUP BY store.store_id, category.name
ORDER BY store.store_id, category.name;
"

# Execute the query
stock_data <- dbGetQuery(con, query)

# Print the data
print(stock_data)
 
# Disconnect from the database
dbDisconnect(con)

# Install ggplot2 if not installed
if(!require(ggplot2)) {
  install.packages("ggplot2") }
# Load ggplot2
library(ggplot2)
  
  # Convert dvd_count from integer64 to numeric
  stock_data$dvd_count <- as.numeric(stock_data$dvd_count)
  
  # Visualize the result using ggplot2
  ggplot(stock_data, aes(x = category, y = dvd_count, fill = factor(store))) +
    geom_bar(stat = "identity", position = "dodge") +
    labs(title = "R-rated DVDs Stock Take by Store and Category", 
         x = "Category", y = "Number of DVDs") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  
  # Disconnect and reconnect to the database
  if (dbIsValid(con)) {
    dbDisconnect(con)
  }
  
#
#-------------------------------------------------------------------------------
# DELIVERABLE 5: Investigate an Execution Plan
#  
con <- dbConnect(
    Postgres(),
    dbname = dbname,
    host = host,
    port = port,
    user = user,
    password = password
  )
  
explain_query <- "
  EXPLAIN SELECT rental.rental_id, rental.rental_date, customer.first_name, customer.last_name
  FROM rental
  JOIN customer ON rental.customer_id = customer.customer_id
"
execution_plan <- dbGetQuery(con, explain_query)
print(execution_plan)



# Disconnect from the database
dbDisconnect(con)

