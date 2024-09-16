# conditionally install packages
if(!require(RPostgres)) {
  install.packages("RPostgres")
}

# packages
library(DBI)
library(RPostgres)


#  environment variables (CONFIRM in environment values)
dbname <- Sys.getenv("PG_DBNAME", "Jeff")
host <- Sys.getenv("PG_HOST", "mathmads.canterbury.ac.nz")
port <- Sys.getenv("PG_PORT", "8909")
user <- Sys.getenv("PG_USR", "student_data422")
password <- Sys.getenv("PG_PASS")



# connection object via environment variables (.Fenviron)
con <- dbConnect(
  Postgres(),
  dbname = dbname,
  host = host,
  port = port,
  user = user,
  password = password
)

# confirm connection status
if (!dbIsValid(con)) {
  stop("FAILED connection to PostgreSQL database.")
} else {
  print("SUCCESSFUL connection to PostgreSQL database.")
}


#-------------------------------------------------------------------------------
# DELIVERABLE 1: List all tables in the connected database
tables <- dbListTables(con)
print(tables)


#-------------------------------------------------------------------------------
# DELIVERABLE 2: List all the fields in a table
fields <- dbListFields(con, "rental")
print(fields)

#-------------------------------------------------------------------------------
# (NOT A DELIVERABLE): List all the fields in  all tables
# check if tables exist 
if(length(tables) == 0) {
  print("No tables found in the database.")
} else {
  # iterate tables
  for (table in tables) {
    cat("\nFields in table:", table, "\n")
    
    # get fields
    fields <- dbListFields(con, table)
    
    # print fields
    print(fields)
  }
}


#-------------------------------------------------------------------------------
# DELIVERABLE 3: Pull some data from the 'rental' table as an example
# SELECT:      SQL to 'pull some data' 
# rental_id:   Unique ID.
# rental_date: The date of  rental occurred.
#inventory_id: ID of the rented item.
#customer_id:  ID of the customer who rented the item.
#LIMIT 10:     clause to limit data
query <- "SELECT rental_id, rental_date, inventory_id, customer_id FROM rental LIMIT 10"
data <- dbGetQuery(con, query)
print(data)



#-------------------------------------------------------------------------------
# DELIVERABLE 4: Plot the data
# Install ggplot2 if not installed
if(!require(ggplot2)) {
  install.packages("ggplot2")
}

# Load ggplot2
library(ggplot2)

# Plot a basic bar chart of rentals per customer
ggplot(data, aes(x = factor(customer_id))) +
  geom_bar() +
  xlab("Customer ID") +
  ylab("Number of Rentals") +
  ggtitle("Number of Rentals per Customer") +
  theme_minimal()




#-------------------------------------------------------------------------------
# DELIVERABLE 5: Perform a JOIN between 'rental' and 'customer' tables to get customer names
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



#-------------------------------------------------------------------------------
# Investigate the execution plan for the JOIN query
# results are in dataframe 
explain_query <- "
  EXPLAIN SELECT rental.rental_id, rental.rental_date, customer.first_name, customer.last_name
  FROM rental
  JOIN customer ON rental.customer_id = customer.customer_id
"
execution_plan <- dbGetQuery(con, explain_query)
print(execution_plan)




#-------------------------------------------------------------------------------
# Disconnect from the database
dbDisconnect(con)
