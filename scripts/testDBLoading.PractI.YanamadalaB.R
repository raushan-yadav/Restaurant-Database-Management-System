#title: "Part F"
#subtitle: "Test Data Loading Process" 
#author: "Bhanu Harsha Yanamadala"
#date: "03/11/2025"
# Database Configuration
# Set your database credentials as environment variables before running:
# Sys.setenv(DB_USER = "your_username")
# Sys.setenv(DB_PASSWORD = "your_password")
# Sys.setenv(DB_NAME = "your_database")
# Sys.setenv(DB_HOST = "your_host")
# Sys.setenv(DB_PORT = "your_port")

# Function to load necessary libraries
loadLibraries <- function() {
  if (!require(DBI)) {
    install.packages("DBI")
    library(DBI)
  }
  if (!require(RMySQL)) {
    install.packages("RMySQL")
    library(RMySQL)
  }
}

# Function for connecting to the Aiven cloud database.
dbConnection <- function() {
  conn <- tryCatch(
    dbConnect(
      RMySQL::MySQL(),
      user = Sys.getenv("DB_USER"),
      password = Sys.getenv("DB_PASSWORD"),
      dbname = Sys.getenv("DB_NAME"),
      host = Sys.getenv("DB_HOST"),
      port = as.integer(Sys.getenv("DB_PORT"))
    ),
    error = function(e) {
      stop("Database connection failed: ", e$message)
    }
  )
  return(conn)
}

# Loading the CSV data.

loadCSVData <- function() {
  df.csv <- read.csv(
    "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv",
    header = TRUE,
    stringsAsFactors = FALSE
  )
  return(df.csv)
}


# Comparing counts.


# A.Restaurants
compareRestaurants <- function(df.csv, conn) {
  csv_unique_restaurants <- length(unique(df.csv$Restaurant))
  db_restaurant_count <- dbGetQuery(conn, "SELECT COUNT(*) AS count FROM RestaurantDetails")$count[1]
  
  cat("Unique Restaurants in CSV  data file  :", csv_unique_restaurants, "\n")
  cat("Rows in DB of RestaurantDetails table :", db_restaurant_count, "\n")
  if (csv_unique_restaurants == db_restaurant_count) {
    cat("Restaurant count MATCHES!\n\n")
  } else {
    cat("Restaurant count DOES NOT MATCH!\n\n")
  }
}

#B.Servers
compareServers <- function(df.csv, conn) {
  csv_unique_servers <- length(unique(df.csv$ServerEmpID[!is.na(df.csv$ServerEmpID) & df.csv$ServerEmpID != ""]))
  db_server_count <- dbGetQuery(
    conn, 
    "SELECT COUNT(DISTINCT ServerEmpID) AS count 
     FROM ServerDetails 
     WHERE ServerEmpID IS NOT NULL AND ServerEmpID != ''"
  )$count[1]
  
  cat("Unique Servers in CSV data file     :", csv_unique_servers, "\n")
  cat("Unique Servers in DB  ServerDetails table     :", db_server_count, "\n")
  if (csv_unique_servers == db_server_count) {
    cat("Server count MATCHES!\n\n")
  } else {
    cat("Server count DOES NOT MATCH!\n\n")
  }
}

#C. Customers
compareCustomers <- function(df.csv, conn) {
  customers_csv <- df.csv[!is.na(df.csv$CustomerName) | !is.na(df.csv$CustomerPhone) | !is.na(df.csv$CustomerEmail), ]
  unique_customers_csv <- nrow(unique(customers_csv[, c("CustomerName", "CustomerPhone", "CustomerEmail")]))
  db_customers_count <- dbGetQuery(conn, "SELECT COUNT(*) AS count FROM CustomerDetails")$count[1]
  
  cat("Unique Customers in CSV data file   :", unique_customers_csv, "\n")
  cat("Rows in DB  CustomerDetails  table  :", db_customers_count, "\n")
  if (unique_customers_csv == db_customers_count) {
    cat("Customer count MATCHES!\n\n")
  } else {
    cat("Customer count DOES NOT MATCH!\n\n")
  }
}

#D. Visits
compareVisits <- function(df.csv, conn) {
  csv_unique_visits <- length(unique(df.csv$VisitID))
  db_visits_count <- dbGetQuery(conn, "SELECT COUNT(*) AS count FROM VisitDetails")$count[1]
  
  cat("Unique Visits in CSV data file      :", csv_unique_visits, "\n")
  cat("Rows in DB VisitDetails table       :", db_visits_count, "\n")
  if (csv_unique_visits == db_visits_count) {
    cat("Visit count MATCHES!\n\n")
  } else {
    cat("Visit count DOES NOT MATCH!\n\n")
  }
}


# 5. Comparing sums for food, alcohol, and tips.

compareSums <- function(df.csv, conn) {
  #A. Sums in CSV
  total_food_csv    <- sum(df.csv$FoodBill, na.rm = TRUE)
  total_alcohol_csv <- sum(df.csv$AlcoholBill, na.rm = TRUE)
  total_tips_csv    <- sum(df.csv$TipAmount, na.rm = TRUE)
  
  #B. Sums in DB
  db_sums <- dbGetQuery(conn, "
    SELECT
      IFNULL(SUM(FoodBill),0)    AS total_food_db,
      IFNULL(SUM(AlcoholBill),0) AS total_alcohol_db,
      IFNULL(SUM(TipAmount),0)   AS total_tips_db
    FROM BillDetails
  ")
  
  total_food_db    <- db_sums$total_food_db[1]
  total_alcohol_db <- db_sums$total_alcohol_db[1]
  total_tips_db    <- db_sums$total_tips_db[1]
  
  cat("Sum FoodBill in CSV  :", total_food_csv, "\n")
  cat("Sum FoodBill in DB   :", total_food_db,   "\n")
  if (abs(total_food_csv - total_food_db) < 0.01) {
    cat("Food bill SUM MATCHES!\n\n")
  } else {
    cat("Food bill SUM DOES NOT MATCH!\n\n")
  }
  
  cat("Sum AlcoholBill in CSV :", total_alcohol_csv, "\n")
  cat("Sum AlcoholBill in DB  :", total_alcohol_db,   "\n")
  if (abs(total_alcohol_csv - total_alcohol_db) < 0.01) {
    cat("Alcohol bill SUM MATCHES !\n\n")
  } else {
    cat("Alcohol bill SUM DOES NOT MATCH!\n\n")
  }
  
  cat("Sum TipAmount in CSV    :", total_tips_csv, "\n")
  cat("Sum TipAmount in DB     :", total_tips_db,   "\n")
  if (abs(total_tips_csv - total_tips_db) < 0.01) {
    cat("Tip amount SUM MATCHES! \n\n")
  } else {
    cat("Tip amount SUM DOES NOT MATCH!\n\n")
  }
}


#  Main Function.

main <- function() {
  # Load necessary libraries.
  loadLibraries()
  
  
  
  # Loading CSV data.
  df.csv <- loadCSVData()
  
  # Connection Establishment to DB.
  conn <- dbConnection()
  print("Connection Establishment to AIVEN DB is success.")
  
  # Compare counts
  compareRestaurants(df.csv, conn)
  compareServers(df.csv, conn)
  compareCustomers(df.csv, conn)
  compareVisits(df.csv, conn)
  
  # Compare sums
  compareSums(df.csv, conn)
  
  print("Testing DB successful.")
  
  # Close the connection
  dbDisconnect(conn)
  cat("Disconnected from the database.\n")
}

# Execute the main function
main()