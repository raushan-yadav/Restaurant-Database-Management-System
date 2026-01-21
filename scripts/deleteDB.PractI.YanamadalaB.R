#title: "Part D"
#subtitle: " Delete Database" 
#author: "Bhanu Harsha Yanamadala"
#date: "03/11/2025"
# Database Configuration
# Set your database credentials as environment variables before running:
# Sys.setenv(DB_USER = "your_username")
# Sys.setenv(DB_PASSWORD = "your_password")
# Sys.setenv(DB_NAME = "your_database")
# Sys.setenv(DB_HOST = "your_host")
# Sys.setenv(DB_PORT = "your_port")

# Function to load necessary libraries.
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

# Function to check if a table exists in the database
tableExists <- function(conn, tableName) {
  query <- paste0("SHOW TABLES LIKE '", tableName, "';")
  result <- dbGetQuery(conn, query)
  return(nrow(result) > 0)
}

# Function to drop all tables from the database
dropAllTables <- function(conn) {
  droppingTables <- c(
    "BillDetails",
    "VisitDetails",
    "ServerDetails",
    "CustomerDetails",
    "RestaurantDetails",
    "MealType",
    "PaymentMethod"
  )
  
  # Drop each table inorder.
  errors <- character()
  for (table in droppingTables) {
    # Check if the table already exists in the db.
    if (!tableExists(conn, table)) {
      errorMsg <- paste("Error: Table '", table, "' does not exist in the database.", sep = "")
      errors <- c(errors, errorMsg)
    } else {
      dropTableQuery <- paste("DROP TABLE", table, ";")
      tryCatch(
        {
          dbExecute(conn, dropTableQuery)
          print(paste("Dropping", table,"table."))
        },
        error = function(e) {
          errors <- c(errors, paste("Error dropping in the table '", table, "': ", e$message, sep = ""))
        }
      )
    }
  }
  
  # Errors if any.
  if (length(errors) > 0) {
    return(errors)
  }
  return(invisible(NULL))
}

# Main Function
main <- function() {
  # Load necessary libraries
  loadLibraries()
  
  # Close any existing connections
  existingConnections <- dbListConnections(RMySQL::MySQL())
  for (conn in existingConnections) {
    dbDisconnect(conn)
  }
  
  # Connection Establishment to DB.
  conn <- dbConnection ()
 
    print("Connection Establishment to AIVEN DB is success.")
    
    
    
    # Dropping tables.
    print("DROPPPING THE TABLES IN DB.")
    dropErrors <- dropAllTables(conn)
    if (!is.null(dropErrors) && length(dropErrors) > 0) {
      for (error in dropErrors) {
        print(error)
      }
    }
    
    
    # Disconnecting from the database after work.
    dbDisconnect(conn)
    print("Disconnected from the database")
  }


#Calling main function
main()