#title: "Part E"
#subtitle: "Populate Database" 
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

# Function to insert data into database tables
insertData <- function(conn, csvData) {
  # Transaction beginning.
  dbBegin(conn)
  
  # Data insertion into PaymentMethod table.
  # There will be insertion of 3 rows which are 'Mobile payment', 'credit card','cash'.
  paymentMethodDF <- data.frame(PaymentMethod = unique(csvData$PaymentMethod))
  paymentMethodVal <- paste(sprintf("('%s')", paymentMethodDF$PaymentMethod), collapse = ", ")
  dbExecute(conn, paste("INSERT INTO PaymentMethod (PaymentMethod) VALUES", paymentMethodVal))
 sprintf("Inserted data into PaymentMethod table.\n")
  
 # Data insertion into RestaurantDetails table.
 # There will be insertion of 9 rows which are unique restaurants.
  restaurantDetailsDF <- data.frame(RestaurantName = unique(csvData$Restaurant))
  restaurantDetailsVal <- paste(sprintf("('%s')", restaurantDetailsDF$RestaurantName), collapse = ", ")
  dbExecute(conn, paste("INSERT INTO RestaurantDetails (RestaurantName) VALUES", restaurantDetailsVal))
  cat("Inserted data into RestaurantDetails table.\n")
  
  # Data insertion into Mealtype table.
  # There will be insertion of 4 rows which are 'Breakfast', 'Take-Out','Dinner','Lunch'.
  mealTypeDF <- data.frame(MealType = unique(csvData$MealType))
  mealTypeVal <- paste(sprintf("('%s')", mealTypeDF$MealType), collapse = ", ")
  dbExecute(conn, paste("INSERT INTO MealType (MealType) VALUES", mealTypeVal))
  cat("Inserted data into MealType table.\n")
  
  # Data insertion into ServerDetails table.
  # In total 48 rows will be inserted out of which 47 are unique server details and one default row.
  # One Default Server row is created as first row for representing inconsistent values of Server details.
  #Inconsistent data values in csv will be pointed to this row.
  dbExecute(conn, "INSERT INTO ServerDetails (ServerEmpID, ServerName, StartDateHired, EndDateHired, HourlyRate, ServerBirthDate, ServerTIN) 
                  VALUES ('0', 'Server not specified', NULL, NULL, '0.00', NULL, NULL)")
  
  uniqueServerdetails <- csvData[!duplicated(csvData$ServerEmpID) & !is.na(csvData$ServerEmpID) & csvData$ServerEmpID != "", ]
  uniqueServerVal <- paste0(
    "(",
    sprintf("'%s'", uniqueServerdetails$ServerEmpID), ", ",
    sprintf("'%s'", uniqueServerdetails$ServerName), ", ",
    sprintf("'%s'", format(as.Date(uniqueServerdetails$StartDateHired, "%Y-%m-%d"), "%Y-%m-%d")), ", ",
    ifelse(is.na(uniqueServerdetails$EndDateHired) | uniqueServerdetails$EndDateHired == "", "NULL", sprintf("'%s'", format(as.Date(uniqueServerdetails$EndDateHired, "%Y-%m-%d"), "%Y-%m-%d"))), ", ",
    uniqueServerdetails$HourlyRate, ", ",
    ifelse(is.na(uniqueServerdetails$ServerBirthDate) | uniqueServerdetails$ServerBirthDate == "", "NULL", sprintf("'%s'", format(as.Date(uniqueServerdetails$ServerBirthDate, "%m/%d/%Y"), "%Y-%m-%d"))), ", ",
    ifelse(is.na(uniqueServerdetails$ServerTIN) | uniqueServerdetails$ServerTIN == "", "NULL", sprintf("'%s'", uniqueServerdetails$ServerTIN)),
    ")"
  )
  uniqueServerValStr <- paste(uniqueServerVal, collapse = ", ")
  dbExecute(conn, paste("INSERT INTO ServerDetails (ServerEmpID, ServerName, StartDateHired, EndDateHired, HourlyRate, ServerBirthDate, ServerTIN) VALUES", uniqueServerValStr))
  cat("Inserted data into ServerDetails table.\n")
  
  # Data insertion into CustomerDetails table.
  # In total 17 rows will be inserted out of which 16 are unique server details and one default row.
  # One Default CustomerDetails row is created as first row for representing inconsistent values of Customer details.
  #Inconsistent data values in csv will be pointed to this row.
  dbExecute(conn, "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO';")
  dbExecute(conn, "INSERT INTO CustomerDetails (CustomerID, CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember) 
                  VALUES (0, 'customer not specified', NULL, NULL, FALSE)")
  dbExecute(conn, "SET SESSION sql_mode = '';")
 
  uniqueCustomerDetails <- csvData[!duplicated(csvData[, c("CustomerName", "CustomerPhone", "CustomerEmail")]) &
                                 !(is.na(csvData$CustomerName) & is.na(csvData$CustomerPhone) & is.na(csvData$CustomerEmail)) &
                                 !(csvData$CustomerName == "" & csvData$CustomerPhone == "" & csvData$CustomerEmail == ""), ]
  customerDetailsVal <- paste0(
    "(",
    sprintf("'%s'", uniqueCustomerDetails$CustomerName), ", ",
    sprintf("'%s'", uniqueCustomerDetails$CustomerPhone), ", ",
    sprintf("'%s'", uniqueCustomerDetails$CustomerEmail), ", ",
    ifelse(uniqueCustomerDetails$LoyaltyMember == "TRUE", "TRUE", "FALSE"),
    ")"
  )
  customerDetailsValStr <- paste(customerDetailsVal, collapse = ", ")
  dbExecute(conn, paste("INSERT INTO CustomerDetails (CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember) VALUES", customerDetailsValStr))
  cat("Inserted data into CustomerDetails table.\n")
  
  # Pre fetching Fk for code optimization.
  customers <- dbGetQuery(conn, "SELECT CustomerID, CustomerName, CustomerPhone, CustomerEmail FROM CustomerDetails")
  customers$key <- paste(ifelse(is.na(customers$CustomerName) | customers$CustomerName == "", "customer not specified", customers$CustomerName),
                         ifelse(is.na(customers$CustomerPhone) | customers$CustomerPhone == "", "Unknown", customers$CustomerPhone),
                         ifelse(is.na(customers$CustomerEmail) | customers$CustomerEmail == "", "Unknown", customers$CustomerEmail), sep = "|")
  restaurants <- dbGetQuery(conn, "SELECT RestaurantID, RestaurantName FROM RestaurantDetails")
  meal_types <- dbGetQuery(conn, "SELECT MealTypeID, MealType FROM MealType")
  payment_methods <- dbGetQuery(conn, "SELECT PaymentID, PaymentMethod FROM PaymentMethod")
  
  # Mapping Fk  to VisitDetails table  Data.
  csvData$customer_key <- paste(ifelse(is.na(csvData$CustomerName) | csvData$CustomerName == "", "customer not specified", csvData$CustomerName),
                                 ifelse(is.na(csvData$CustomerPhone) | csvData$CustomerPhone == "", "Unknown", csvData$CustomerPhone),
                                 ifelse(is.na(csvData$CustomerEmail) | csvData$CustomerEmail == "", "Unknown", csvData$CustomerEmail), sep = "|")
  csvData$CustomerID <- customers$CustomerID[match(csvData$customer_key, customers$key)]
  csvData$RestaurantID <- restaurants$RestaurantID[match(csvData$Restaurant, restaurants$RestaurantName)]
  csvData$MealTypeID <- meal_types$MealTypeID[match(csvData$MealType, meal_types$MealType)]
  csvData$PaymentID <- payment_methods$PaymentID[match(csvData$PaymentMethod, payment_methods$PaymentMethod)]
  
  # Data insertion into VisitDetails table in batch size of 1000.
  batchSize <- 1000
  batches <- ceiling(nrow(csvData) / batchSize)
  for (batch in 1:batches) {
    startIndx <- (batch - 1) * batchSize + 1
    endIndx <- min(batch * batchSize, nrow(csvData))
    batchDataVal <- csvData[startIndx:endIndx, ]
    
    values <- paste0(
      "(",
      batchDataVal$VisitID, ", '",
      batchDataVal$VisitDate, "', ",
      ifelse(is.na(batchDataVal$VisitTime) | batchDataVal$VisitTime == "", "NULL", paste0("'", batchDataVal$VisitTime, "'")), ", ",
      batchDataVal$CustomerID, ", ",
      batchDataVal$MealTypeID, ", ",
      batchDataVal$RestaurantID, ", ",
      ifelse(batchDataVal$PartySize == 99, "NULL", batchDataVal$PartySize), ", '",
      batchDataVal$Genders, "', ",
      pmax(batchDataVal$WaitTime, 0), ", '",
      ifelse(is.na(batchDataVal$ServerEmpID) | batchDataVal$ServerEmpID == "", "0", batchDataVal$ServerEmpID), "')"
    )
    values_str <- paste(values, collapse = ", ")
    query <- paste("INSERT INTO VisitDetails (VisitID, VisitDate, VisitTime, CustomerID, MealTypeID, RestaurantID, PartySize, Genders, WaitTime, ServerEmpID) VALUES", values_str)
    dbExecute(conn, query)
  }
  cat("Inserted data into VisitDetails table.\n")
  
   # Data insertion into BillDetails table.
  for (batch in 1:batches) {
    startIndx <- (batch - 1) * batchSize + 1
    endIndx <- min(batch * batchSize, nrow(csvData))
    batchDataVal <- csvData[startIndx:endIndx, ]
    values <- paste0(
      "(",
      batchDataVal$VisitID, ", ",
      batchDataVal$FoodBill, ", ",
      batchDataVal$TipAmount, ", ",
      batchDataVal$DiscountApplied, ", ",
      batchDataVal$PaymentID, ", ",
      ifelse(tolower(batchDataVal$orderedAlcohol) == "yes", "TRUE", "FALSE"), ", ",
      batchDataVal$AlcoholBill, ")"
    )
    values_str <- paste(values, collapse = ", ")
    query <- paste("INSERT INTO BillDetails (VisitID, FoodBill, TipAmount, DiscountApplied, PaymentID, orderedAlcohol, AlcoholBill) VALUES", values_str)
    dbExecute(conn, query)
  }
  cat("Inserted data into BillDetails.\n")
  
  # Ensuring commiting is only done after data is successfully into all tables.
  dbCommit(conn)
}

# Main function
main <- function() {
  
  # Load necessary libraries.
  loadLibraries()
  
  # Connection Establishment to DB.
  conn <- dbConnection()
  print("Connection Establishment to AIVEN DB is success.")
  
    # Reading CSV data file.
    sprintf("Reading CSV data file.")
    df.orig <- read.csv("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv", 
                        header = TRUE, stringsAsFactors = FALSE)
    
    # Data Insertion.
    insertData(conn, df.orig)
    sprintf("Data insertion is successfully completed.")
    
    # Disconnect from the database
    dbDisconnect(conn)
    print("Disconnected from DB.")
  }

#Calling main function.
main()