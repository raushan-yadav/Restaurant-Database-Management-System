#title: "Part C"
#subtitle: "  Realize Database" 
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



# Function for checking if a table already exists in the db before creation.
checkIfTableExists <- function(dbConn, tableName) {
  query <- paste("SHOW TABLES LIKE '", tableName, "'", sep = "")
  result <- dbGetQuery(dbConn, query)
  return(nrow(result) > 0)
}
# Function for creation of "visit Details" table.
visitDetailsTableCreation <- function(dbConn) {
  if (!checkIfTableExists(dbConn, "VisitDetails")) {
    queryToCreateVisitDetailsTable <- "
    CREATE TABLE VisitDetails (
      VisitID INTEGER PRIMARY KEY,
      RestaurantID INTEGER NOT NULL,
      ServerEmpID INTEGER NOT NULL,
      VisitDate DATE NOT NULL,
      VisitTime TIME,
      MealTypeID INTEGER NOT NULL,
      PartySize INTEGER,
      Genders TEXT NOT NULL,
      WaitTime INTEGER NOT NULL,
      CustomerID INTEGER NOT NULL,
      FOREIGN KEY (CustomerID) REFERENCES CustomerDetails(CustomerID),
      FOREIGN KEY (RestaurantID) REFERENCES RestaurantDetails(RestaurantID),
      FOREIGN KEY (MealTypeID) REFERENCES MealType(MealTypeID),
      FOREIGN KEY (ServerEmpID) REFERENCES ServerDetails(ServerEmpID)
    );
    "
    dbExecute(dbConn, queryToCreateVisitDetailsTable)
    print("VisitDetails table creation is done.")
  } else {
    print("VisitDetails table already exists in the db.")
  }
}

# Function for creation of "Restaurant Details" table.
restaurantDetailsTableCreation <- function(dbConn) {
  if (!checkIfTableExists(dbConn, "RestaurantDetails")) {
    queryToCreateRestaurantdDetailsTable <- "
    CREATE TABLE RestaurantDetails (
      RestaurantID INTEGER PRIMARY KEY AUTO_INCREMENT,
      RestaurantName TEXT NOT NULL
    );
    "
    dbExecute(dbConn,  queryToCreateRestaurantdDetailsTable)
    print("RestaurantDetails table creation is done.")
  } else {
    print("RestaurantDetails table already exists in the db.")
  }
}

# Function for creation of "Server Details" table.
serverDetailsTableCreation <- function(dbConn) {
  if (!checkIfTableExists(dbConn, "ServerDetails")) {
    queryToCreateServerDetailsTable <- "
    CREATE TABLE ServerDetails (
      ServerEmpID INTEGER PRIMARY KEY,
      ServerName TEXT NOT NULL,
      StartDateHired DATE,
      EndDateHired DATE,
      HourlyRate NUMERIC(20,10),
      ServerBirthDate DATE,
      ServerTIN TEXT
    );
    "
    dbExecute(dbConn, queryToCreateServerDetailsTable)
    print("ServerDetails table creation is done.")
  } else {
    print("ServerDetails table already exists in the db.")
  }
}

# Function for creation of "Customer Details" table.
customerDetailsTableCreation <- function(dbConn) {
  if (!checkIfTableExists(dbConn, "CustomerDetails")) {
    queryToCreateCustomerDetailsTable <- "
    CREATE TABLE CustomerDetails (
      CustomerID INTEGER PRIMARY KEY AUTO_INCREMENT,
      CustomerName TEXT NOT NULL,
      CustomerPhone TEXT,
      CustomerEmail TEXT,
      LoyaltyMember BOOLEAN NOT NULL
    );
    "
    dbExecute(dbConn, queryToCreateCustomerDetailsTable)
    print("CustomerDetails table creation is done.")
  } else {
    print("CustomerDetails table already exists in the db.")
  }
}

# Function for creation of "Mealtype" table.
mealTypeTableCreation <- function(dbConn) {
  if (!checkIfTableExists(dbConn, "MealType")) {
    queryToCreateMealTypeTable <- "
    CREATE TABLE MealType (
      MealTypeID INTEGER PRIMARY KEY AUTO_INCREMENT,
      MealType TEXT NOT NULL
    );
    "
    dbExecute(dbConn, queryToCreateMealTypeTable)
    print("MealType table creation is done.")
  } else {
    print("MealType table already exists in the db.")
  }
}

# Function for creation of "Bill details" table.
billDetailsTableCreation <- function(dbConn) {
  if (!checkIfTableExists(dbConn, "BillDetails")) {
    queryToCreateBillDetailsTable <- "
    CREATE TABLE BillDetails (
      BillID INTEGER PRIMARY KEY AUTO_INCREMENT,
      VisitID INTEGER NOT NULL,
      FoodBill NUMERIC(20,10) NOT NULL,
      TipAmount NUMERIC(20,10) NOT NULL,
      DiscountApplied NUMERIC(20,10) NOT NULL,
      PaymentID INTEGER NOT NULL,
      orderedAlcohol BOOLEAN NOT NULL,
      AlcoholBill NUMERIC(20,10) NOT NULL,
      FOREIGN KEY (VisitID) REFERENCES VisitDetails(VisitID),
      FOREIGN KEY (PaymentID) REFERENCES PaymentMethod(PaymentID)
    );
    "
    dbExecute(dbConn, queryToCreateBillDetailsTable)
    print("BillDetails table creation is done.")
  } else {
    print("BillDetails table already exists in the db.")
  }
}

# Function for creation of "Payment Method" table.
paymentMethodTableCreation <- function(dbConn) {
  if (!checkIfTableExists(dbConn, "PaymentMethod")) {
    queryToCreatePaymentMethodTable <- "
    CREATE TABLE PaymentMethod (
      PaymentID INTEGER PRIMARY KEY AUTO_INCREMENT,
      PaymentMethod TEXT NOT NULL
    );
    "
    dbExecute(dbConn, queryToCreatePaymentMethodTable)
    print("PaymentMethod table creation is done.")
  } else {
    print("PaymentMethod table already exists in the db.")
  }
}

#Function for creation of all tables in the database.
tablesCreation <- function(dbConn) {
  paymentMethodTableCreation(dbConn)
  customerDetailsTableCreation(dbConn)
  restaurantDetailsTableCreation(dbConn)
  mealTypeTableCreation(dbConn)
  serverDetailsTableCreation(dbConn)
  visitDetailsTableCreation (dbConn)
  billDetailsTableCreation(dbConn)
}
# Main function to execute all functions.
main <- function() {
  # Load necessary libraries
  loadLibraries()
  
  # Connection Establishment to DB.
  conn <- dbConnection()
  
    print("Connection Establishment to AIVEN DB is success.")
    
    # Table creation in db.
    tablesCreation(conn)
    
    # Disconnecting from the database after work.
    dbDisconnect(conn)
    print("Database disconnected.")
  }


#Calling main function.
main()




