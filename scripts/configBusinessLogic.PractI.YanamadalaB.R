#title: "Part H"
#subtitle: "Add Business Logic" 
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

# Function for setting up initial test data.
settingupTestData <- function(conn) {
  # Inserting testcase for RestaurantDetails.
  dbExecute(conn, "INSERT IGNORE INTO RestaurantDetails (RestaurantID, RestaurantName) VALUES (1, 'popeyes');")
  
  # Inserting testcase for CustomerDetails.
  dbExecute(conn, "INSERT IGNORE INTO CustomerDetails (CustomerID, CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember) 
                  VALUES (1, 'Bhanu Harsha', '401-369-9123', 'ybh@email.com', TRUE);")
  
  # Inserting testcase for ServertDetails.
  dbExecute(conn, "INSERT IGNORE INTO ServerDetails (ServerEmpID, ServerName) VALUES (1001, 'Aadarsh');")
  
  # Inserting testcase for Mealtype.
  dbExecute(conn, "INSERT IGNORE INTO MealType (MealTypeID, MealType) VALUES (1, 'Lunch');")
  
  # Inserting testcase for PaymentMethod.
  dbExecute(conn, "INSERT IGNORE INTO PaymentMethod (PaymentID, PaymentMethod) VALUES (1, 'Credit Card');")
  
  print("Test data setup for tables is completed.\n")
}

# Creation of StoreVisit Stored Procedure.
creatingStoreVisitProcedure <- function(conn) {
  dbSendQuery(conn, "DROP PROCEDURE 
              IF EXISTS StoreVisit;")
    
  sqlQuery <- "
  CREATE PROCEDURE StoreVisit(
    IN pRestaurantID INTEGER,IN pCustomerID INTEGER,IN pVisitDate DATE,
    IN pVisitTime TIME,IN pMealTypeID INTEGER,IN pPartySize INTEGER,
    IN pGenders TEXT,IN pWaitTime INTEGER,IN pFoodBill NUMERIC(20,10),
    IN pAlcoholBill NUMERIC(20,10),IN pTipAmount NUMERIC(20,10),
    IN pDiscountApplied NUMERIC(20,10),IN pOrderedAlcohol BOOLEAN,
    IN pPaymentID INTEGER,IN pServerEmpID INTEGER
  )
  BEGIN
    DECLARE visVisitID INTEGER;
    
    -- Generate new VisitID
    SELECT COALESCE(MAX(VisitID), 0) + 1 INTO visVisitID FROM VisitDetails;
    
    INSERT INTO VisitDetails (VisitID, RestaurantID, CustomerID, VisitDate, VisitTime, MealTypeID, PartySize, Genders, WaitTime, ServerEmpID)
    VALUES (visVisitID, pRestaurantID, pCustomerID, pVisitDate, pVisitTime, pMealTypeID, pPartySize, pGenders, pWaitTime, pServerEmpID);
    
    INSERT INTO BillDetails (VisitID, FoodBill, AlcoholBill, TipAmount, DiscountApplied, OrderedAlcohol, PaymentID)
    VALUES (visVisitID, pFoodBill, pAlcoholBill, pTipAmount, pDiscountApplied, pOrderedAlcohol, pPaymentID);
  END
  "
  
  tryCatch({
    dbExecute(conn, sqlQuery)
    cat("Creation of StoreVisit stored procedure is successful.\n")
  }, error = function(e) {
    stop("Error in creation of  StoreVisit procedure: ", e$message, "\n")
  })
}

# Creation of  StoreNewVisit Stored Procedure.
creatingStoreNewVisitProcedure <- function(conn) {
  dbSendQuery(conn, "DROP PROCEDURE IF EXISTS StoreNewVisit;")
  
  sqlQuery <- "
  CREATE PROCEDURE StoreNewVisit(
    IN pRestaurantName TEXT,IN pCustomerName TEXT,IN pCustomerPhone TEXT,
    IN pCustomerEmail TEXT,IN pLoyaltyMember BOOLEAN,IN pServerName TEXT,
    IN pVisitDate DATE,IN pVisitTime TIME,IN pMealTypeID INTEGER,
    IN pPartySize INTEGER,IN pGenders TEXT,IN pWaitTime INTEGER,
    IN pFoodBill NUMERIC(20,10),IN pAlcoholBill NUMERIC(20,10),
    IN pTipAmount NUMERIC(20,10),IN pDiscountApplied NUMERIC(20,10),
    IN pOrderedAlcohol BOOLEAN,IN pPaymentID INTEGER
  )
  BEGIN
    DECLARE visRestaurantID INTEGER;
    DECLARE visCustomerID INTEGER;
    DECLARE visServerEmpID INTEGER;
    DECLARE visVisitID INTEGER;
    
    -- Check and insert Restaurant if not exists
    SELECT RestaurantID INTO visRestaurantID FROM RestaurantDetails WHERE RestaurantName = pRestaurantName;
    IF visRestaurantID IS NULL THEN
      INSERT INTO RestaurantDetails (RestaurantName) VALUES (pRestaurantName);
      SET visRestaurantID = LAST_INSERT_ID();
    END IF;
    
    -- Check and insert Customer if not exists
    SELECT CustomerID INTO visCustomerID FROM CustomerDetails 
    WHERE CustomerName = pCustomerName AND CustomerPhone = pCustomerPhone AND CustomerEmail = pCustomerEmail;
    IF visCustomerID IS NULL THEN
      INSERT INTO CustomerDetails (CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember)
      VALUES (pCustomerName, pCustomerPhone, pCustomerEmail, pLoyaltyMember);
      SET visCustomerID = LAST_INSERT_ID();
    END IF;
    
    -- Check and insert Server if not exists
    SELECT ServerEmpID INTO visServerEmpID FROM ServerDetails WHERE ServerName = pServerName;
    IF visServerEmpID IS NULL THEN
      SELECT COALESCE(MAX(ServerEmpID), 0) + 1 INTO visServerEmpID FROM ServerDetails;
      INSERT INTO ServerDetails (ServerEmpID, ServerName) VALUES (visServerEmpID, pServerName);
    END IF;
    
    -- Generate new VisitID
    SELECT COALESCE(MAX(VisitID), 0) + 1 INTO visVisitID FROM VisitDetails;
    
    -- Insert Visit
    INSERT INTO VisitDetails (VisitID, RestaurantID, CustomerID, VisitDate, VisitTime, MealTypeID, PartySize, Genders, WaitTime, ServerEmpID)
    VALUES (visVisitID, visRestaurantID, visCustomerID, pVisitDate, pVisitTime, pMealTypeID, pPartySize, pGenders, pWaitTime, visServerEmpID);
    
    -- Insert Bill
    INSERT INTO BillDetails (VisitID, FoodBill, AlcoholBill, TipAmount, DiscountApplied, OrderedAlcohol, PaymentID)
    VALUES (visVisitID, pFoodBill, pAlcoholBill, pTipAmount, pDiscountApplied, pOrderedAlcohol, pPaymentID);
  END
  "
  
  tryCatch({
    dbExecute(conn, sqlQuery)
    cat("creation of StoreNewVisit stored procedure is successful.\n")
  }, error = function(e) {
    stop("Error creating StoreNewVisit procedure: ", e$message, "\n")
  })
}

# Function for calling StoreVisit.
callStoreVisit <- function(conn) {
  sqlQuery <- "CALL StoreVisit(1, 1, '2025-03-13', '14:00:00', 1, 2, 'M,F', 15, 75.50, 20.00, 15.00, 5.00, TRUE, 1, 1001);"
  tryCatch({
    dbExecute(conn, sqlQuery)
    cat("StoreVisit is executed successfully.\n")
    # Verifying the insertion.
    result <- dbGetQuery(conn, "SELECT * FROM VisitDetails ORDER BY VisitID DESC LIMIT 1;")
    billResult <- dbGetQuery(conn, "SELECT * FROM BillDetails ORDER BY BillID DESC LIMIT 1;")
    cat("Last Restaurant Visit Details:\n")
    print(result)
    cat("Last Restaurant Bill Details:\n")
    print(billResult)
  }, error = function(e) {
    stop("Error executing StoreVisit: ", e$message, "\n")
  })
}

# Function for calling StoreNewVisit.
callStoreNewVisit <- function(conn) {
  sqlQuery <- "CALL StoreNewVisit('Shahs Halal', 'Bhanu Harsha', '401-123-4567', 'ybh@email.com', TRUE, 'harsh', '2025-03-13', '18:00:00', 2, 4, 'M,F,M,F', 20, 250.00, 30.00, 25.00, 10.00, TRUE, 1);"
  tryCatch({
    dbExecute(conn, sqlQuery)
    cat("StoreNewVisit executed successfully.\n")
    # Verifying the insertion.
    result <- dbGetQuery(conn, "
      SELECT v.*, r.RestaurantName, c.CustomerName, s.ServerName, b.*
      FROM VisitDetails v
      JOIN RestaurantDetails r ON v.RestaurantID = r.RestaurantID
      JOIN CustomerDetails c ON v.CustomerID = c.CustomerID
      JOIN ServerDetails s ON v.ServerEmpID = s.ServerEmpID
      JOIN BillDetails b ON v.VisitID = b.VisitID
      ORDER BY v.VisitID DESC LIMIT 1;")
    cat("Last Inserted Record Details:\n")
    print(result)
  }, error = function(e) {
    stop("Error executing StoreNewVisit: ", e$message, "\n")
  })
}

# Main function for execution.
main <- function() {
  # Loading required libraries
  loadLibraries()
  
  # Establish database connection
  conn <- dbConnection()
  cat("Database connection established successfully.\n")
  
  # Setup test data
  settingupTestData(conn)
  
  # Create stored procedures
  creatingStoreVisitProcedure(conn)
  creatingStoreNewVisitProcedure(conn)
  
  # Test the stored procedures
  callStoreVisit(conn)
  callStoreNewVisit(conn)
  
  # Disconnect from database
  dbDisconnect(conn)
  cat("Database connection closed.\n")
}

# Execute the script
main()