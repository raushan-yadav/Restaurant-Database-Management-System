
# Restaurant Database Management System

A relational database management system designed using normalization (3NF) to analyze restaurant visits, billing transactions, and revenue patterns for business decision-making.

--

## Project Overview
This project focuses on designing and implementing a **relational database
management system** for a restaurant business to analyze customer visits,
sales transactions, and revenue patterns.

The database is built using **normalization principles (up to 3NF)** to
ensure data integrity, reduce redundancy, and support efficient querying for
business analysis.

---

## Problem Statement
Restaurants generate large volumes of transactional data related to:
- Customer visits
- Orders and billing
- Payments and revenue

Without a structured database, it becomes difficult to analyze performance,
track customer behavior, and generate meaningful insights.  
This project addresses the problem by converting raw transactional data into
a well-structured relational database.

---

## Objectives
- Design a normalized relational database schema
- Implement database tables with proper primary and foreign keys
- Load and manage restaurant visit and billing data
- Perform analytical queries to extract business insights
- Support reporting on revenue and customer trends

---

## Database Design
The database is structured using **Third Normal Form (3NF)** to eliminate
data redundancy and maintain consistency.

### Key Entities
- Customers
- Restaurants
- Servers
- Visits
- Bills
- Meal Types
- Payment Methods

### Entity Relationship Diagram (ERD)
<p align="center">
  <img src="RestaurantDB-ERD.png" height="400">
</p>

---

## Technologies Used
- **Database**: MySQL
- **Programming Language**: R
- **Tools**:
  - RStudio
  - SQL
  - R Markdown
- **Version Control**: Git & GitHub

---

## Project Structure
restaurant-database-management-system/
│
├── data/ # Raw dataset
├── docs/ # Documentation and ERD
├── scripts/ # R scripts for DB creation and loading
├── notebooks/ # Analysis and reports
├── outputs/ # Generated reports
├── README.md
└── .gitignore


---

## Implementation Workflow
1. Designed relational schema based on functional dependencies  
2. Created database tables with constraints  
3. Loaded data using R scripts (ETL process)  
4. Validated data integrity and relationships  
5. Executed SQL queries for analysis and reporting  

---

## Analysis & Insights
- Identified high-revenue restaurants
- Analyzed visit frequency and customer patterns
- Studied revenue trends across time
- Evaluated customer loyalty behavior

These insights help restaurant management in decision-making related to
operations and business growth.

---

## Learning Outcomes
- Practical understanding of **DBMS concepts**
- Hands-on experience with **database normalization**
- Writing complex SQL queries
- Integrating R with MySQL
- Applying database systems to real-world business problems

---

## Future Enhancements
- Add advanced SQL views and stored procedures
- Integrate dashboarding tools (Power BI / Tableau)
- Extend analysis with predictive modeling
- Develop a web-based interface for database interaction

---

## Author
**Raushan Yadav**  
Data Analyst | Database & Analytics  
