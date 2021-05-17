"""
Katie Sipos
Tabular and Linked Data - Spring 2021
Overview: this program uses 2 datasets from WPRDC (population & 311 data) and performs some basic analysis using SQL
Notes: the first version of this program is a bit repetetive. As a follow i'll add varied analysis and implement DRY principles in the future!
"""

import psycopg2
import csv
from csv import reader
import pandas as pd

# db connection string
connstring = "host=localhost dbname=pghdata user=pyuser password=passwrd"

# creating dataframes outside of my functions so I can use them for various analyses where I don't want to use SQL directly from the DB

neighborhoods_df = pd.read_csv("neighborhoods_pop.csv")
pgh_311_df = pd.read_csv("pgh_311_2021.csv")


def create_tables():
    # establish a connection with the connstring variable set above
    dbconn = psycopg2.connect(connstring)
    # create cursor with that connection
    cur = dbconn.cursor()

    # SQL to create the tables
    create_sql = """
        DROP TABLE IF EXISTS pgh_311, neighborhoods;

        CREATE TABLE neighborhoods (
            id INT,
            neighborhood_name VARCHAR(50) NOT NULL,
            pop_estimate INT
            );
            
        CREATE TABLE pgh_311 (
            id INT,
            created_on VARCHAR(50),
            request_type VARCHAR(50),
            request_origin VARCHAR(50),
            neighborhood_name VARCHAR(50)  
            );
    """

    # execute the create table SQL
    cur.execute(create_sql)
    # commit changes
    dbconn.commit()
    # close cursor and db connection
    cur.close()
    dbconn.close()
    print()
    print("The neighborhood table has been initialized.")
    print()
    print("The 311 data (2021) table has been initialized.")

    create_menu()


def insert_csv():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()

    # cursor function copy_from to insert DB records
    # https://www.psycopg.org/docs/cursor.html
    with open("neighborhoods_pop.csv", "r") as f:
        # skip the header row so it doesn't throw a type error
        next(f)
        cur.copy_from(
            f,
            "neighborhoods",
            sep=",",
            columns=("id", "neighborhood_name", "pop_estimate"),
        )

    dbconn.commit()
    print()
    print("The neighborhood table data has been inserted from the csv.")
    print()

    with open("pgh_311_2021.csv", "r") as f:
        # skip the header row so it doesn't throw a type error
        next(f)
        cur.copy_from(
            f,
            "pgh_311",
            sep=",",
            columns=(
                "id",
                "created_on",
                "request_type",
                "request_origin",
                "neighborhood_name",
            ),
        )

    dbconn.commit()
    print()
    print("The 311 pgh table data has been inserted from the csv.")
    print()

    dbconn.close()
    cur.close()
    create_menu()


def select_all():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()
    # select everything from the neighborhoods table, limit to 5
    neighborhood_all = """
        SELECT * FROM neighborhoods LIMIT 5;
    """

    cur.execute(neighborhood_all)
    r = cur.fetchall()
    records = pd.DataFrame(r)
    dbconn.commit()
    print()
    print("Printing a sample of the neighborhoods table:")
    print()
    print(records)
    print()

    # same thing but for the 311 dataset
    pgh_311_all = """
        SELECT * FROM pgh_311 LIMIT 5;
    """

    cur.execute(pgh_311_all)
    r = cur.fetchall()
    records = pd.DataFrame(r)
    dbconn.commit()
    print()
    print("Printing a sample of the 311 data table:")
    print()
    print(records)
    print()
    create_menu()


def top_5_population():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()
    # SQL for getting top 5 neighborhoods by population
    print()
    print("Top 5 neighborhoods by population:")
    print()
    top_sql = """
    
    SELECT neighborhood_name as name, cast(pop_estimate as int) as populuation FROM neighborhoods ORDER BY cast(pop_estimate as int) DESC LIMIT 5;
    
    """

    cur.execute(top_sql)
    r = cur.fetchall()
    records = pd.DataFrame(r)
    print(records)
    create_menu()


def top_10_requests():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()

    print()
    print("Top 10 request types with counts:")
    print()

    # SQL for getting the top 10 request types across all neighborhoods limited to 10 so as not
    top_sql = """
    
    SELECT request_type, COUNT(request_type) from pgh_311 GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
    
    """

    cur.execute(top_sql)
    r = cur.fetchall()
    records = pd.DataFrame(r)
    print(records)
    create_menu()


def requests_by_month():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()

    print()
    print("Request counts grouped by month: ")
    print()
    # top requests by month using date trunc to group by month
    month_sql = """
    
    SELECT DATE_TRUNC('month', DATE(created_on)), COUNT(request_type) from pgh_311 GROUP BY 1 ORDER BY 2 DESC;
    
    """

    cur.execute(month_sql)
    r = cur.fetchall()
    records = pd.DataFrame(r)
    print(records)
    create_menu()


def requests_by_loc():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()

    print()
    print("Request counts by neighborhood")
    print()
    # get request counts by location

    neigh_counts = """
    
    SELECT neighborhood_name, COUNT(id) FROM pgh_311 WHERE neighborhood_name is not NULL GROUP BY 1 ORDER BY 2 DESC;
    
    """

    cur.execute(neigh_counts)
    r = cur.fetchall()
    records = pd.DataFrame(r)
    print(records)
    create_menu()


def pop_and_requests():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()

    print(
        """
    
    Squirrel Hill (South) population and number of requests in 2021 so far:
    
    """
    )

    # join the 2 tables to get pop estimate and number of requests for a given location
    joined_sql = """
    
    SELECT n.neighborhood_name, n.pop_estimate, COUNT(p.*) 
    FROM neighborhoods n 
    INNER JOIN pgh_311 p 
    ON n.neighborhood_name = p.neighborhood_name
    WHERE n.neighborhood_name = 'Squirrel Hill South'
    GROUP BY 1,2
        
    """

    cur.execute(joined_sql)
    r = cur.fetchall()
    records = pd.DataFrame(r)
    print(records)
    create_menu()


def show_bar():

    # NOTE TO MR D: something is happening where my charts aren't printing until after the program ends intermittently.
    # Just noting in case this happens to you. Haven't been able to debug since it's only happened a few times so I think it's a notebook problem
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()

    # request bar chart SQL and chart
    request_sql = """

    SELECT request_type, COUNT(request_type) from pgh_311 GROUP BY 1 ORDER BY 2 DESC LIMIT 25;

    """

    cur.execute(request_sql)
    r = cur.fetchall()
    records = pd.DataFrame(r, columns=["count_of_requests", "request_type"])

    ax1 = records.plot.bar(x="count_of_requests", y="request_type")

    # population bar chart SQL and chart

    pop_sql = """

    SELECT neighborhood_name, pop_estimate FROM neighborhoods ORDER BY 2 DESC LIMIT 25;

    """

    cur.execute(pop_sql)
    r2 = cur.fetchall()
    records2 = pd.DataFrame(r, columns=["neighborhood_name", "pop_estimate"])

    ax1 = records2.plot.bar(x="neighborhood_name", y="pop_estimate", color="green")

    create_menu()


def create_menu():
    print()
    print(
        """
    
    Enter the number choice of what you'd like to do.
    1. Initialize the tables
    2. Insert records
    3. See a preview of the tables
    4. Show population and request counts from the highest populated neighborhood in Pittsburgh
    5. Show count of 311 requests by month
    6. Shoq top 5 neighborhoods with highest recorded population
    7. Show top 10 311 request types in 2021 with counts 
    8. Show 311 request counts by neighborhood 
    9. Show a bar chart of top 25 311 request types with counts for 2021
    10. Quit
    """
    )
    user_input = input("What would you like to do: ")
    if user_input == "1":
        create_tables()
    elif user_input == "2":
        insert_csv()
    elif user_input == "3":
        select_all()
    elif user_input == "4":
        pop_and_requests()
    elif user_input == "5":
        requests_by_month()
    elif user_input == "6":
        top_5_population()
    elif user_input == "7":
        top_10_requests()
    elif user_input == "8":
        requests_by_loc()
    elif user_input == "9":
        show_bar()
    elif user_input == "10":
        print(
            """
        Goodbye! For more interesting data about Pittsburgh check out https://data.wprdc.org/!
        They have most of their data available in CSVs and in public API endpoints.
        """
        )


def main():
    print()
    print(
        """
        Hello! This program let's you explore data about Pittsburgh neighborhoods. 
    To start, we are going to look at population and 311 call data from 2021. There's so many awesome stats to pull
    Data is this program came from WPRDC (https://data.wprdc.org/).
    
    
    """
    )
    create_menu()


main()
