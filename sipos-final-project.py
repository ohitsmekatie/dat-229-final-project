# import necessary modules

import psycopg2
import csv
from csv import reader
import os
import pandas as pd

# db connection string
connstring = "host=localhost dbname=pghdata user=pyuser password=passwrd"

# creating dataframes outside of my functions so I can use them for various analyses where I don't want to use SQL directly from the DB

neighborhoods_df = pd.read_csv("neighborhoods_pop.csv")
salaries_df = pd.read_csv("pgh_salaries.csv")


def create_tables():
    # establish a connection with the connstring variable set above
    dbconn = psycopg2.connect(connstring)
    # create cursor with that connection
    cur = dbconn.cursor()

    # SQL to create the tables
    # nid and ids in other tables are the same from setting up the CSV instead of auto generating
    create_sql = """
        DROP TABLE IF EXISTS salaries, neighborhoods;

        CREATE TABLE neighborhoods (
            nid INT,
			neighborhood_name VARCHAR(50) NOT NULL,
            pop_estimate INT
        );

        CREATE TABLE salaries (
            snid INT,
            neighborhood_name VARCHAR(50) NOT NULL,
            total_salary_count INT,
            total_less_10 INT,
            total_10_to_29 INT,
            total_30_to_49 INT,
            total_50_to_74 INT,
            total_75_to_99 INT,
            total_100_to_124 INT,
            total_125_to_149 INT,
            total_above_150 INT
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
    print("The salaries table has been initialized")
    print()
    create_menu()


def insert_csv():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()

    with open("neighborhoods_pop.csv", "r") as f:
        # skip the header row so it uses the names I have above
        next(f)
        cur.copy_from(f, "neighborhoods", sep=",")

    dbconn.commit()
    print()
    print("The neighborhood table data has been inserted from the csv.")
    print()

    with open("pgh_salaries.csv", "r") as f:
        next(f)
        cur.copy_from(f, "salaries", sep=",")

    dbconn.commit()
    cur.close()
    dbconn.close()
    print("The salary table data has been inserted from the csv.")
    print()

    create_menu()


def select_all():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()
    # select everything from the neighborhoods table, limit to 5
    neighborhood_all = """
        SELECT * FROM neighborhoods LIMIT 5;
    """

    cur.execute(neighborhood_all)
    records = cur.fetchall()
    dbconn.commit()
    print()
    print("Printing a sample of the neighborhoods table:")
    print()
    for record in records:
        print(record)
    print()

    # select everything from the salaries table, limiting it to 5
    salaries_all = """
        SELECT * FROM salaries LIMIT 5;
    """
    cur.execute(salaries_all)
    records2 = cur.fetchall()
    dbconn.commit()
    cur.close()
    dbconn.close()
    print()
    print("Printing a sample of the salaries table:")
    print()
    for record in records2:
        print(record)
    print()
    # take user back to main menu
    create_menu()


def print_column_names(neighborhoods_df, salaries_df):
    # print column names of neighborhood table
    print(
        """
    ########## NEIGHBORHOOD TABLE COLUMNS ##########
    ################################################
    """
    )
    for col in neighborhoods_df.columns:
        print(col)

    # print column names of salaries table
    print(
        """
    ########## SALARY TABLE COLUMNS ##########
    ##########################################

    Note: each column represents a salary range - outside of id, neighborhood, and total and the fields are the total count of humans in a given area that falls into that range
    """
    )

    for col in salaries_df.columns:
        print(col)

    print()
    print()


def average_pop():
    dbconn = psycopg2.connect(connstring)
    cur = dbconn.cursor()
    avg_sql = """
        SELECT ROUND(AVG(pop_estimate),2) FROM neighborhoods;
    """

    cur.execute(avg_sql)
    records = cur.fetchall()
    # TODO if I have time: fix formatting
    print(records)


def salary_stats():
    print("Salary stats. ")


def create_menu():
    print(
        """
    Hello! This program let's you explore data about Pittsburgh neighborhoods. Data is this program came from WPRDC (https://data.wprdc.org/).
    Enter the number choice of what you'd like to do.
    1. Initialize the tables
    2. Insert records
    3. See a preview of the tables
    4. List all neighborhoods and populations
    5. Print the average population of Pittsburgh neighborhoods
    6. Print stats on neighborhood salary data
    7. Quit
    """
    )
    user_input = input("What would you like to do: ")
    if user_input == "1":
        create_tables()
    elif user_input == "2":
        insert_csv()
    elif user_input == "3":
        print_column_names(neighborhoods_df, salaries_df)
    elif user_input == "5":
        average_pop()
    elif user_input == "6":
        salary_stats()
    elif user_input == "7":
        print(
            """
        Goodbye! For more interesting data about Pittsburgh check out https://data.wprdc.org/!
        They have most of their data available in CSVs and in public API endpoints.
        """
        )


def main():
    create_menu()


main()
