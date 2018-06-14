import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Java project for CS 157A
 * Group Number: 6
 * Professor: Ahmed Ezzat
 * Goal: Use JDBC to create a Books database, populate it, and then execute different
 * SQL statements to query or manipulate the Books database.
 */
public class jdbc {
    public static void main(String[] args) {

        // Establish a process ( i.e. connection to local mysql database )
        Process process = new Process();

        try {
            // Enter user and password for sql connection. Change this to make it work locally.
            process.connection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "Password123");
            process.statement = process.connection.createStatement();

            // Initialize authorScanners to parse data files containing information to populate into tables
            Scanner authorScan = new Scanner(new File("authorList.txt"));
            Scanner publisherScan = new Scanner(new File("publishers.txt"));
            Scanner isbnScan = new Scanner(new File("authorISBN.txt"));
            Scanner titleScan = new Scanner(new File("titleTable.txt"));

            // if the books database already exists from a previous run, drop it so a new one can be created.
            ResultSet result = process.connection.getMetaData().getCatalogs();
            while (result.next()) {
                String databases = result.getString(1);
                if (databases.contains("BOOKS")) {
                    process.statement.executeUpdate("DROP DATABASE BOOKS;");
                }
            }

            // ---------- CREATE DATABASE BOOKS AND INITIALIZE TABLES ----------
            // Initialization of database "books" and tables
            process.statement.executeUpdate("CREATE DATABASE BOOKS");
            String useDB = "USE books";
            String authors = "CREATE TABLE authors (authorID INTEGER NOT NULL auto_increment, first CHAR(20) NOT NULL, last CHAR(20) NOT NULL, PRIMARY KEY (authorID))";
            String authorISBN = "CREATE TABLE authorISBN (authorID INTEGER NOT NULL, isbn CHAR(10) NOT NULL, PRIMARY KEY (isbn))";
            String title = "CREATE TABLE title (isbn CHAR(10) NOT NULL, title VARCHAR(500) NOT NULL, editionNumber INTEGER NOT NULL, Year CHAR(4) NOT NULL, publisherID INTEGER NOT NULL, price FLOAT NOT NULL, PRIMARY KEY (isbn))";
            String publishers = "CREATE TABLE publishers( publisherID INTEGER NOT NULL auto_increment, publisherName CHAR(100) NOT NULL, PRIMARY KEY (publisherID))";
            process.statement.execute(useDB);

            // Print out the queries and the creation of the database
            System.out.println("CREATE DATABASE BOOKS");
            System.out.println("authorID INTEGER NOT NULL auto_increment, first CHAR(20) NOT NULL, last CHAR(20) NOT NULL, PRIMARY KEY (authorID)");
            System.out.println("CREATE TABLE authorISBN (authorID INTEGER NOT NULL, isbn CHAR(20) NOT NULL, PRIMARY KEY (isbn))");
            System.out.println("CREATE TABLE title (isbn CHAR(20) NOT NULL, title VARCHAR(255) NOT NULL, editionNumber INTEGER NOT NULL, Year CHAR(4) NOT NULL, publisherID INTEGER NOT NULL, price FLOAT NOT NULL)");
            System.out.println("CREATE TABLE publishers( publisherID INTEGER NOT NULL auto_increment, publisherName CHAR(100) NOT NULL, PRIMARY KEY (publisherID))");

            // Print out the execution of the create statements
            System.out.println(useDB + ";\n" +
                    authors + ";\n" +
                    authorISBN + ";\n" +
                    title + ";\n" +
                    publishers + ";\n");

            // execute all statements define for creating the books database and empty tables
            process.statement.execute(authors);
            process.statement.execute(title);
            process.statement.execute(authorISBN);
            process.statement.execute(publishers);

            // ---------- INITIALIZE TABLES WITH AT LEAST 15 ENTRIES ----------
            // insert all authors(first and last name) read from a file and inserted into the authors table
            while (authorScan.hasNextLine()) {
                String next = authorScan.nextLine();
                String[] hold = next.split(" ");
                String firstN = hold[0]; // hold first name
                String lastN = hold[1]; // hold last name
                process.statement.execute("Insert INTO authors(first, last) VALUES ('" + firstN + "','" + lastN + "')");
                System.out.println("Insert INTO authors(first, last) VALUES ('" + firstN + "','" + lastN + "')");
            }
            System.out.println();

            // insert all publishers read from a file and inserted into the publishers table
            while (publisherScan.hasNextLine()) {
                String next = publisherScan.nextLine();
                process.statement.execute("Insert INTO publishers(publisherName) VALUES ('" + next + "');");
                System.out.println("Insert INTO publishers(publisherName) VALUES ('" + next + "');");
            }
            System.out.println();

            // Create author ID and arrayList of author IDs
            int AIDHold = 0; // author ID hold
            ArrayList<Integer> AID = new ArrayList<Integer>();

            // Select all author IDs from authors table
            ResultSet authorResultSet = process.statement.executeQuery("SELECT authorID FROM authors;");
            System.out.println("SELECT authorID FROM authors;\n");
            System.out.println("authorID\n");

            // iterate through author IDs and add them to array list
            while (authorResultSet.next()) {
                AID.add(authorResultSet.getInt("authorID"));
                System.out.println(authorResultSet.getString("authorID"));
            }
            System.out.println();

            // iterate through the isbn list file and insert them into the author isbn table
            while (isbnScan.hasNextLine()) {
                String next = isbnScan.nextLine();
                process.statement.execute("Insert INTO authorISBN(authorID,isbn) VALUES ('" + AID.get(AIDHold) + "','" + next + "');");
                System.out.println("Insert INTO authorISBN(authorID,isbn) VALUES ('" + AID.get(AIDHold) + "','" + next + "');");
                AIDHold++;
            }
            System.out.println();

            // instantiate arrayList and temp variable for publisher IDs
            int PIhold = 0;
            ArrayList<Integer> PID = new ArrayList<Integer>();

            // iterate through the publisher ID list file and insert them into the publisher ID list
            ResultSet rs = process.statement.executeQuery("SELECT publisherID FROM publishers;");
            System.out.println("SELECT publisherID FROM publishers;\n");
            System.out.println("publisherID\n");

            while (rs.next()) {
                PID.add(rs.getInt("publisherID"));
                System.out.println(rs.getInt("publisherID"));
            }
            System.out.println();

            // iterate through the file, break up the string and get specific variables to insert into table
            while (titleScan.hasNextLine()) {
                String next = titleScan.nextLine();
                String[] hold = next.split("_"); // array of values read in from file
                String isbn = hold[0]; // isbn
                String title2 = hold[1]; // title of book
                int edition = Integer.parseInt(hold[2]); // edition of the book
                String year = hold[3]; // year the book was created
                int pid = Integer.parseInt(hold[4]); // publisher id
                float price = Float.parseFloat(hold[5]); // price of the book

                process.statement.execute("INSERT INTO title(isbn, title, editionNumber, Year, publisherID, price)VALUES ('" + isbn + "','" + title2 +
                        "','" + edition + "','" + year + "','" + PID.get(pid) + "','" + price + "')");
                System.out.println("INSERT INTO title(isbn, title, editionNumber, Year, publisherID, price)VALUES ('" + isbn + "','" + title2 +
                        "','" + edition + "','" + year + "','" + PID.get(pid) + "','" + price + "')");
                PIhold++;
            }
            System.out.println();

            // ---------- IMPLEMENTING SQL QUERIES TO MANIPULATE THE DATABASE ----------

            // Step 1
            // select all authors from the authors table alphabetically
            ResultSet authorPrint = process.statement.executeQuery("Select last, first from authors order by last, first;");
            System.out.println("Select last, first from authors order by last, first;\n");
            System.out.printf("%-20s %s\n", "LastName", "FirstName");
            System.out.println();

            while (authorPrint.next()) {
                System.out.printf("%-20s %s\n", authorPrint.getString("last"), authorPrint.getString("first"));
            }

            // Step 2
            // Select all publishers from the publishers table
            ResultSet publisherList = process.statement.executeQuery("Select publisherName from publishers;");
            System.out.println();
            System.out.println("Select publisherName from publishers;\n");
            System.out.println("publisherName\n");
            while (publisherList.next()) {
                System.out.println(publisherList.getString("publisherName"));
            }
            System.out.println();

            // Step 3
            // select a specific publisher and list all books published by the publisher
            // include the title, year, and ISBN number. order the information alphabetically by title
            ResultSet publisherID = process.statement.executeQuery("select publisherID from publishers Where publisherName = 'ThomasReuters';");
            System.out.println("select publisherID from publishers Where publisherName = 'ThomasReuters';\n");
            System.out.println("publisherID\n");
            int PID2 = 0;
            while (publisherID.next()) {
                PID2 = publisherID.getInt("publisherID");
                System.out.println(publisherID.getInt("publisherID"));
            }
            System.out.println();

            // Step 3 (continued)
            // using the publisher id, get titles made by the publishers, along with other information
            ResultSet titleSet = process.statement.executeQuery("select title, year, isbn from title where publisherID = " + PID2 + " order by title;");
            System.out.println("select title, year, isbn from title where publisherID = " + PID2 + " order by title;");
            System.out.println();
            System.out.printf("%-30s %-5s %s \n", "title", "year", "isbn");

            while (titleSet.next()) {
                System.out.printf("%-30s %-5s %s \n",titleSet.getString("title"), titleSet.getInt("Year"), titleSet.getString("isbn"));
            }
            System.out.println();

            // Step 4
            // adds new author and other information related to other tables
            process.statement.execute("Insert INTO authors(first, last) VALUES ('" + "Tom" + "','" + "Tran" + "')");
            System.out.println("Insert INTO authors(first, last) VALUES ('" + "Tom" + "','" + "Tran" + "')\n");
            ResultSet updateID = process.statement.executeQuery("select authorID from authors where first = 'Tom' AND last = 'Tran';");

            int insertID = 0;
            while (updateID.next()) {
                insertID = updateID.getInt("authorID");
            }

            // Step 4 (continued)
            // update the isbn of our newly inserted author
            process.statement.execute("Insert INTO authorISBN(authorID,isbn) VALUES ('" + insertID + "','" + "NULL" + "');");
            System.out.println("Insert INTO authorISBN(authorID,isbn) VALUES ('" + insertID + "','" + "NULL" + "');\n");

            // get author name form specified id number
            ResultSet beforeUpdate = process.statement.executeQuery("Select * from authors where authorID = 16;");
            System.out.println("Select * from authors where authorID = 16;\n");

            while (beforeUpdate.next()) {
                System.out.println();
                System.out.println("Before Edit/Update the existing information about an author");
                System.out.printf("%-10s %-10s %s \n","authorID", "first", "last");
                System.out.printf("%-10s %-10s %s \n", beforeUpdate.getInt("authorID"), beforeUpdate.getString("first"), beforeUpdate.getString("last"));
            }

            // Step 5
            // edit/update the existing information about an author i..e the name
            process.statement.execute("UPDATE authors SET last='Dinh' where authorID=16;");
            System.out.println();
            System.out.println("UPDATE authors SET last='Dinh' where authorID=16;\n");

            // Step 5 (continued)
            // check to make sure the update happened
            ResultSet afterUpdate = process.statement.executeQuery("Select * from authors where authorID = 16;");
            System.out.println("Select * from authors where authorID = 16;\n");

            while (afterUpdate.next()) {
                System.out.println();
                System.out.println("After Edit/Update the existing information about an author");
                System.out.printf("%-10s %-10s %s \n","authorID", "first", "last");
                System.out.printf("%-10s %-10s %s \n", afterUpdate.getInt("authorID"), afterUpdate.getString("first"), afterUpdate.getString("last"));
            }
            ResultSet bu = process.statement.executeQuery("Select * from authorisbn where authorID = 16;");

            // print the entire result set from the authors isbn table where the author id is 16
            while (bu.next()) {
                System.out.println();
                System.out.println("Before Edit/Update the existing information about an authorisbn");
                System.out.printf("%-10s %-10s \n", "authorID", "isbn");
                System.out.printf("%-10s %-10s \n", bu.getString("authorID"), bu.getString("isbn"));
            }

            // set new isbn for new author
            process.statement.execute("UPDATE authorisbn SET isbn = '5555555555' where authorID = 16;");
            System.out.println();
            System.out.println("UPDATE authorisbn SET isbn = '5555555555' where authorID = 16;");

            // get all isbns depending on author id
            ResultSet au = process.statement.executeQuery("Select * from authorisbn where authorID = 16;");

            while (au.next()) {
                System.out.println();
                System.out.println("After Edit/Update the existing information about an authorisbn");
                System.out.printf("%-10s %-10s \n", "authorID", "isbn");
                System.out.printf("%-10s %-10s \n", au.getString("authorID"), au.getString("isbn"));
            }

            // Step 6
            // add a new title for an author
            System.out.println();
            process.statement.execute("INSERT INTO title(isbn, title, editionNumber, Year, publisherID, price)VALUES ('5555555555', 'My Life' ,'1' ,'2017' ,16 ,'20.00');");
            System.out.println("INSERT INTO title(isbn, title, editionNumber, Year, publisherID, price)VALUES ('5555555555', 'My Life' ,'1' ,'2017' ,16 ,'20.00');");
            System.out.println();

            ResultSet addNew = process.statement.executeQuery("select * from title where publisherID = 16;");
            System.out.println("select * from title where publisherID = 16;");

            while (addNew.next()) {
                System.out.println();
                System.out.println("Add new information about an title");
                System.out.printf("%-10s %-10s %-15s %-5s %-20s %-10s \n", "isbn", "title", "editionNumber", "Year", "publisherID", "price");
                System.out.printf("%-10s %-10s %-15s %-5s %-20s %-10s \n", addNew.getString("isbn"), addNew.getString("title"), addNew.getString("editionNumber"), addNew.getString("Year"), addNew.getString("publisherID"), addNew.getString("price"));
            }
            process.statement.execute("UPDATE authorisbn SET isbn = '5555555555' where authorID = 16;");

            // Step 7
            // add new publish (adds new publisher and update new information for other tables)
            process.statement.execute("Insert INTO publishers(publisherName) VALUES ('" + "Khois work" + "');");
            System.out.println();
            System.out.println("Insert INTO publishers(publisherName) VALUES ('" + "Khois work" + "');");

            ResultSet beforeUp = process.statement.executeQuery("select * from publishers where publisherID = 16;");
            System.out.println("select * from publishers where publisherID = 16;");

            while (beforeUp.next()) {
                System.out.println();
                System.out.println("Before Edit/Update the existing information about an publishers");
                System.out.printf("%-10s %-10s \n", "publisherID", "publisherName");
                System.out.printf("%-10s %-10s \n", beforeUp.getString("publisherID"), beforeUp.getString("publisherName"));
            }

            System.out.println();

            // Step 8
            // edit/update the existing information about a publisher
            process.statement.execute("UPDATE publishers SET publisherName = 'Kate Dinh' where publisherID = '16';");
            System.out.println("UPDATE publishers SET publisherName = 'Kate Dinh' where publisherID = '16';");

            ResultSet afterUp = process.statement.executeQuery("select * from publishers where publisherID = 16;");
            System.out.println("select * from publishers where publisherID = 16;");

            while (afterUp.next()) {
                System.out.println();
                System.out.println("After Edit/Update the existing information about an publishers");
                System.out.printf("%-10s %-10s \n", "publisherID", "publisherName");
                System.out.printf("%-10s %-10s \n", afterUp.getString("publisherID"), afterUp.getString("publisherName"));
            }

            process.statement.execute("Insert INTO publishers(publisherName) VALUES ('" + "Khois work" + "');");

            process.statement.execute("UPDATE publishers SET publisherName = 'Kate Dinh' where publisherID = '16'; ");
            System.out.println();

            // catch any exceptions that might occur
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            process.statement.close();
            process.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
