import csv
import random

import mysql.connector
from faker import Faker

db = mysql.connector.connect(
        host="34.94.182.22",
        user="myappuser",
        passwd="barfoo",
        database="Students"
    )

def begin():
    fileName = input("Enter filename (with .csv): ")
    recordCount = int(input("Enter number of records wanted: "))
    main(recordCount, fileName)

def main(records, fileName):
    genData(records, fileName)
    importData(records, fileName)

def genData(records, fileName):
    fake = Faker()

    customerID = []
    productID = []
    manuID = []
    locationID = []
    transID = []
    counter = 0

    csv_file = open(fileName,"w")
    writer = csv.writer(csv_file)
    writer.writerow(["ProductID","ProductName","manuId"])
    for x in range(0, records):
        counter = counter + 1
        temp = counter
        temp1 = counter

        productID.append(temp)
        manuID.append(temp1)

        writer.writerow([temp,fake.name(),temp1])
    writer.writerow(["CustomerID", "CustomerName", "CustomerLocationID"])
    counter = 0
    for x in range(0, records):
        counter = counter + 1
        temp = counter
        temp1 = counter

        customerID.append(temp)
        locationID.append(temp1)

        writer.writerow([temp,fake.name(),temp1])
    writer.writerow(["TransID", "CustomerID", "ProductID"])
    counter = 0
    for x in range(0, records):
        counter = counter + 1
        temp = counter

        transID.append(temp)

        writer.writerow([temp, customerID[x], productID[x]])
    writer.writerow(["ManuID", "ManuName", "ManuCountry"])
    for x in range(0, records):
        writer.writerow([manuID[x],fake.name(),fake.country()])

    writer.writerow(["LocationID", "LocationName"])
    for x in range(0, records):
        writer.writerow([locationID[x],fake.name()])

def dropData():
    curr = db.cursor()
    curr.execute("Drop TABLE Main")
    curr.close()

    curr = db.cursor()
    curr.execute("Drop Table Manufacturer")
    curr.close

    curr = db.cursor()
    curr.execute("Drop Table Location")
    curr.close

    curr = db.cursor()
    curr.execute("Drop TABLE Product")
    curr.close()

    curr = db.cursor()
    curr.execute("Drop TABLE Customer")
    curr.close()

def importData(records, fileName):
    curr = db.cursor()
    curr.execute("CREATE TABLE Product(ProductID INT PRIMARY KEY, Product_Name VARCHAR(30),ProductManuID INT UNIQUE)")
    curr.execute("CREATE TABLE Customer(CustomerID INT PRIMARY KEY, Customer_Name VARCHAR(30),Customer_Location_ID INT UNIQUE)")
    curr.execute("CREATE TABLE Main(TransID INT PRIMARY KEY, CustomerID INT, ProductID INT, FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID), FOREIGN KEY(ProductID) REFERENCES Product(ProductID))")
    curr.execute("CREATE TABLE Manufacturer(ManuID INT UNIQUE, ManuName VARCHAR(30),ManuCountry VARCHAR(50), FOREIGN KEY(ManuID) REFERENCES Product(ProductManuID))")
    curr.execute("CREATE TABLE Location(LocationID INT UNIQUE, LocationName VARCHAR(30), FOREIGN KEY(LocationID) REFERENCES Customer(Customer_Location_ID))")

    count = 0

    stop1 = records + 1
    stop2 = (records + 1) * 2
    stop3 = (records + 1) * 3
    stop4 = (records + 1) * 4

    stop5 = (records * 2) + 1
    stop6 = (records * 3) + 2
    stop7 = (records * 4) + 3

    with open(fileName) as csvFile:
        reader = csv.DictReader(csvFile)

        for row in reader:
            count = count + 1
            scount = str(count)
            print("Row count: " + scount)
            print(row)

            if count == stop1:
                reader.fieldnames = "CustomerID", "CustomerName", "CustomerLocationID"
                continue
            if count == stop2:
                reader.fieldnames = "TransID", "CustomerID", "ProductID"
                continue
            if count == stop3:
                reader.fieldnames = "ManuID", "ManuName", "ManuCountry"
                continue
            if count == stop4:
                reader.fieldnames = "LocationID", "LocationName"
                continue
            if count <= records:
                print("Executed 1")
                curr.execute("INSERT INTO Product(ProductID, Product_Name, ProductManuID) " "VALUES (%s,%s,%s);", (row['ProductID'],row['ProductName'],row['manuId']))
            if (count > records) & (count < stop2):
                print("Executed 2")
                curr.execute("INSERT INTO Customer(CustomerID, Customer_Name, Customer_Location_ID) " "VALUES (%s,%s,%s);", (row['CustomerID'], row['CustomerName'], row['CustomerLocationID']))
            if (count > stop5) & (count < stop3):
                print("Executed 3")
                curr.execute("INSERT INTO Main(TransID, CustomerID, ProductID) " "VALUES (%s,%s,%s);", (row['TransID'], row['CustomerID'], row['ProductID']))
            if (count > stop6) & (count < stop4):
                print("Executed 4")
                curr.execute("INSERT INTO Manufacturer(ManuID, ManuName, ManuCountry) " "VALUES (%s,%s,%s);", (row['ManuID'], row['ManuName'], row['ManuCountry']))
            if count > stop7:
                print("Executed 5")
                curr.execute("INSERT INTO Location(LocationID, LocationName) " "VALUES (%s,%s);", (row['LocationID'], row['LocationName']))

    curr.close()
    db.commit()

begin()
