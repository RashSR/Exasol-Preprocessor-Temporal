import pyexasol
import time

#Number of test loops
loopCount = 2

conn = pyexasol.connect(dsn='10.127.0.33', user='REINHOLD', password='REINHOLD', compression=True)
conn.execute("OPEN SCHEMA REINHOLD")

# CREATE-TEST
def createTest():
    start_time = time.time()

    for x in range(loopCount):
        createQuery = "CREATE TABLE Person_"+ str(x) + " (ID int, first_name VARCHAR(250), last_name VARCHAR(250));"
        conn.execute(createQuery)

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"CREATE: {elapsed_time:.2f} s")

# INSERT-TEST
def insertTest():
    start_time = time.time()
    
    for x in range(loopCount):
        insertQuery = "INSERT INTO Person_"+ str(x) + " VALUES (2, 'Emmet', 'Brown'), (3, 'Clara', 'Clayton');"
        conn.execute(insertQuery)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"INSERT: {elapsed_time:.2f} s")

# SELECT-Test
def selectTest():
    start_time = time.time()
    
    for x in range(loopCount):
        selectQuery = "SELECT * FROM Person_" + str(x) + ";"
        conn.execute(selectQuery)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"SELECT: {elapsed_time:.2f} s")

# UPDATE-TEST
def updateTest():
    start_time = time.time()
    
    for x in range(loopCount):
        updateQuery = "UPDATE Person_" + str(x) + " SET last_name = 'Clayton-Brown' WHERE id = 3;"
        conn.execute(updateQuery)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"UPDATE: {elapsed_time:.2f} s")

# DELETE-TEST
def deleteTest():
    start_time = time.time()
    
    for x in range(loopCount):
        deleteQuery = "DELETE FROM Person_" + str(x) + ";"
        conn.execute(deleteQuery)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"DELETE: {elapsed_time:.2f} s")

#Clean-Up
def cleanUp(isPreprocessorActive: bool = False):
    for x in range(loopCount):
        conn.execute("DROP TABLE Person_" + str(x) + ";")
        if(isPreprocessorActive):
            conn.execute("DROP VIEW HIST_Person_" + str(x) + ";")

    print("Removed all created data.")

# test performance WITHOUT Preprocessor
print("Start tests WITHOUT Preprocessor for " + str(loopCount) + " loops.")
conn.execute("ALTER SESSION SET sql_preprocessor_script = null;")

createTest()
insertTest()
selectTest()
updateTest()
deleteTest()
cleanUp()

# test performance WITH Preprocessor
print("Start tests WITH Preprocessor for " + str(loopCount) + " loops.")
isPreprocessorActive = True
conn.execute("ALTER SESSION SET sql_preprocessor_script = myPreprocessor;")

createTest()
insertTest()
selectTest()
updateTest()
deleteTest()
cleanUp(isPreprocessorActive)
