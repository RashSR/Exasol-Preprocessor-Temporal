import pyexasol
import time

#Number of test loops
loopCount = 100

conn = pyexasol.connect(dsn='myDns, user='myUser', password='myPw', compression=True)
conn.execute("OPEN SCHEMA REINHOLD")

# Clean up
def cleanUp(isPreprocessorActive: bool = False):
    if(isPreprocessorActive):
        conn.execute("DROP VIEW Person;")
        conn.execute("DROP TABLE HIST_Person;")
    else:
        conn.execute("DROP TABLE Person;")

# CREATE-TEST
def createTest(isPreprocessorActive: bool = False):
    
    fullTime = 0
    
    for x in range(loopCount):
        startTime = time.time()
        createQuery = "CREATE TABLE Person (ID int, first_name VARCHAR(250), last_name VARCHAR(250));"
        conn.execute(createQuery)
        endTime = time.time()
        elapsedTime = endTime - startTime
        fullTime += elapsedTime
        cleanUp(isPreprocessorActive)

    print(f"CREATE: {fullTime:.2f} s")
    return fullTime
    
# INSERT-TEST
def insertTest(isPreprocessorActive: bool = False):
    
    fullTime = 0
    createQuery = "CREATE TABLE Person (ID int, first_name VARCHAR(250), last_name VARCHAR(250));"
    conn.execute(createQuery)
    
    for x in range(loopCount):
        startTime = time.time()
        insertQuery = "INSERT INTO Person VALUES (2, 'Emmet', 'Brown'), (3, 'Clara', 'Clayton');"
        conn.execute(insertQuery)
        endTime = time.time()
        elapsedTime = endTime - startTime
        fullTime += elapsedTime
        conn.execute("DELETE FROM Person;")
        
    cleanUp(isPreprocessorActive)
    print(f"INSERT: {fullTime:.2f} s")
    return fullTime

# SELECT-Test
def selectTest(isPreprocessorActive: bool = False):

    fullTime = 0
    createQuery = "CREATE TABLE Person (ID int, first_name VARCHAR(250), last_name VARCHAR(250));"
    conn.execute(createQuery)
    insertQuery = "INSERT INTO Person VALUES (2, 'Emmet', 'Brown'), (3, 'Clara', 'Clayton');"
    conn.execute(insertQuery)
    
    for x in range(loopCount):
        startTime = time.time()
        selectQuery = "SELECT * FROM Person;"
        conn.execute(selectQuery)
        endTime = time.time()
        elapsedTime = endTime - startTime
        fullTime += elapsedTime

    cleanUp(isPreprocessorActive)
    print(f"SELECT: {fullTime:.2f} s")
    return fullTime

# UPDATE-TEST
def updateTest(isPreprocessorActive: bool = False):
    
    fullTime = 0
    createQuery = "CREATE TABLE Person (ID int, first_name VARCHAR(250), last_name VARCHAR(250));"
    conn.execute(createQuery)

    for x in range(loopCount):
        insertQuery = "INSERT INTO Person VALUES (2, 'Emmet', 'Brown'), (3, 'Clara', 'Clayton');"
        conn.execute(insertQuery)
        startTime = time.time()
        updateQuery = "UPDATE Person SET last_name = 'Clayton-Brown' WHERE id = 3;"
        conn.execute(updateQuery)
        endTime = time.time()
        elapsedTime = endTime - startTime
        fullTime += elapsedTime
        conn.execute("DELETE FROM Person;")
    
    cleanUp(isPreprocessorActive)
    print(f"UPDATE: {fullTime:.2f} s")
    return fullTime

# DELETE-TEST
def deleteTest(isPreprocessorActive: bool = False):
    
    fullTime = 0
    createQuery = "CREATE TABLE Person (ID int, first_name VARCHAR(250), last_name VARCHAR(250));"
    conn.execute(createQuery)

    for x in range(loopCount):
        insertQuery = "INSERT INTO Person VALUES (2, 'Emmet', 'Brown'), (3, 'Clara', 'Clayton');"
        conn.execute(insertQuery)
        startTime = time.time()
        deleteQuery = "DELETE FROM Person;"
        conn.execute(deleteQuery)
        endTime = time.time()
        elapsedTime = endTime - startTime
        fullTime += elapsedTime

    cleanUp(isPreprocessorActive)
    print(f"DELETE: {fullTime:.2f} s")
    return fullTime

# test performance WITHOUT Preprocessor
print("Start tests WITHOUT Preprocessor for " + str(loopCount) + " loops.")
conn.execute("ALTER SESSION SET sql_preprocessor_script = null;")

t1WithoutPP = createTest()
t2WithoutPP = insertTest()
t3WithoutPP = selectTest()
t4WithoutPP = updateTest()
t5WithoutPP = deleteTest()

tGesWithoutPP = t1WithoutPP + t2WithoutPP + t3WithoutPP + t4WithoutPP + t5WithoutPP
print(f"Full time for all tests WITHOUT Preprocessor: {tGesWithoutPP:.2f} s")

# test performance WITH Preprocessor
print("---------------------------------------------------------------------")
print("Start tests WITH Preprocessor for " + str(loopCount) + " loops.")
isPreprocessorActive = True
conn.execute("ALTER SESSION SET sql_preprocessor_script = mypreprocessor;")

t1WithPP = createTest(isPreprocessorActive)
t2WithPP = insertTest(isPreprocessorActive)
t3WithPP = selectTest(isPreprocessorActive)
t4WithPP = updateTest(isPreprocessorActive)
t5WithPP = deleteTest(isPreprocessorActive)

tGesWithPP = t1WithPP + t2WithPP + t3WithPP + t4WithPP + t5WithPP
print(f"Full time for all tests WITH Preprocessor: {tGesWithPP:.2f} s")

print("---------------------------------------------------------------------")
print("Performance analysis:")
print(f"Performance loss CREATE: {t1WithPP/t1WithoutPP:.2f}")
print(f"Performance loss INSERT: {t2WithPP/t2WithoutPP:.2f}")
print(f"Performance loss SELECT: {t3WithPP/t3WithoutPP:.2f}")
print(f"Performance loss UPDATE: {t4WithPP/t4WithoutPP:.2f}")
print(f"Performance loss DELETE: {t5WithPP/t5WithoutPP:.2f}")
print(f"Performance loss overall: {tGesWithPP/tGesWithoutPP:.2f}")
