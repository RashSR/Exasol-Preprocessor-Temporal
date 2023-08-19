import org.junit.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Stream;

import static junit.framework.TestCase.*;

public class SqlTests {

    //region Fields
    private static Connection connection;
    private static Statement statement;
    private static ResultSet resultSet;

    //endregion

    //region Setup and Teardown
    @BeforeClass
    public static void oneTimeSetUp()
    {
        // one-time initialization code
        try
        {
            connection = DriverManager.getConnection(
                    "jdbc:exa:192.168.56.101/02F61E335534AF0C9CE92A35EA597DEA9C3AFB7C7603397ECAAC471E43CF4B65;schema=TEST",
                    "sys",
                    "exasol"
            );
            statement = connection.createStatement();
            resultSet = null;
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void oneTimeTearDown(){
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void teardown() {
        resultSet = null;
    }

    //endregion

    //region Library
    @Test
    public void testExtractTableNameAndSqlCommandScript()
    {
        //Arrange
        ArrayList<String> output = new ArrayList<>();
        String tableName = "\"t\"";
        //String tableName = "new\"Name";
        String inputCommand = "UPDATE " + tableName + " SET name = ''Martin'' WHERE name = ''Peter''";
        String sqlCommand = inputCommand.replace(tableName, tableName.toUpperCase()).replace("''", "'");
        String histTableName = "HIST_" + tableName.toUpperCase();
        String newSqlCommand = inputCommand.replace(tableName, histTableName).replace("''", "'");

        //Act
        try
        {
            statement.executeUpdate("CREATE OR REPLACE SCRIPT testExtractTableNameAndSqlCommand(sqlCommand) AS " +
                    "import('TEST.HISTORYLIB', 'history_lib') " +
                    "tableName, sqlCommand, histTableName, newSqlCommand = history_lib.extractTableNameAndSqlCommand(sqlCommand) " +
                    "output(tableName)" +
                    "output(sqlCommand)" +
                    "output(histTableName)" +
                    "output(newSqlCommand)");
            resultSet = statement.executeQuery("EXECUTE SCRIPT testExtractTableNameAndSqlCommand('" + inputCommand + "') with output;");

            while(resultSet.next()){
                output.add(resultSet.getString("OUTPUT"));
            }

            statement.executeUpdate("DROP SCRIPT testExtractTableNameAndSqlCommand");
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertEquals(4, output.size());
        assertEquals(tableName.toUpperCase(), output.get(0));
        assertEquals(sqlCommand, output.get(1));
        assertEquals(histTableName, output.get(2));
        assertEquals(newSqlCommand, output.get(3));
    }

    @Test
    public void testAllColumnsScript()
    {
        //Arrange
        String output;

        //Act
        try
        {
            statement.executeUpdate("CREATE TABLE MyNewTestTbl (id INT, name VARCHAR(32))");
            statement.executeUpdate("CREATE OR REPLACE SCRIPT testAllColumns(tableName) AS import('TEST.HISTORYLIB', 'history_lib') output(table.concat(history_lib.getAllColumns(tableName), ', '))");

            resultSet = statement.executeQuery("EXECUTE SCRIPT testAllColumns('MyNewTestTbl') with output;");
            resultSet.next();
            output = resultSet.getString("OUTPUT");
            statement.executeUpdate("DROP TABLE MyNewTestTbl");
            statement.executeUpdate("DROP SCRIPT testAllColumns");
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertTrue(output.equalsIgnoreCase("id, name"));
    }


    private static Stream<Arguments> isKeywordParameters()
    {
        return Stream.of(
                Arguments.of("'CREATE TABLE testTable (id INT, name VARCHAR(32))', 'CREATE', 1", true),
                Arguments.of("'CREATE TABLE testTable (id INT, name VARCHAR(32))', 'CREATE', 2", false),
                Arguments.of("'CREATE TABLE testTable (id INT, name VARCHAR(32))', 'TABLE', 1", false),
                Arguments.of("'CREATE TABLE testTable (id INT, name VARCHAR(32))', 'TABLE', 2", true),
                Arguments.of("'INSERT INTO testTable (id) VALUES (1)', 'into', 2", true)
        );
    }

    @ParameterizedTest
    @MethodSource("isKeywordParameters")
    public void testIsKeyword(String sqlCommand, boolean assertion)
    {
        //Arrange
        oneTimeSetUp();
        boolean output;

        //Act
        try
        {
            statement.executeUpdate("CREATE OR REPLACE SCRIPT testIsKeyword(sqlCommand, keyword, pos) AS import('TEST.HISTORYLIB', 'history_lib') output(history_lib.isKeyword(sqlCommand, keyword, pos))");
            resultSet = statement.executeQuery("EXECUTE SCRIPT testIsKeyword("+sqlCommand+") with output;");
            resultSet.next();
            output = resultSet.getBoolean("OUTPUT");
            statement.executeUpdate("DROP SCRIPT testIsKeyword");
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertEquals(assertion, output);
    }

    //endregion

    //region CREATE TABLE
    @Test
    public void createTable()
    {
        //Arrange
        String viewColumns;
        String histColumns;

        //Act
        try
        {
            statement.executeUpdate("ALTER SESSION SET sql_preprocessor_script = TEST.MYPREPROCESSOR");
            statement.executeQuery("CREATE TABLE MyNewTestTbl (id INT, name VARCHAR(32))");
            resultSet = statement.executeQuery("SELECT * FROM MYNEWTESTTBL");
            viewColumns = resultSet.toString();
            resultSet = statement.executeQuery("SELECT * FROM HIST_MYNEWTESTTBL");
            histColumns = resultSet.toString();

            teardownPreprocessor();
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertNotNull(resultSet);
        assertTrue(viewColumns.equalsIgnoreCase("id name "));
        assertTrue(histColumns.equalsIgnoreCase("id name valid_from valid_until "));
    }

    @Test
    public void createTableLike()
    {
        //Arrange
        String viewColumns;
        String histColumns;

        //Act
        try
        {
            statement.executeUpdate("ALTER SESSION SET sql_preprocessor_script = TEST.MYPREPROCESSOR");
            statement.executeQuery("CREATE TABLE MyNewTestTbl (id INT, name VARCHAR(32))");
            statement.executeQuery("CREATE TABLE MyNewTestTbl2 LIKE MYNEWTESTTBL");
            resultSet = statement.executeQuery("SELECT * FROM MyNewTestTbl2");
            viewColumns = resultSet.toString();
            resultSet = statement.executeQuery("SELECT * FROM HIST_MYNEWTESTTBL2");
            histColumns = resultSet.toString();
            teardownPreprocessor();
            statement.executeUpdate("DROP VIEW MYNEWTESTTBL2");
            statement.executeUpdate("DROP TABLE HIST_MYNEWTESTTBL2");
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertNotNull(resultSet);
        assertTrue(viewColumns.equalsIgnoreCase("id name "));
        assertTrue(histColumns.equalsIgnoreCase("id name valid_from valid_until "));
    }

    //endregion

    //region INSERT INTO
    @Test
    public void testSimpleInsert()
    {
        //Arrange
        int id;
        String name;

        //Act
        try
        {
            setupPreprocessor();
            statement.executeQuery("INSERT INTO MyNewTestTbl (id, name) VALUES (1, 'TestName')");

            resultSet = statement.executeQuery("SELECT * FROM MYNEWTESTTBL");
            resultSet.next();
            id = resultSet.getInt(1);
            name = resultSet.getString(2);

            teardownPreprocessor();
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertNotNull(resultSet);
        assertEquals(1, id);
        assertTrue(name.equals("TestName"));
    }

    @Test
    public void testSimpleMultipleInsert()
    {
        //Arrange
        int[] ids = new int[2];
        String[] names = new String[2];

        //Act
        try {
            setupPreprocessor();
            statement.executeQuery("INSERT INTO MyNewTestTbl (id, name) VALUES (1, 'TestName'), (2, 'SecondName')");
            resultSet = statement.executeQuery("SELECT * FROM MYNEWTESTTBL");
            for(int i = 0; i < ids.length; i++)
            {
                resultSet.next();
                ids[i] = resultSet.getInt(1);
                names[i] = resultSet.getString(2);
            }

            teardownPreprocessor();
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertEquals(1, ids[0]);
        assertTrue(names[0].equals("TestName"));
        assertEquals(2, ids[1]);
        assertTrue(names[1].equals("SecondName"));
    }

    @Test
    public void insertFromAnotherTableAllColumns()
    {
        //Arrange
        int[] ids = new int[2];
        String[] names = new String[2];

        //Act
        try
        {
            setupPreprocessor();
            statement.executeQuery("CREATE TABLE SecondTestTable LIKE MyNewTestTbl");
            statement.executeQuery("INSERT INTO SecondTestTable (id, name) VALUES (1, 'TestName'), (2, 'SecondName')");
            statement.executeQuery("INSERT INTO MyNewTestTbl SELECT * FROM SecondTestTable");
            resultSet = statement.executeQuery("SELECT * FROM MYNEWTESTTBL");
            for(int i = 0; i < ids.length; i++)
            {
                resultSet.next();
                ids[i] = resultSet.getInt(1);
                names[i] = resultSet.getString(2);
            }

            teardownPreprocessor();
            statement.executeUpdate("DROP VIEW SecondTestTable");
            statement.executeUpdate("DROP TABLE HIST_SECONDTESTTABLE");
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }
        //Assert
        assertEquals(1, ids[0]);
        assertTrue(names[0].equals("TestName"));
        assertEquals(2, ids[1]);
        assertTrue(names[1].equals("SecondName"));
    }

    @Test
    public void insertFromAnotherTableSpecifiedColumns()
    {
        //Arrange
        int[] ids = new int[2];
        String[] names = new String[2];

        //Act
        try
        {
            setupPreprocessor();
            statement.executeQuery("CREATE TABLE SecondTestTable LIKE MyNewTestTbl");
            statement.executeQuery("INSERT INTO SecondTestTable (id, name) VALUES (1, 'TestName'), (2, 'SecondName')");
            statement.executeQuery("INSERT INTO MyNewTestTbl SELECT (id, name) FROM SecondTestTable");
            resultSet = statement.executeQuery("SELECT * FROM MYNEWTESTTBL");
            for(int i = 0; i < ids.length; i++)
            {
                resultSet.next();
                ids[i] = resultSet.getInt(1);
                names[i] = resultSet.getString(2);
            }

            teardownPreprocessor();
            statement.executeUpdate("DROP VIEW SecondTestTable");
            statement.executeUpdate("DROP TABLE HIST_SECONDTESTTABLE");
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }
        //Assert
        assertEquals(1, ids[0]);
        assertTrue(names[0].equals("TestName"));
        assertEquals(2, ids[1]);
        assertTrue(names[1].equals("SecondName"));
    }

    //endregion

    //region UPDATE

    @Test
    public void updateWithWhereClause()
    {
        //Arrange
        String name;
        int count;

        //Act
        try {
            setupPreprocessor();
            statement.executeQuery("INSERT INTO MyNewTestTbl (id, name) VALUES (1, 'TestName')");
            statement.executeQuery("UPDATE MyNewTestTbl SET name = 'NewName' WHERE name = 'TestName'");
            resultSet = statement.executeQuery("SELECT * FROM MyNewTestTbl WHERE id = 1");
            resultSet.next();
            name = resultSet.getString(2);
            resultSet = statement.executeQuery("SELECT COUNT(*) FROM HIST_MYNEWTESTTBL WHERE id = 1 AND valid_until != '9999-12-31 23:59:59.999'");
            resultSet.next();
            count = resultSet.getInt(1);
            teardownPreprocessor();
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }
        //Assert
        assertTrue(name.equals("NewName"));
        assertEquals(1, count);
    }

    @Test
    public void updateAllRows()
    {
        //Arrange
        String name;

        //Act
        try {
            setupPreprocessor();
            statement.executeQuery("INSERT INTO MyNewTestTbl (id, name) VALUES (1, 'TestName')");
            statement.executeQuery("UPDATE MyNewTestTbl SET name = 'NewName'");
            resultSet = statement.executeQuery("SELECT * FROM MyNewTestTbl WHERE id = 1");
            resultSet.next();
            name = resultSet.getString(2);
            teardownPreprocessor();
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertTrue(name.equals("NewName"));
    }

    //endregion

    //region DELETE

    @Test
    public void DeleteAllRows()
    {
        //Arrange
        int countView;
        int countHistTable;

        //Act
        try {
            setupPreprocessor();
            statement.executeQuery("INSERT INTO MyNewTestTbl (id, name) VALUES (1, 'TestName')");
            statement.executeQuery("DELETE FROM MyNewTestTbl");
            resultSet = statement.executeQuery("SELECT COUNT(*) FROM MyNewTestTbl");
            resultSet.next();
            countView = resultSet.getInt(1);
            resultSet = statement.executeQuery("SELECT COUNT(*) FROM HIST_MYNEWTESTTBL");
            resultSet.next();
            countHistTable = resultSet.getInt(1);
            teardownPreprocessor();
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertEquals(0, countView);
        assertEquals(1, countHistTable);
    }

    @Test
    public void deleteWithWhereClause()
    {
        //Arrange
        int count;

        //Act
        try {
            setupPreprocessor();
            statement.executeQuery("INSERT INTO MyNewTestTbl (id, name) VALUES (1, 'TestName'), (2, 'Ute')");
            statement.executeQuery("DELETE FROM MyNewTestTbl WHERE id = 1");
            resultSet = statement.executeQuery("SELECT COUNT(*) FROM MyNewTestTbl");
            resultSet.next();
            count = resultSet.getInt(1);
            teardownPreprocessor();
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertEquals(1, count);
    }

    //endregion

    //region OneInstructionCheck

    private static Stream<Arguments> oneLineInstructions()
    {
        return Stream.of(
                Arguments.of("CREATE TABLE TEST_TABLE (id INT, name VARCHAR(32))"),
                Arguments.of("CREATE TABLE E (id INT, name VARCHAR(32))"),
                Arguments.of("CREATE TABLE CLONE_E LIKE E"),
                Arguments.of("CREATE TABLE T1 AS SELECT count(*) AS my_count FROM TEST_TABLE WITH NO DATA"),
                Arguments.of("INSERT INTO E VALUES (1, 'abc')"),
                Arguments.of("INSERT INTO TEST_TABLE VALUES (2, 'ghi'), (3, 'pqr')"),
                Arguments.of("INSERT INTO E (id, name) SELECT (id, name) FROM TEST_TABLE"),
                Arguments.of("UPDATE TEST_TABLE SET id=1 WHERE name='ghi'"),
                Arguments.of("SELECT * FROM TEST_TABLE WHERE id = 1 AS OF SYSTEM TIME '9999-12-31 23:59:59.999'"),
                Arguments.of("SELECT * FROM TEST_TABLE AS OF SYSTEM TIME '9999-12-31 23:59:59.999'"),
                Arguments.of("DELETE FROM E"),
                Arguments.of("DELETE FROM CLONE_E"),
                Arguments.of("ALTER TABLE E DROP COLUMN id"),
                Arguments.of("RENAME TABLE E TO E2"),
                Arguments.of("SELECT COUNT(*) FROM HIST_TESTTABLE"),
                Arguments.of("SELECT * FROM HIST_TESTTABLE"),
                Arguments.of("TRUNCATE testTable"),
                Arguments.of("SELECT e.NAME FROM E e JOIN TEST_TABLE USING (id) GROUP BY e.name ORDER BY e.name"),
                Arguments.of("SELECT 3+3"),
                Arguments.of("DROP VIEW TEST_TABLE"),
                Arguments.of("DROP TABLE HIST_TEST_TABLE"),
                Arguments.of("SELECT CURRENT_SESSION")

        );
    }

    @ParameterizedTest
    @MethodSource("oneLineInstructions")
    public void testOneInstruction(String sqlCommand)
    {
        //Arrange
        oneTimeSetUp();

        boolean success;
        //Act
        try
        {
            statement.executeUpdate("ALTER SESSION SET sql_preprocessor_script = TEST.MYPREPROCESSOR");
            statement.execute(sqlCommand);
            success = true;
        }
        catch(SQLException e)
        {
            success = false;
        }

        //Assert
        assertTrue(success);
    }

    //endregion

    //region help methods

    private void setupPreprocessor() throws SQLException
    {
        statement.executeUpdate("ALTER SESSION SET sql_preprocessor_script = TEST.MYPREPROCESSOR");
        statement.executeQuery("CREATE TABLE MyNewTestTbl (id INT, name VARCHAR(32))");
    }

    private void teardownPreprocessor() throws SQLException
    {
        statement.executeUpdate("ALTER SESSION SET sql_preprocessor_script = null");
        statement.executeUpdate("DROP TABLE HIST_MYNEWTESTTBL");
        statement.executeUpdate("DROP VIEW MYNEWTESTTBL");
    }

    //endregion
}
