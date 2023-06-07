import org.junit.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class SqlTests {


    private static Connection connection;
    private static Statement statement;

    private static ResultSet resultSet;
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
    public void testSetEndSemicolonScript()
    {
        //Arrange
        String sqlCommand = "SELECT * FROM movies";
        String output;

        //Act
        try
        {
            statement.executeUpdate("CREATE OR REPLACE SCRIPT testSetEndSemicolon(sqlCommand) AS import('TEST.HISTORYLIB', 'history_lib') output(history_lib.setEndSemicolon(sqlCommand))");
            resultSet = statement.executeQuery("EXECUTE SCRIPT testSetEndSemicolon('" + sqlCommand + "') with output;");
            resultSet.next();
            output = resultSet.getString("OUTPUT");
            statement.executeUpdate("DROP SCRIPT testSetEndSemicolon");
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        //Assert
        assertEquals(sqlCommand + ";", output);
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
                Arguments.of("'CREATE TABLE testTable (id INT, name VARCHAR(32))', 'TABLE', 2", true)
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
}
