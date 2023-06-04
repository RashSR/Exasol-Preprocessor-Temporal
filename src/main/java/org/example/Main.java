package org.example;
import java.sql.*;

public class Main {
    public static void main(String[] args)
    {
        try { Class.forName("com.exasol.jdbc.EXADriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection con=null;
        Statement stmt=null;
        try {
            con = DriverManager.getConnection(
                    "jdbc:exa:192.168.56.101/02F61E335534AF0C9CE92A35EA597DEA9C3AFB7C7603397ECAAC471E43CF4B65;schema=TEST",
                    "sys",
                    "exasol"
            );
            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM movie");
            System.out.println("Schema SYS contains:");
            while(rs.next())
            {
                int id = rs.getInt(1);
                String name = rs.getString(2);
                String director = rs.getString("DIRECTOR");
                System.out.println("Movie: " + name + ", director: " + director);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {stmt.close();} catch (Exception e) {e.printStackTrace();}
            try {con.close();} catch (Exception e) {e.printStackTrace();}
        }
    }
}