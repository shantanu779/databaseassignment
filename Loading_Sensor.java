import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Loading_Sensor {
    public static void createDatabase(Connection connection)
    {
        executeStatement(connection, "CREATE TABLE SensorData (Sensor_ID INT, Sensor_Name VARCHAR(64), PRIMARY KEY (Sensor_ID))");
        executeStatement(connection, "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (null,'SensorData','SensorData.csv',null,null,null,0)");

    }
    private static void executeStatement( Connection connection, String query ) {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(query);
            statement.close();
        } catch(SQLException sqle) {
            System.err.println("Failed to execute: " + query);
            System.err.println( sqle.getMessage());
        }
    }

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:derby:testdb;create=true");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        long start = System.currentTimeMillis();
        createDatabase(conn);
        long end = System.currentTimeMillis();
        System.out.println("database loading " +
                (end - start) + "ms");



    }
}
