package ru.nsu.fit.g15205.jdbc;

import java.sql.*;

public class Main {
    static String url = "jdbc:postgresql://localhost:5432/postgres";
    static String user = "postgres";
    static String password = "8888";

    public static void main(String[] args) {
        long start, finish;

        String createTable = "CREATE TABLE test_data("
                +"id SERIAL PRIMARY KEY, "
                +"name CHARACTER VARYING(20), "
                +"name2 CHARACTER VARYING(20));";

        String insertStatement = "INSERT INTO test_data "
                +"(name, name2) "
                +"VALUES('%s', '%s')";

        String insertPrepared = "INSERT INTO test_data "
                +"(name, name2) "
                +"VALUES(?, ?)";

        String truncateTable = "TRUNCATE TABLE test_data";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            connection.setAutoCommit(false);

            //STATEMENT
            try (Statement statement = connection.createStatement()) {
                //statement.executeUpdate("DROP TABLE test_data");
                //statement.executeUpdate(createTable);

                start = System.currentTimeMillis();
                for (int i = 0; i < 1000; i++) {
                    statement.executeUpdate(String.format(insertStatement, "1name1_" + i, "1name2_" + i));
                }
                connection.commit();
                finish = System.currentTimeMillis();

                System.out.println("STATEMENT: " + (finish - start));
                System.out.println("milisec: " + String.valueOf(1000.0 / (finish - start)));
                statement.executeUpdate(truncateTable);
            }

            //PREPARED STATEMENT
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertPrepared);
            Statement statement = connection.createStatement()) {
                start = System.currentTimeMillis();
                for (int i = 0; i < 1000; i++) {
                    preparedStatement.setString(1, "2name1_" + i);
                    preparedStatement.setString(2, "2name2_" + i);
                    preparedStatement.executeUpdate();
                }
                connection.commit();
                finish = System.currentTimeMillis();

                System.out.println("PREPARED STATEMENT: " + (finish - start));
                System.out.println("milisec: " + String.valueOf(1000.0 / (finish - start)));
                statement.executeUpdate(truncateTable);
            }

            //BATCH
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertPrepared);
            Statement statement = connection.createStatement()) {
                start = System.currentTimeMillis();
                for (int i = 0; i < 1000; i++) {
                    preparedStatement.setString(1, "3name1_" + i);
                    preparedStatement.setString(2, "3name2_" + i);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
                finish = System.currentTimeMillis();

                System.out.println("BATCH: " + (finish - start));
                System.out.println("milisec: " + String.valueOf(1000.0 / (finish - start)));
                statement.executeUpdate(truncateTable);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }
    }
}
