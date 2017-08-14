package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Migration;

import java.sql.*;

/**
 * Created by albert on 14-8-2017.
 */
public abstract class BaseIntegration {
    public void performIntegration(
            Connection connection
    ) throws SQLException, ClassNotFoundException, InterruptedException {
        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO some_table (name) VALUES ('test1')");
        statement.execute("INSERT INTO some_table (name) VALUES ('test2')");
        statement.execute("SELECT * FROM some_table");

        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
            System.out.print("\t" + resultSet.getInt(1));
            System.out.println("\t" + resultSet.getString(2));
        }

        statement.close();
        connection.close();
    }

    public Migration buildMigration() {
        Migration.Builder builder = new Migration.Builder();

        builder.addTable("some_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("name", Column.TYPE.VARCHAR)
                .addColumn("some_chars", Column.TYPE.CHAR, column -> column.size(25))
                .addColumn("some_uuid", Column.TYPE.UUID);

        builder.addTable("some_other_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("some_table_id", Column.TYPE.INTEGER)
                .addColumn("name", Column.TYPE.VARCHAR, column -> {
                    column.size(2);
                })
                .foreignKey("some_table", "some_table_id", "id", key -> {
                    key.cascadeDelete(ForeignKey.CASCADE.RESTRICT);
                    key.cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
                });

        return builder.build();
    }

    protected Connection acquireConnection(String connectionUri, String username, String password) {
        try {
            System.out.println("Getting connection");
            return DriverManager.getConnection(
                    connectionUri,
                    username,
                    password
            );
        } catch (SQLException sqlException) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Re-attempt acquiring connection");
            return acquireConnection(connectionUri, username, password);
        }
    }
}
