package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.profile.Profile;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by albert on 14-8-2017.
 */
public abstract class BaseIntegration {
    public void performIntegration(
            Connection connection
    ) throws SQLException, ClassNotFoundException, InterruptedException {
        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO some_table (name, change_type) VALUES ('test1', 'type-test')");
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

        builder.table("some_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("name", Column.TYPE.VARCHAR)
                .addColumn("name_non_null", Column.TYPE.VARCHAR, column -> column.notNull(true).defaultValue("default"))
                .addColumn("some_chars", Column.TYPE.CHAR, column -> column.size(25))
                .addColumn("some_uuid", Column.TYPE.UUID)
                .addColumn("change_type", Column.TYPE.UUID);

        builder.table("some_other_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("some_table_id", Column.TYPE.INTEGER)
                .addColumn("name", Column.TYPE.VARCHAR, column -> {
                    column.size(2);
                })
                .foreignKey("some_table", "some_table_id", "id", key -> {
                    key.cascadeDelete(ForeignKey.CASCADE.RESTRICT);
                    key.cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
                });

        builder.table("some_table")
                .changeColumn("change_type", column -> column.type(Column.TYPE.VARCHAR));

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

    public void testRenamingWithDefaults(Connection connection, Profile profile) throws ClassNotFoundException, SQLException {
        Migration.Builder builder = new Migration.Builder();

        builder.table("test_rename_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("fixed_name", Column.TYPE.VARCHAR)
                .addColumn("name", Column.TYPE.VARCHAR, column -> column.notNull(true).defaultValue("default-value"));


        profile.createDatabase(
                connection,
                builder.build()
        );

        builder = new Migration.Builder();

        builder.table("test_rename_table")
                .changeColumn("name", column -> column.rename("renamed"));

        profile.createDatabase(
                connection,
                builder.build()
        );

        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO test_rename_table (fixed_name) VALUES ('FIXED')");
        statement.execute("SELECT renamed FROM test_rename_table");

        ResultSet resultSet = statement.getResultSet();
        assertTrue(resultSet.next());
        String defaultValue = resultSet.getString(1);
        assertEquals("default-value", defaultValue);

        statement.close();

        connection.close();
    }

}
