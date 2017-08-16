package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.profile.Profile;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by albert on 14-8-2017.
 */
public abstract class BaseIntegration {
    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, InterruptedException {
        Connection connection = getConnection();
        getProfile().createDatabase(
                connection,
                buildMigration()
        );

        performIntegration();
    }

    public void performIntegration(
    ) throws SQLException, ClassNotFoundException, InterruptedException {
        Connection connection = getConnection();

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
                .addForeignKey("some_FK", "some_table", "some_table_id", "id", key -> {
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

    // @TODO: H2 and Postgres are not throwing SQLIntegrityConstraintViolationException
    @Test(expected = Exception.class)
    public void testForeignKeyConstraint() throws Exception {
        Migration.Builder builder = new Migration.Builder();

        builder.table("some_foreign_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("name", Column.TYPE.VARCHAR);

        builder.table("some_foreign_other_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("some_table_id", Column.TYPE.INTEGER)
                .addForeignKey("some_foreign_FK", "some_foreign_table", "some_table_id", "id", key -> {
                    key.cascadeDelete(ForeignKey.CASCADE.RESTRICT);
                    key.cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
                });

        getProfile().createDatabase(
                getConnection(),
                builder.build()
        );

        Statement statement = getConnection().createStatement();
        statement.execute("INSERT INTO some_foreign_other_table (some_table_id) VALUES (1)");
        statement.close();
    }

    @Test
    public void testAddingNewColumnsToExistingTable() throws ClassNotFoundException, SQLException {
        Migration.Builder builder = new Migration.Builder();

        builder.table("some_appending_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("name", Column.TYPE.VARCHAR);

        getProfile().createDatabase(
                getConnection(),
                builder.build()
        );

        builder = new Migration.Builder();

        builder.table("some_appending_table")
                .addColumn("some_table_id", Column.TYPE.INTEGER);

        getProfile().createDatabase(
                getConnection(),
                builder.build()
        );

    }
    @Test
    public void testRenamingWithDefaults() throws ClassNotFoundException, SQLException {
        Connection connection = getConnection();
        Profile profile = getProfile();

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

    @Test
    public void testChangingDefaults() throws ClassNotFoundException, SQLException {
        Connection connection = getConnection();
        Profile profile = getProfile();
        Migration.Builder builder = new Migration.Builder();

        builder.table("test_change_default_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("fixed_name", Column.TYPE.VARCHAR)
                .addColumn("name", Column.TYPE.VARCHAR, column -> column.notNull(true).defaultValue("default-value"));


        profile.createDatabase(
                connection,
                builder.build()
        );

        builder = new Migration.Builder();

        builder.table("test_change_default_table")
                .changeColumn("name", column -> column.defaultValue("changed-value"));

        profile.createDatabase(
                connection,
                builder.build()
        );

        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO test_change_default_table (fixed_name) VALUES ('FIXED')");
        statement.execute("SELECT name FROM test_change_default_table");

        ResultSet resultSet = statement.getResultSet();
        assertTrue(resultSet.next());
        String defaultValue = resultSet.getString(1);
        assertEquals("changed-value", defaultValue);

        statement.close();

        connection.close();
    }

    protected abstract Profile getProfile();

    protected abstract Connection getConnection() throws ClassNotFoundException;
}
