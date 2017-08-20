package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.database.DatabaseCommands;
import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.database.query.PhraseTranslator;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.processor.Migrator;
import nl.myndocs.database.migrator.validator.TableValidator;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

/**
 * Created by albert on 14-8-2017.
 */
public abstract class BaseIntegration {

    @Before
    public void dropChangelog() throws ClassNotFoundException, SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();

        TableValidator tableValidator = new TableValidator(connection);

        if (tableValidator.tableExists("migration_changelog")) {
            statement.execute("DROP TABLE migration_changelog");
        }

        statement.close();
        getConnection().close();
    }

    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, InterruptedException {
        Connection connection = getConnection();
        getMigrator().migrate(
                connection1 -> buildMigration()
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
        Migration.Builder builder = new Migration.Builder("migration-1");

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
            sqlException.printStackTrace();
            System.out.println("Re-attempt acquiring connection");
            return acquireConnection(connectionUri, username, password);
        }
    }

    // @TODO: H2 and Postgres are not throwing SQLIntegrityConstraintViolationException
    @Test(expected = Exception.class)
    public void testForeignKeyConstraint() throws Exception {
        Migration.Builder builder = new Migration.Builder("migration-1");

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

        getMigrator().migrate(
                connection -> builder.build()
        );

        Statement statement = getConnection().createStatement();
        statement.execute("INSERT INTO some_foreign_other_table (some_table_id) VALUES (1)");
        statement.close();
    }

    @Test
    public void testRemoveForeignKeyConstraint() throws Exception {
        final Migration.Builder createBuilder = new Migration.Builder("migration-1");

        createBuilder.table("some_foreign_drop_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("name", Column.TYPE.VARCHAR);

        createBuilder.table("some_foreign_drop_other_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("some_table_id", Column.TYPE.INTEGER)
                .addForeignKey("some_foreign_drop_FK", "some_foreign_drop_table", "some_table_id", "id", key -> {
                    key.cascadeDelete(ForeignKey.CASCADE.RESTRICT);
                    key.cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
                });

        final Migration.Builder dropTableBuilder = new Migration.Builder("migration-2");

        dropTableBuilder.table("some_foreign_drop_other_table")
                .dropForeignKey("some_foreign_drop_FK");

        getMigrator().migrate(
                connection -> createBuilder.build(),
                connection -> dropTableBuilder.build()
        );

        Statement statement = getConnection().createStatement();
        statement.execute("INSERT INTO some_foreign_drop_other_table (some_table_id) VALUES (1)");
        statement.close();
    }

    @Test
    public void testAddUniqueConstraint() throws ClassNotFoundException, SQLException {
        final Migration.Builder createBuilder = new Migration.Builder("migration-1");

        createBuilder.table("some_add_unique_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("name", Column.TYPE.VARCHAR);

        getMigrator().migrate(
                connection -> createBuilder.build()
        );

        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO some_add_unique_table (name) VALUES ('test1')");
        statement.execute("INSERT INTO some_add_unique_table (name) VALUES ('test1')");
        statement.execute("TRUNCATE TABLE some_add_unique_table");

        final Migration.Builder uniqueBuilder = new Migration.Builder("migration-2");

        uniqueBuilder.table("some_add_unique_table")
                .addConstraint("unique_constraint_name", Constraint.TYPE.UNIQUE, "name");

        getMigrator().migrate(
                connection1 -> uniqueBuilder.build()
        );

        statement.execute("INSERT INTO some_add_unique_table (name) VALUES ('test1')");
        try {
            statement.execute("INSERT INTO some_add_unique_table (name) VALUES ('test1')");
            fail("This should no succeed");
        } catch (Exception exception) {
            // @TODO: H2 and Postgres are not throwing SQLIntegrityConstraintViolationException
        }

        statement.close();
        connection.close();
    }

    @Test
    public void testDropUniqueConstraint() throws Exception {
        final Migration.Builder createBuilder = new Migration.Builder("migration-1");

        createBuilder.table("some_add_and_drop_unique_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("name", Column.TYPE.VARCHAR);

        final Migration.Builder addConstraintBuilder = new Migration.Builder("migration-2");

        addConstraintBuilder.table("some_add_and_drop_unique_table")
                .addConstraint("unique_add_and_drop_constraint_name", Constraint.TYPE.UNIQUE, "name");

        final Migration.Builder dropConstraintBuilder = new Migration.Builder("migration-3");

        dropConstraintBuilder.table("some_add_and_drop_unique_table")
                .dropConstraint("unique_add_and_drop_constraint_name");

        getMigrator().migrate(
                connection -> createBuilder.build(),
                connection -> addConstraintBuilder.build(),
                connection -> dropConstraintBuilder.build()
        );

        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO some_add_and_drop_unique_table (name) VALUES ('test1')");
        statement.execute("INSERT INTO some_add_and_drop_unique_table (name) VALUES ('test1')");

        statement.close();
        connection.close();
    }

    @Test
    public void testAddingNewColumnsToExistingTable() throws ClassNotFoundException, SQLException {
        final Migration.Builder builder = new Migration.Builder("migration-1");

        builder.table("some_appending_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("name", Column.TYPE.VARCHAR);

        final Migration.Builder addNewColumnBuilder = new Migration.Builder("migration-2");

        addNewColumnBuilder.table("some_appending_table")
                .addColumn("some_table_id", Column.TYPE.INTEGER);

        getMigrator().migrate(
                connection -> builder.build(),
                connection -> addNewColumnBuilder.build()
        );

    }

    @Test
    public void testDroppedColumn() throws ClassNotFoundException, SQLException {
        final Migration.Builder builder = new Migration.Builder("migration-1");

        builder.table("some_dropped_column_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("name", Column.TYPE.VARCHAR)
                .addColumn("some_table_id", Column.TYPE.INTEGER);

        final Migration.Builder dropColumnBuilder = new Migration.Builder("migration-2");

        dropColumnBuilder.table("some_dropped_column_table")
                .dropColumn("some_table_id");

        getMigrator().migrate(
                connection -> builder.build(),
                connection -> dropColumnBuilder.build()
        );

        Connection connection = getConnection();
        Statement statement = connection.createStatement();

        statement.execute("SELECT * FROM some_dropped_column_table");
        ResultSet resultSet = statement.getResultSet();
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String columnLabel = metaData.getColumnLabel(i);

            if (columnLabel.equalsIgnoreCase("some_table_id")) {
                fail("Column should not exist");
            }
        }

        statement.close();
        connection.close();
    }

    @Test
    public void testRenamingWithDefaults() throws ClassNotFoundException, SQLException {
        Connection connection = getConnection();
        Migrator migrator = getMigrator();

        final Migration.Builder builder = new Migration.Builder("migration-1");

        builder.table("test_rename_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("fixed_name", Column.TYPE.VARCHAR)
                .addColumn("name", Column.TYPE.VARCHAR, column -> column.notNull(true).defaultValue("default-value"));

        final Migration.Builder renameBuilder = new Migration.Builder("migration-2");

        renameBuilder.table("test_rename_table")
                .changeColumn("name", column -> column.rename("renamed"));

        migrator.migrate(
                connection1 -> builder.build(),
                connection1 -> renameBuilder.build()
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
        Migrator migrator = getMigrator();
        final Migration.Builder builder = new Migration.Builder("migration-1");

        builder.table("test_change_default_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                .addColumn("fixed_name", Column.TYPE.VARCHAR)
                .addColumn("name", Column.TYPE.VARCHAR, column -> column.notNull(true).defaultValue("default-value"));

        final Migration.Builder changeDefaultBuilder = new Migration.Builder("migration-2");

        changeDefaultBuilder.table("test_change_default_table")
                .changeColumn("name", column -> column.defaultValue("changed-value"));

        migrator.migrate(
                connection1 -> builder.build(),
                connection1 -> changeDefaultBuilder.build()
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

    protected Migrator getMigrator() {
        try {
            return new Migrator(
                    new DatabaseCommands(
                            getConnection(),
                            phraseTranslator()
                    ),
                    database()
            );
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract PhraseTranslator phraseTranslator();

    protected abstract Database database();

    protected abstract Connection getConnection() throws ClassNotFoundException;
}
