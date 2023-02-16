package nl.myndocs.database.migrator.integration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.UUID;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.myndocs.database.migrator.MigrationScript;
import nl.myndocs.database.migrator.database.Selector;
import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Index;
import nl.myndocs.database.migrator.definition.PartitionSet;
import nl.myndocs.database.migrator.integration.tools.SimpleMigrationScript;
import nl.myndocs.database.migrator.processor.Migrator;

/**
 * Created by albert on 14-8-2017.
 */
public abstract class BaseIntegration {
    private static final Logger logger = LoggerFactory.getLogger(BaseIntegration.class);

    @Before
    public void dropChangelog() throws ClassNotFoundException, SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();

        if (database().hasTable("migration_changelog")) {
            statement.execute("DROP TABLE migration_changelog");
        }

        statement.close();
        getConnection().close();
    }

    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, InterruptedException {
        Connection connection = getConnection();
        getMigrator().migrate(
                buildMigration()
        );


        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO some_table (name, change_type) VALUES ('test1', 'type-test')");
        statement.execute("INSERT INTO some_table (name) VALUES ('test2')");
        statement.execute("SELECT * FROM some_table");

        ResultSet resultSet = statement.getResultSet();

        int lastIndex = -1;

        while (resultSet.next()) {
            int index = resultSet.getInt(1);
            assertThat(index, is(Matchers.greaterThan(lastIndex)));
            lastIndex = index;
        }

        statement.close();
        connection.close();
    }

    public MigrationScript buildMigration() {

        return new SimpleMigrationScript(
                "migration-1",
                migration -> {
                    migration.table("some_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("small_id", Column.TYPE.SMALL_INTEGER)
                            .addColumn("big_id", Column.TYPE.BIG_INTEGER)
                            .addColumn("name", Column.TYPE.VARCHAR)
                            .addColumn("name_non_null", Column.TYPE.VARCHAR, column -> column.notNull(true).defaultValue("default"))
                            .addColumn("some_chars", Column.TYPE.CHAR, column -> column.size(25))
                            .addColumn("some_uuid", Column.TYPE.UUID)
                            .addColumn("change_type", Column.TYPE.UUID)
                            .addColumn("test_date", Column.TYPE.DATE)
                            .addColumn("test_time", Column.TYPE.TIME)
                            .addColumn("test_timestamp", Column.TYPE.TIMESTAMP)
                            .addColumn("test_boolean", Column.TYPE.BOOLEAN, column -> column.defaultValue("false"))
                            .save();

                    migration.table("some_other_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("some_table_id", Column.TYPE.INTEGER)
                            .addColumn("name", Column.TYPE.VARCHAR, column -> {
                                column.size(2);
                            })
                            .addConstraint("some_FK", Constraint.TYPE.FOREIGN_KEY, cb -> {
                                cb.columns("some_table_id")
                                  .foreignKey(fk -> {
                                    fk.foreignTable("some_table")
                                      .foreignKeys("id")
                                      .cascadeDelete(ForeignKey.CASCADE.RESTRICT)
                                      .cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
                                });
                            })
                            .save();

                    migration.table("some_table")
                            .changeColumn("change_type", column -> column.type(Column.TYPE.VARCHAR))
                            .save();

                });
    }

    protected Connection acquireConnection(String connectionUri, String username, String password) {
        try {
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
            logger.warn("Re-attempt acquiring connection");
            return acquireConnection(connectionUri, username, password);
        }
    }

    @Test
    public void testPartitions() throws Exception {
        SimpleMigrationScript sis = new SimpleMigrationScript("partitions-1",
                migration -> {
                    migration.table("partitioned_test")
                        .addColumn("name", Column.TYPE.VARCHAR, column -> column.size(255))
                        .addPartitions(PartitionSet.TYPE.HASH, set -> {
                            set.partitions(() -> {
                                return Collections.emptyList();
                            });
                        })
                        .save();
                });
    }

    @Test
    public void testBigIntegerIncrement() throws Exception {
        incrementTest("test-big-int-increment", "increment_big_integer", Column.TYPE.BIG_INTEGER);
    }

    @Test
    public void testIntegerIncrement() throws Exception {
        incrementTest("test-int-increment", "increment_integer", Column.TYPE.INTEGER);
    }

    @Test
    public void testSmallIntegerIncrement() throws Exception {
        incrementTest("test-small-int-increment", "increment_small_integer", Column.TYPE.SMALL_INTEGER);
    }

    private void incrementTest(String migrationId, String tableName, Column.TYPE columnType) throws Exception {
        SimpleMigrationScript simpleMigrationScript = new SimpleMigrationScript(
                migrationId,
                migration -> {
                    migration.table(tableName)
                            .addColumn("incremental", columnType, column -> column.primary(true).autoIncrement(true))
                            .addColumn("name", Column.TYPE.VARCHAR, column -> column.size(255))
                            .save();
                }
        );

        getMigrator().migrate(simpleMigrationScript);

        Statement statement = getConnection().createStatement();
        statement.execute("INSERT INTO " + tableName + " (name) VALUES ('value1')");
        statement.execute("INSERT INTO " + tableName + " (name) VALUES ('value2')");
        statement.execute("INSERT INTO  " + tableName + "(name) VALUES ('value3')");

        statement.execute("SELECT incremental FROM " + tableName + " ORDER BY incremental");

        ResultSet resultSet = statement.getResultSet();

        int lastIndex = -1;
        while (resultSet.next()) {
            int incremental = resultSet.getInt(1);
            assertThat(lastIndex, Matchers.lessThan(incremental));

            lastIndex = incremental;
        }
        statement.close();
    }

    @Test
    public void testMultiplePrimaryKeys() throws ClassNotFoundException, SQLException {
        SimpleMigrationScript simpleMigrationScript = new SimpleMigrationScript(
                "test-multiple-primary-keys",
                migration -> {
                    migration.table("multiple_primary_keys")
                            .addColumn("first_key", Column.TYPE.INTEGER, column -> column.notNull(true))
                            .addColumn("second_key", Column.TYPE.INTEGER, column -> column.notNull(true))
                            .addConstraint("multiple_primary_keys_pkey", Constraint.TYPE.PRIMARY_KEY, "first_key", "second_key")
                            .save();
                }
        );

        getMigrator().migrate(simpleMigrationScript);

        Statement statement = getConnection().createStatement();
        statement.execute("INSERT INTO multiple_primary_keys (first_key, second_key) VALUES (1, 1)");
        try {
            statement.execute("INSERT INTO multiple_primary_keys (first_key, second_key) VALUES (1, 1)");
            fail("This should not succeed");
        } catch (Exception exception) {
            assertTrue(isConstraintViolationException(exception));
        } finally {
            statement.close();
        }
    }

    @Test
    public void testIndex() throws ClassNotFoundException, SQLException {
        SimpleMigrationScript simpleMigrationScript = new SimpleMigrationScript(
                "test-simple_index",
                migration -> {
                    migration.table("simple_index")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.notNull(true).primary(true).autoIncrement(true))
                            .addColumn("name", Column.TYPE.VARCHAR, column -> column.size(255))
                            .addIndex("simple_index_name", Index.TYPE.DEFAULT, "name")
                            .save();
                }
        );

        getMigrator().migrate(simpleMigrationScript);

    }

    @Test
    public void testTextField() throws ClassNotFoundException, SQLException {
        SimpleMigrationScript simpleMigrationScript = new SimpleMigrationScript(
                "test-text-field",
                migration -> {
                    migration.table("text_field")
                            .addColumn("id", Column.TYPE.INTEGER)
                            .addColumn("name", Column.TYPE.TEXT)
                            .save();
                }
        );

        getMigrator().migrate(simpleMigrationScript);

        StringBuilder longString = new StringBuilder();

        for (int i = 0; i < 1000; i++) {
            longString.append(UUID.randomUUID().toString());
        }

        PreparedStatement insertStatement = getConnection().prepareStatement("INSERT INTO text_field (id, name) VALUES (1, ?)");
        insertStatement.setString(1, longString.toString());
        insertStatement.execute();
        insertStatement.close();

        Statement statement = getConnection().createStatement();
        statement.execute("SELECT name FROM text_field WHERE id = 1");

        ResultSet resultSet = statement.getResultSet();
        resultSet.next();

        String result = resultSet.getString(1);
        assertThat(result, is(equalTo(longString.toString())));
        statement.close();

    }

    @Test
    public void testForeignKeyConstraint() throws Exception {
        try {
            SimpleMigrationScript migrationScript = new SimpleMigrationScript(
                    "migration-1",
                    migration -> {

                        migration.table("some_foreign_table")
                                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                                .addColumn("name", Column.TYPE.VARCHAR)
                                .save();

                        migration.table("some_foreign_other_table")
                                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                                .addColumn("some_table_id", Column.TYPE.INTEGER)
                                .addConstraint("some_foreign_FK", Constraint.TYPE.FOREIGN_KEY, cb -> {
                                    cb.columns("some_table_id")
                                      .foreignKey(fk -> {
                                       fk.foreignTable("some_foreign_table")
                                         .foreignKeys("id")
                                         .cascadeDelete(ForeignKey.CASCADE.RESTRICT)
                                         .cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
                                    });
                                })
                                .save();
                    });

            getMigrator().migrate(migrationScript);

            Statement statement = getConnection().createStatement();
            statement.execute("INSERT INTO some_foreign_other_table (some_table_id) VALUES (1)");
            statement.close();

            fail("Exception should occur");
        } catch (Exception exception) {
        }
    }

    @Test
    public void testRemoveForeignKeyConstraint() throws Exception {
        SimpleMigrationScript migrationScript = new SimpleMigrationScript(
                "migration-1",
                migration -> {
                    migration.table("some_foreign_drop_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("name", Column.TYPE.VARCHAR)
                            .save();

                    migration.table("some_foreign_drop_other_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("some_table_id", Column.TYPE.INTEGER)
                            .addConstraint("some_foreign_drop_FK", Constraint.TYPE.FOREIGN_KEY, cb -> {
                                cb.columns("some_table_id")
                                  .foreignKey(fk -> {
                                      fk.foreignTable("some_foreign_drop_table")
                                        .foreignKeys("id")
                                        .cascadeDelete(ForeignKey.CASCADE.RESTRICT)
                                        .cascadeUpdate(ForeignKey.CASCADE.RESTRICT);
                                  });
                            })
                            .save();
                });

        SimpleMigrationScript migrationScript2 = new SimpleMigrationScript(
                "migration-2",
                migration -> {
                    migration.table("some_foreign_drop_other_table")
                            .dropConstraint("some_foreign_drop_FK")
                            .save();
                }
        );

        getMigrator().migrate(
                migrationScript,
                migrationScript2
        );

        Statement statement = getConnection().createStatement();
        statement.execute("INSERT INTO some_foreign_drop_other_table (some_table_id) VALUES (1)");
        statement.close();
    }

    @Test
    public void testAddUniqueConstraint() throws ClassNotFoundException, SQLException {
        SimpleMigrationScript migrationScript = new SimpleMigrationScript(
                "migration-1",
                migration -> {
                    migration.table("some_add_unique_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("name", Column.TYPE.VARCHAR)
                            .save();

                }
        );
        getMigrator().migrate(migrationScript);

        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO some_add_unique_table (name) VALUES ('test1')");
        statement.execute("INSERT INTO some_add_unique_table (name) VALUES ('test1')");
        statement.execute("TRUNCATE TABLE some_add_unique_table");

        migrationScript = new SimpleMigrationScript(
                "migration-2",
                migration -> {
                    migration.table("some_add_unique_table")
                            .addIndex("unique_constraint_name", Index.TYPE.UNIQUE, "name")
                            .save();
                }
        );

        getMigrator().migrate(migrationScript);

        statement.execute("INSERT INTO some_add_unique_table (name) VALUES ('test1')");
        try {
            statement.execute("INSERT INTO some_add_unique_table (name) VALUES ('test1')");
            fail("This should no succeed");
        } catch (Exception exception) {
            assertTrue(isConstraintViolationException(exception));
        }

        statement.close();
        connection.close();
    }

    @Test
    public void testDropUniqueConstraint() throws Exception {
        SimpleMigrationScript createBuilder = new SimpleMigrationScript(
                "migration-1",
                migration -> {
                    migration.table("some_add_and_drop_unique_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("name", Column.TYPE.VARCHAR)
                            .save();

                }
        );

        SimpleMigrationScript addIndexBuilder = new SimpleMigrationScript(
                "migration-2",
                migration -> {

                    migration.table("some_add_and_drop_unique_table")
                            .addIndex("unique_add_and_drop_constraint_name", Index.TYPE.UNIQUE, "name")
                            .save();

                }
        );

        SimpleMigrationScript dropConstraintBuilder = new SimpleMigrationScript(
                "migration-3",
                migration -> {
                    migration.table("some_add_and_drop_unique_table")
                            .dropConstraint("unique_add_and_drop_constraint_name")
                            .save();
                }
        );

        getMigrator().migrate(
                createBuilder,
                addIndexBuilder,
                dropConstraintBuilder
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
        SimpleMigrationScript builder = new SimpleMigrationScript(
                "migration-1",
                migration -> {
                    migration.table("some_appending_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("name", Column.TYPE.VARCHAR)
                            .save();
                }
        );

        SimpleMigrationScript addNewColumnBuilder = new SimpleMigrationScript(
                "migration-2",
                migration -> {
                    migration.table("some_appending_table")
                            .addColumn("some_table_id", Column.TYPE.INTEGER)
                            .save();
                }
        );

        getMigrator().migrate(
                builder,
                addNewColumnBuilder
        );

    }

    @Test
    public void testDroppedColumn() throws ClassNotFoundException, SQLException {
        SimpleMigrationScript builder = new SimpleMigrationScript(
                "migration-1",
                migration -> {
                    migration.table("some_dropped_column_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("name", Column.TYPE.VARCHAR)
                            .addColumn("some_table_id", Column.TYPE.INTEGER)
                            .save();
                }
        );

        SimpleMigrationScript dropColumnBuilder = new SimpleMigrationScript(
                "migration-2",
                migration -> {
                    migration.table("some_dropped_column_table")
                            .dropColumn("some_table_id")
                            .save();

                }
        );
        getMigrator().migrate(
                builder,
                dropColumnBuilder
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

        SimpleMigrationScript builder = new SimpleMigrationScript(
                "migration-1",
                migration -> {
                    migration.table("test_rename_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("fixed_name", Column.TYPE.VARCHAR)
                            .addColumn("name", Column.TYPE.VARCHAR, column -> column.notNull(true).defaultValue("default-value"))
                            .save();
                }
        );

        SimpleMigrationScript renameBuilder = new SimpleMigrationScript(
                "migration-2",
                migration -> {
                    migration.table("test_rename_table")
                            .changeColumn("name", column -> column.rename("renamed"))
                            .save();
                }
        );
        migrator.migrate(
                builder,
                renameBuilder
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

        SimpleMigrationScript builder = new SimpleMigrationScript(
                "migration-1",
                migration -> {
                    migration.table("test_change_default_table")
                            .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true).autoIncrement(true))
                            .addColumn("fixed_name", Column.TYPE.VARCHAR)
                            .addColumn("name", Column.TYPE.VARCHAR, column -> column.notNull(true).defaultValue("default-value"))
                            .save();
                }
        );

        SimpleMigrationScript changeDefaultBuilder = new SimpleMigrationScript(
                "migration-2",
                migration -> {
                    migration.table("test_change_default_table")
                            .changeColumn("name", column -> column.defaultValue("changed-value\\'"))
                            .save();
                }
        );

        migrator.migrate(
                builder,
                changeDefaultBuilder
        );

        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO test_change_default_table (fixed_name) VALUES ('FIXED')");
        statement.execute("SELECT name FROM test_change_default_table");

        ResultSet resultSet = statement.getResultSet();
        assertTrue(resultSet.next());
        String defaultValue = resultSet.getString(1);
        assertEquals("changed-value\\'", defaultValue);

        statement.close();

        connection.close();
    }

    @Test
    public void testDatabaseIsOfExpectedClass() {
        assertThat(database(), is(instanceOf(expectedDatabaseClass())));
    }

    protected Migrator getMigrator() {
        return new Migrator(
                database()
        );
    }

    protected Database database() {
        try {
            return new Selector().loadFromConnection(getConnection());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract Connection getConnection() throws ClassNotFoundException;

    protected abstract Class<? extends Database> expectedDatabaseClass();

    protected abstract boolean isConstraintViolationException(Exception exception);
}
