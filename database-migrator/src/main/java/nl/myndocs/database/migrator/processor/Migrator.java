package nl.myndocs.database.migrator.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.function.Consumer;

import nl.myndocs.database.migrator.MigrationScript;
import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Index;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.definition.Raw;
import nl.myndocs.database.migrator.definition.Table;

/**
 * Created by albert on 15-8-2017.
 */
public class Migrator {
    private static final String DEFAULT_CHANGE_LOG_TABLE = "migration_changelog";

    private static final String FIELD_ID = "id";

    private static final String FIELD_MIGRATION_ID = "migration_id";

    private static final String FIELD_AUTHOR = "author";

    private static final String FIELD_APPLY_DATE = "apply_date";

    private final Database database;

    private final String changeLogTable;

    public Migrator(Database database) {
        this.database = database;
        this.changeLogTable = DEFAULT_CHANGE_LOG_TABLE;
    }

    public Migrator(Database database, String changeLogTable) {
        this.database = database;
        this.changeLogTable = changeLogTable;
    }

    public void migrate(MigrationScript... migrationScripts) throws SQLException {
        migrate(null, migrationScripts);
    }

    public void migrate(MigrationContext ctx, MigrationScript... migrationScripts) throws SQLException {

        if (!database.hasTable(changeLogTable)) {
            new Table.Builder(changeLogTable, newTableConsumer())
                    .addColumn(FIELD_ID, Column.TYPE.INTEGER, column -> column.autoIncrement(true).primary(true))
                    .addColumn(FIELD_MIGRATION_ID, Column.TYPE.VARCHAR)
                    .addColumn(FIELD_AUTHOR, Column.TYPE.VARCHAR)
                    .addColumn(FIELD_APPLY_DATE, Column.TYPE.TIMESTAMP)
                    .addIndex("ix_" + changeLogTable + "_" + FIELD_MIGRATION_ID, Index.TYPE.UNIQUE, FIELD_MIGRATION_ID)
                    .save();
        }

        Connection connection = database.getConnection();
        boolean isAutocommit = connection.getAutoCommit();
        try {

            connection.setAutoCommit(false);
            for (MigrationScript migrationScript : migrationScripts) {

                boolean migrationExist = false;
                try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + changeLogTable + " WHERE migration_id = ?")) {

                    preparedStatement.setString(1, migrationScript.migrationId());
                    preparedStatement.execute();

                    try (ResultSet resultSet = preparedStatement.getResultSet()) {
                        migrationExist = resultSet.next();
                    }
                }

                if (migrationExist) {
                    continue;
                }

                try (PreparedStatement insertPreparedStatement = connection.prepareStatement(
                        new StringBuilder("INSERT INTO ")
                            .append(changeLogTable)
                            .append(" (")
                            .append(FIELD_MIGRATION_ID)
                            .append(", ")
                            .append(FIELD_AUTHOR)
                            .append(", ")
                            .append(FIELD_APPLY_DATE)
                            .append(") VALUES (?, ?, ?)")
                            .toString())) {

                    Migration m = new Migration(migrationScript.migrationId(), database, newTableConsumer(), newRawConsumer(), ctx);
                    migrationScript.migrate(m);

                    insertPreparedStatement.setString(1, migrationScript.migrationId());
                    insertPreparedStatement.setString(2, migrationScript.author());
                    insertPreparedStatement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                    insertPreparedStatement.execute();

                    connection.commit();

                    if (ctx != null) {
                        ctx.getApplied().add(m);
                    }

                } catch (Exception e) {
                    connection.rollback();
                    throw new CouldNotProcessException("Migration [" + migrationScript.migrationId() + "] failed.", e);
                }
            }
        } finally {
            connection.setAutoCommit(isAutocommit);
        }
    }

    private Consumer<Table> newTableConsumer() {
        return (table -> {
            applyRaw(table);
            applyStart(table);
            applyDrop(table);
            applyAlter(table);
            applyCreate(table);
            applyFinish(table);
        });
    }

    private Consumer<Raw> newRawConsumer() {
        return raw -> raw.getRawSQL().forEach(this::applyRawSQL);
    }

    private void applyStart(Table table) {

        if (database.hasTable(table.getTableName())) {
            database.updateTable(table);
            table.getNewColumns().forEach(column -> database.alterTable(table).addColumn(column));
        } else {
            database.createTable(table, table.getNewColumns());
        }
    }

    private void applyDrop(Table table) {

        table.getDropConstraints().forEach(constraintName -> database.alterTable(table).dropConstraint(constraintName));
        table.getDropIndexes().forEach(indexName -> database.alterTable(table).dropIndex(indexName));

        if (table.isPartitioned()) {
            table.getPartitionStream().forEach(p -> {
                p.getDropConstraints().forEach(constraintName -> database.alterPartition(p).dropConstraint(constraintName));
                p.getDropIndexes().forEach(indexName -> database.alterPartition(p).dropIndex(indexName));
            });
        }

        table.getDropColumns().forEach(columnName -> database.alterTable(table).dropColumn(columnName));
    }

    private void applyAlter(Table table) {

        for (Column column : table.getChangeColumns()) {
            if (column.getType() != null) {
                database.alterTable(table)
                        .alterColumn(column)
                        .changeType();
            }

            if (column.getDefaultValue() != null) {
                database.alterTable(table)
                        .alterColumn(column)
                        .setDefault();
            }

            if (column.getIsNotNull() != null) {
                database.alterTable(table)
                    .alterColumn(column)
                    .setNotNull();
            }

            if (column.getIsNull() != null) {
                database.alterTable(table)
                    .alterColumn(column)
                    .setNull();
            }
        }

        // Make sure renames always happens last
        // Otherwise alterColumnType and alterColumnDefault will break
        for (Column column : table.getChangeColumns()) {
            if (column.getRename() != null) {
                database.alterTable(table)
                        .alterColumn(column)
                        .rename();
            }
        }
    }

    private void applyCreate(Table table) {

        table.getNewConstraints().forEach(constraint -> database.alterTable(table).addConstraint(constraint));
        table.getNewIndexes().forEach(index -> database.alterTable(table).addIndex(index));

        if (table.isPartitioned()) {
            table.getPartitionStream().forEach(p -> {
                p.getNewConstraints().forEach(constraint -> database.alterPartition(p).addConstraint(constraint));
                p.getNewIndexes().forEach(index -> database.alterPartition(p).addIndex(index));
            });
        }
    }

    private void applyRaw(Table table) {
        table.getRawSQL().forEach(this::applyRawSQL);
    }

    private void applyRawSQL(String sql) {
        Connection connection = database.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new CouldNotProcessException("Execution of raw SQL [" + sql + "] failed.", e);
        }
    }

    private void applyFinish(Table table) {
        database.finishTable(table);
    }

    /**
     * @return the database
     */
    public Database getDatabase() {
        return database;
    }

    /**
     * @return the changeLogTable
     */
    public String getChangeLogTable() {
        return changeLogTable;
    }
}
