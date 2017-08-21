package nl.myndocs.database.migrator.processor;

import nl.myndocs.database.migrator.MigrationScript;
import nl.myndocs.database.migrator.database.DatabaseCommands;
import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.database.query.option.ColumnOptions;
import nl.myndocs.database.migrator.database.query.option.ForeignKeyOptions;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by albert on 15-8-2017.
 */
public class Migrator {
    private static final String CHANGE_LOG_TABLE = "migration_changelog";

    private final DatabaseCommands databaseCommands;
    private final Database database;

    public Migrator(DatabaseCommands databaseCommands, Database database) {
        this.databaseCommands = databaseCommands;
        this.database = database;
    }

    public void migrate(MigrationScript... migrationScripts) throws SQLException {
        if (!databaseCommands.getTableValidator()
                .tableExists(CHANGE_LOG_TABLE)) {
            Table table = new Table.Builder(CHANGE_LOG_TABLE)
                    .addColumn("id", Column.TYPE.INTEGER, column -> column.autoIncrement(true).primary(true))
                    .addColumn("migration_id", Column.TYPE.VARCHAR)
                    .addConstraint("migration_migration_id", Constraint.TYPE.UNIQUE, "migration_id")
                    .build();

            databaseCommands.addColumnsWithCreateTable(table);
        }

        for (MigrationScript migrationScript : migrationScripts) {
            Connection connection = databaseCommands.getConnection();

            Migration migration = migrationScript.migrate(connection);

            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + CHANGE_LOG_TABLE + " WHERE migration_id = ?");
            preparedStatement.setString(1, migration.getMigrationId());
            preparedStatement.execute();

            ResultSet resultSet = preparedStatement.getResultSet();
            boolean migrationExist = resultSet.next();
            preparedStatement.close();

            if (migrationExist) {
                return;
            }

            for (Table table : migration.getTables()) {
                if (databaseCommands.getTableValidator().tableExists(table.getTableName())) {
                    getColumnOptionsFromNewColumns(table).forEach(columnOptions -> database.alterTable(table.getTableName()).addColumn(columnOptions));
                } else {
                    database.createTable(table.getTableName(), getColumnOptionsFromNewColumns(table));
                }

                table.getDropColumns().forEach(column -> database.alterTable(table.getTableName()).dropColumn(column));
                table.getDropForeignKeys().forEach(constraintName -> database.alterTable(table.getTableName()).dropForeignKey(constraintName));
                table.getNewConstraints().forEach(constraint ->
                        database.alterTable(table.getTableName()).addConstraint(
                                constraint.getConstraintName(),
                                constraint.getColumnNames(),
                                constraint.getType().get()
                        )
                );
                table.getDropConstraints().forEach(constraintName -> database.alterTable(table.getTableName()).dropConstraint(constraintName));
                table.getNewForeignKeys().forEach(foreignKey ->
                        database.alterTable(table.getTableName()).addForeignKey(
                                foreignKey.getConstraintName(),
                                foreignKey.getForeignTable(),
                                foreignKey.getLocalKeys(),
                                foreignKey.getForeignKeys(),
                                new ForeignKeyOptions(
                                        foreignKey.getDeleteCascade(),
                                        foreignKey.getUpdateCascade()
                                )
                        )
                );


                for (Column column : table.getChangeColumns()) {
                    if (column.getType().isPresent()) {
                        database.alterTable(table.getTableName())
                                .alterColumn(column.getColumnName())
                                .changeType(
                                        column.getType().get(),
                                        new ChangeTypeOptions(
                                                column.getAutoIncrement(),
                                                column.getSize()
                                        )
                                );
                    }

                    if (column.getDefaultValue().isPresent()) {
                        database.alterTable(table.getTableName())
                                .alterColumn(column.getColumnName())
                                .setDefault(column.getDefaultValue().get());
                    }
                }

                // Make sure renames always happens last
                // Otherwise alterColumnType and alterColumnDefault will break
                for (Column column : table.getChangeColumns()) {
                    if (column.getRename().isPresent()) {
                        database.alterTable(table.getTableName())
                                .alterColumn(column.getColumnName())
                                .rename(column.getRename().get());
                    }
                }
            }

            PreparedStatement insertPreparedStatement = connection.prepareStatement("INSERT INTO " + CHANGE_LOG_TABLE + " (migration_id) VALUES (?)");
            insertPreparedStatement.setString(1, migration.getMigrationId());
            insertPreparedStatement.execute();
        }
    }

    private Collection<ColumnOptions> getColumnOptionsFromNewColumns(Table table) {
        Collection<ColumnOptions> columnOptions = new ArrayList<>();

        for (Column column : table.getNewColumns()) {
            columnOptions.add(
                    new ColumnOptions(
                            column.getColumnName(),
                            column.getType().get(),
                            column.getAutoIncrement(),
                            column.getSize(),
                            column.getDefaultValue(),
                            column.getIsNotNull(),
                            column.getPrimary()
                    )
            );
        }

        return columnOptions;
    }
}
