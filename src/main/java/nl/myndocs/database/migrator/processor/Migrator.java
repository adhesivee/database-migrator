package nl.myndocs.database.migrator.processor;

import nl.myndocs.database.migrator.MigrationScript;
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
import java.util.function.Consumer;

/**
 * Created by albert on 15-8-2017.
 */
public class Migrator {
    private static final String CHANGE_LOG_TABLE = "migration_changelog";

    private final Database database;

    public Migrator(Database database) {
        this.database = database;
    }

    public void migrate(MigrationScript... migrationScripts) throws SQLException {
        if (!database.hasTable(CHANGE_LOG_TABLE)) {
            new Table.Builder(CHANGE_LOG_TABLE, newTableConsumer())
                    .addColumn("id", Column.TYPE.INTEGER, column -> column.autoIncrement(true).primary(true))
                    .addColumn("migration_id", Column.TYPE.VARCHAR)
                    .addConstraint("migration_migration_id", Constraint.TYPE.UNIQUE, "migration_id")
                    .save();
        }

        for (MigrationScript migrationScript : migrationScripts) {
            Connection connection = database.getConnection();

            Migration migration = new Migration(
                    migrationScript.migrationId(),
                    database,
                    newTableConsumer()
            );

            migrationScript.migrate(migration);

            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + CHANGE_LOG_TABLE + " WHERE migration_id = ?");
            preparedStatement.setString(1, migration.getMigrationId());
            preparedStatement.execute();

            ResultSet resultSet = preparedStatement.getResultSet();
            boolean migrationExist = resultSet.next();
            preparedStatement.close();

            if (migrationExist) {
                return;
            }

            PreparedStatement insertPreparedStatement = connection.prepareStatement("INSERT INTO " + CHANGE_LOG_TABLE + " (migration_id) VALUES (?)");
            insertPreparedStatement.setString(1, migration.getMigrationId());
            insertPreparedStatement.execute();
        }
    }

    private Consumer<Table> newTableConsumer() {
        return (table -> {
            if (database.hasTable(table.getTableName())) {
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
                                    foreignKey.getDeleteCascade().orElse(null),
                                    foreignKey.getUpdateCascade().orElse(null)
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
                                            column.getAutoIncrement().orElse(null),
                                            column.getSize().orElse(null)
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
        });
    }

    private Collection<ColumnOptions> getColumnOptionsFromNewColumns(Table table) {
        Collection<ColumnOptions> columnOptions = new ArrayList<>();

        for (Column column : table.getNewColumns()) {
            columnOptions.add(
                    new ColumnOptions.Builder
                            (
                                    column.getColumnName(),
                                    column.getType().get()
                            ).setAutoIncrement(column.getAutoIncrement().orElse(null))
                            .setColumnSize(column.getSize().orElse(null))
                            .setDefaultValue(column.getDefaultValue().orElse(null))
                            .setNotNull(column.getIsNotNull().orElse(null))
                            .setPrimary(column.getPrimary().orElse(null))
                            .build()
            );
        }

        return columnOptions;
    }
}
