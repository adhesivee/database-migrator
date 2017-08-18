package nl.myndocs.database.migrator.processor;

import nl.myndocs.database.migrator.database.DatabaseCommands;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.SQLException;

/**
 * Created by albert on 15-8-2017.
 */
public class MigratorImpl implements Migrator {
    private static final String CREATE_FOREIGN_KEY_FORMAT = "FOREIGN KEY (%s) REFERENCES %s (%s)";
    private final DatabaseCommands databaseCommands;

    public MigratorImpl(DatabaseCommands databaseCommands) {
        this.databaseCommands = databaseCommands;
    }

    public void migrate(Migration migration) throws SQLException {
        for (Table table : migration.getTables()) {

            if (databaseCommands.getTableValidator().tableExists(table.getTableName())) {
                databaseCommands.addColumnsWithAlterTable(table);
            } else {
                databaseCommands.addColumnsWithCreateTable(table);
            }

            table.getDropColumns().forEach(column -> databaseCommands.dropColumn(table, column));
            table.getDropForeignKeys().forEach(constraintName -> databaseCommands.dropForeignKey(table, constraintName));
            table.getNewConstraints().forEach(constraint -> databaseCommands.addConstraint(table, constraint));
            table.getDropConstraints().forEach(constraintName -> databaseCommands.dropConstraint(table, constraintName));
            table.getNewForeignKeys().forEach(foreignKey -> databaseCommands.addForeignKey(table, foreignKey));


            for (Column column : table.getChangeColumns()) {
                if (column.getType().isPresent()) {
                    databaseCommands.alterColumnType(table, column);
                }

                if (column.getDefaultValue().isPresent()) {
                    databaseCommands.alterColumnDefault(table, column);
                }
            }

            // Make sure renames always happens last
            // Otherwise alterColumnType and alterColumnDefault will break
            for (Column column : table.getChangeColumns()) {
                if (column.getRename().isPresent()) {
                    databaseCommands.alterColumnName(table, column);
                }
            }
        }
    }
}
