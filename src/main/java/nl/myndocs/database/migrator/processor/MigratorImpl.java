package nl.myndocs.database.migrator.processor;

import nl.myndocs.database.migrator.definition.*;
import nl.myndocs.database.migrator.engine.Engine;

import java.sql.*;

/**
 * Created by albert on 15-8-2017.
 */
public class MigratorImpl implements Migrator {
    private static final String CREATE_FOREIGN_KEY_FORMAT = "FOREIGN KEY (%s) REFERENCES %s (%s)";
    private final Engine engine;

    public MigratorImpl(Engine engine) {
        this.engine = engine;
    }

    public void migrate(Migration migration) throws SQLException {
        for (Table table : migration.getTables()) {

            if (engine.getTableValidator().tableExists(table.getTableName())) {
                engine.addColumnsWithAlterTable(table);
            } else {
                engine.addColumnsWithCreateTable(table);
            }

            table.getDropColumns().forEach(column -> engine.dropColumn(table, column));
            table.getDropForeignKeys().forEach(constraintName -> engine.dropForeignKey(table, constraintName));
            table.getNewConstraints().forEach(constraint -> engine.addConstraint(table, constraint));
            table.getDropConstraints().forEach(constraintName -> engine.dropConstraint(table, constraintName));
            table.getNewForeignKeys().forEach(foreignKey -> engine.addForeignKey(table, foreignKey));


            for (Column column : table.getChangeColumns()) {
                if (column.getType().isPresent()) {
                    engine.alterColumnType(table, column);
                }

                if (column.getDefaultValue().isPresent()) {
                    engine.alterColumnDefault(table, column);
                }
            }

            // Make sure renames always happens last
            // Otherwise alterColumnType and alterColumnDefault will break
            for (Column column : table.getChangeColumns()) {
                if (column.getRename().isPresent()) {
                    engine.alterColumnName(table, column);
                }
            }
        }
    }
}
