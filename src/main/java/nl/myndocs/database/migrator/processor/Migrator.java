package nl.myndocs.database.migrator.processor;

import nl.myndocs.database.migrator.MigrationScript;
import nl.myndocs.database.migrator.database.DatabaseCommands;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by albert on 15-8-2017.
 */
public class Migrator {
    private static final String CHANGE_LOG_TABLE = "migration_changelog";

    private final DatabaseCommands databaseCommands;

    public Migrator(DatabaseCommands databaseCommands) {
        this.databaseCommands = databaseCommands;
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

            PreparedStatement insertPreparedStatement = connection.prepareStatement("INSERT INTO " + CHANGE_LOG_TABLE + " (migration_id) VALUES (?)");
            insertPreparedStatement.setString(1, migration.getMigrationId());
            insertPreparedStatement.execute();
        }
    }
}
