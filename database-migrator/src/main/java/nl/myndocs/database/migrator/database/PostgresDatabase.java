package nl.myndocs.database.migrator.database;

import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.definition.Column;

import java.sql.Connection;

/**
 * Created by albert on 18-8-2017.
 */
public class PostgresDatabase extends DefaultDatabase {
    public PostgresDatabase(Connection connection) {
        super(connection);
    }

    @Override
    public void changeType(Column.TYPE type, ChangeTypeOptions changeTypeOptions) {
        String alterTypeFormat = "ALTER TABLE %s ALTER COLUMN %s TYPE %s";

        executeInStatement(
                String.format(
                        alterTypeFormat,
                        getAlterTableName(),
                        getAlterColumnName(),
                        getNativeColumnDefinition(type, new ChangeTypeOptions())
                )
        );
    }

    @Override
    public void rename(String rename) {
        executeInStatement(
                String.format(
                        "ALTER TABLE %s RENAME %s TO %s",
                        getAlterTableName(),
                        getAlterColumnName(),
                        rename
                )
        );
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case BIG_INTEGER:
            case INTEGER:
            case UUID:
                getNativeColumnDefinition(columnType, new ChangeTypeOptions());
        }

        return super.getNativeColumnDefinition(columnType);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType, ChangeTypeOptions changeTypeOptions) {
        switch (columnType) {
            case BIG_INTEGER:
                if (changeTypeOptions.getAutoIncrement().orElse(false)) {
                    return "BIGSERIAL";
                }
            case SMALL_INTEGER:
                if (changeTypeOptions.getAutoIncrement().orElse(false)) {
                    return "SMALLSERIAL";
                }
            case INTEGER:
                if (changeTypeOptions.getAutoIncrement().orElse(false)) {
                    return "SERIAL";
                }
                return super.getNativeColumnDefinition(columnType);
            case UUID:
                return "UUID";
        }
        return super.getNativeColumnDefinition(columnType, changeTypeOptions);
    }
}
