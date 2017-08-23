package nl.myndocs.database.migrator.database;

import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.definition.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Created by albert on 18-8-2017.
 */
public class DerbyDatabase extends DefaultDatabase {
    private static final Logger logger = LoggerFactory.getLogger(DerbyDatabase.class);

    public DerbyDatabase(Connection connection) {
        super(connection);
    }

    @Override
    public void changeType(Column.TYPE type, ChangeTypeOptions changeTypeOptions) {
        String[] alters = new String[]{
                "ALTER TABLE %1$s ADD COLUMN %2$s_newtype %3$s",
                "UPDATE %1$s SET %2$s_newtype = %2$s",
                "ALTER TABLE %1$s DROP COLUMN %2$s",
                "RENAME COLUMN %1$s.%2$s_newtype TO %2$s"
        };


        for (String alter : alters) {
            executeInStatement(String.format(
                    alter,
                    getAlterTableName(),
                    getAlterColumnName(),
                    getNativeColumnDefinition(type)
                    )
            );

        }
    }

    @Override
    public void rename(String rename) {
        executeInStatement(
                String.format(
                        "RENAME COLUMN %s.%s TO %s",
                        getAlterTableName(),
                        getAlterColumnName(),
                        rename
                )
        );
    }

    @Override
    public String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case INTEGER:
            case UUID:
                return getNativeColumnDefinition(columnType, ChangeTypeOptions.empty());
            case VARCHAR:
                return getNativeColumnDefinition(columnType, ChangeTypeOptions.ofSize(255));
            case CHAR:
                return getNativeColumnDefinition(columnType, ChangeTypeOptions.ofSize(254));
        }

        throw new RuntimeException("Unknown type");
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType, ChangeTypeOptions changeTypeOptions) {
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (changeTypeOptions.getAutoIncrement().orElse(false) ? "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)" : "");
            case UUID:
                logger.warn("UUID not supported, creating CHAR(36) instead");
                return getNativeColumnDefinition(Column.TYPE.CHAR, ChangeTypeOptions.ofSize(36));
        }

        return super.getNativeColumnDefinition(columnType, changeTypeOptions);
    }
}
