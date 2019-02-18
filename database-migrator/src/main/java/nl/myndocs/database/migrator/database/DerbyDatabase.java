package nl.myndocs.database.migrator.database;

import java.sql.Connection;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.myndocs.database.migrator.definition.Column;

/**
 * Created by albert on 18-8-2017.
 */
public class DerbyDatabase extends DefaultDatabase {
    private static final Logger logger = LoggerFactory.getLogger(DerbyDatabase.class);

    public DerbyDatabase(Connection connection) {
        super(connection);
    }

    @Override
    public void changeType() {
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
                    getNativeColumnDefinition(getCurrentColumn())
                    )
            );

        }
    }

    @Override
    public void rename() {
        executeInStatement(
                String.format(
                        "RENAME COLUMN %s.%s TO %s",
                        getAlterTableName(),
                        getAlterColumnName(),
                        getCurrentColumn().getRename()
                )
        );
    }

    @Override
    protected String getNativeColumnDefinition(Column column) {

        switch (column.getType()) {
            case BIG_INTEGER:
            case INTEGER:
            case SMALL_INTEGER:
                return super.getNativeColumnDefinition(column)
                        + " "
                        + (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement()
                                ? "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)"
                                : "");
            case UUID:
                logger.warn("UUID not supported, creating CHAR(36) instead");
                return "CHAR(36)";
            case TEXT:
                logger.warn("TEXT not supported, creating CLOB instead");
                return "CLOB";
            default:
                break;
        }

        return super.getNativeColumnDefinition(column);
    }
}
