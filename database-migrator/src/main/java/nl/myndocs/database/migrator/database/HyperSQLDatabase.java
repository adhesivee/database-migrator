package nl.myndocs.database.migrator.database;

import java.sql.Connection;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.myndocs.database.migrator.definition.Column;

/**
 * Created by albert on 18-8-2017.
 */
public class HyperSQLDatabase extends DefaultDatabase {
    private static final Logger logger = LoggerFactory.getLogger(HyperSQLDatabase.class);
    public HyperSQLDatabase(Connection connection) {
        super(connection);
    }

    @Override
    protected String getNativeColumnDefinition(Column column) {

        switch (column.getType()) {
            case BIG_INTEGER:
            case SMALL_INTEGER:
            case INTEGER:
                return super.getNativeColumnDefinition(column)
                        + " "
                        + (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement() ? "IDENTITY" : "");
            case UUID:
                return "UUID";
            case TEXT:
                logger.warn("TEXT not supported, creating CLOB instead");
                return "CLOB";
            default:
                break;
        }

        return super.getNativeColumnDefinition(column);
    }
}
