package nl.myndocs.database.migrator.database;

import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.definition.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Created by albert on 18-8-2017.
 */
public class HyperSQLDatabase extends DefaultDatabase {
    private static final Logger logger = LoggerFactory.getLogger(HyperSQLDatabase.class);
    public HyperSQLDatabase(Connection connection) {
        super(connection);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case BIG_INTEGER:
            case SMALL_INTEGER:
            case INTEGER:
            case UUID:
            case TEXT:
                getNativeColumnDefinition(columnType, new ChangeTypeOptions());
            case VARCHAR:
            case CHAR:
                return getNativeColumnDefinition(columnType, ChangeTypeOptions.ofSize(255));
        }

        return super.getNativeColumnDefinition(columnType);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType, ChangeTypeOptions changeTypeOptions) {
        switch (columnType) {
            case BIG_INTEGER:
            case SMALL_INTEGER:
            case INTEGER:
                return super.getNativeColumnDefinition(columnType) + " " + (changeTypeOptions.getAutoIncrement().orElse(false) ? "IDENTITY" : "");
            case UUID:
                return "UUID";
            case TEXT:
                logger.warn("TEXT not supported, creating CLOB instead");
                return "CLOB";
        }
        return super.getNativeColumnDefinition(columnType, changeTypeOptions);
    }
}
