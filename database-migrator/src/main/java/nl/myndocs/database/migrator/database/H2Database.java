package nl.myndocs.database.migrator.database;

import java.sql.Connection;
import java.util.Objects;

import nl.myndocs.database.migrator.definition.Column;

/**
 * Created by albert on 18-8-2017.
 */
public class H2Database extends DefaultDatabase {
    public H2Database(Connection connection) {
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
                        + (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement() ? "AUTO_INCREMENT" : "");
            case UUID:
                return "UUID";
            default:
                break;
        }

        return super.getNativeColumnDefinition(column);
    }
}
