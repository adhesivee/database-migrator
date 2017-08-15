package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 13-8-2017.
 */
public class Postgres extends BaseProfile {
    protected String getNativeColumnDefinition(Column column) {
        Column.TYPE columnType = column.getType();
        switch (columnType) {
            case INTEGER:
                if (column.isAutoIncrement() != null && column.isAutoIncrement()) {
                    return "SERIAL";
                }
                return "INTEGER";
            case VARCHAR:
                return "VARCHAR " + getWithSizeIfPossible(column);
            case CHAR:
                return "CHAR " + getWithSizeIfPossible(column);
            case UUID:
                return "UUID";
        }

        throw new RuntimeException("Unknown type");
    }
}
