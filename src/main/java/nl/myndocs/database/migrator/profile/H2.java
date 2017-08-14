package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.Column;

/**
 * Created by albert on 14-8-2017.
 */
public class H2 extends BaseProfile {
    protected String getNativeColumnDefinition(Column column) {
        Column.TYPE columnType = column.getType();
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (column.isAutoIncrement() ? "AUTO_INCREMENT" : "");
            case VARCHAR:
                return "VARCHAR " + getWithSizeOrDefault(column, "255");
            case CHAR:
                return "CHAR " + getWithSizeOrDefault(column, "255");
            case UUID:
                return "UUID";
        }

        throw new RuntimeException("Unknown type");
    }
}
