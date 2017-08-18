package nl.myndocs.database.migrator.database.query.translator;

import nl.myndocs.database.migrator.definition.Column;

/**
 * Created by albert on 18-8-2017.
 */
public class H2PhraseTranslator extends DefaultPhraseTranslator {
    @Override
    public String getNativeColumnDefinition(Column column) {
        Column.TYPE columnType = column.getType().get();
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (column.getAutoIncrement().orElse(false) ? "AUTO_INCREMENT" : "");
            case VARCHAR:
                return "VARCHAR " + getWithSizeOrDefault(column, 255);
            case CHAR:
                return "CHAR " + getWithSizeOrDefault(column, 255);
            case UUID:
                return "UUID";
        }

        throw new RuntimeException("Unknown type");
    }
}
