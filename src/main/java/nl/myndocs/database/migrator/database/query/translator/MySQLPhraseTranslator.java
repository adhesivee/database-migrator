package nl.myndocs.database.migrator.database.query.translator;

import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.database.query.Phrase;
import nl.myndocs.database.migrator.database.query.Query;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by albert on 18-8-2017.
 */
public class MySQLPhraseTranslator extends DefaultPhraseTranslator {
    private static final Logger logger = LoggerFactory.getLogger(MySQLPhraseTranslator.class);
    private final Connection connection;

    private Map<Phrase, Function<Query, String>> phrasesMap = new HashMap<>();

    public MySQLPhraseTranslator(Connection connection) {
        super();

        this.connection = connection;

        phrasesMap.put(Phrase.DROP_FOREIGN_KEY, query -> "DROP FOREIGN KEY " + query.getConstraintName());
        phrasesMap.put(Phrase.DROP_CONSTRAINT, query -> "DROP INDEX " + query.getConstraintName());
    }

    @Override
    protected Function<Query, String> translatePhrase(Phrase phrase) {
        return phrasesMap.getOrDefault(phrase, super.translatePhrase(phrase));
    }

    @Override
    public String[] translatePhrases(Query query, Phrase... phrases) {
        if (query.equals(Phrase.ALTER_TABLE, Phrase.ALTER_COLUMN, Phrase.RENAME)) {
            return new String[]{translatePhrase(Phrase.ALTER_TABLE).apply(query) + " " + alterColumnName().apply(query)};
        }

        if (query.equals(Phrase.ALTER_TABLE, Phrase.ALTER_COLUMN, Phrase.TYPE)) {
            return new String[]{
                    buildQuery(
                            query,
                            translatePhrase(Phrase.ALTER_TABLE),
                            buildQuery -> "MODIFY COLUMN " + buildQuery.getColumn().getColumnName(),
                            translatePhrase(Phrase.TYPE)
                    )
            };
        }

        return super.translatePhrases(query, phrases);
    }

    private Function<Query, String> alterColumnName() {
        return query -> {
            Table table = query.getTable();
            Column column = query.getColumn();

            DatabaseColumn databaseColumn = loadDatabaseColumn(table, column);
            return String.format(
                    "CHANGE %s %s %s %s %s",
                    column.getColumnName(),
                    column.getRename().get(),
                    databaseColumn.getColumnType(),
                    (databaseColumn.getColumnDefault() != null && !databaseColumn.getColumnDefault().isEmpty() ? "DEFAULT '" + databaseColumn.getColumnDefault() + "'" : ""),
                    databaseColumn.getNotNullValue()
            );
        };
    }


    private DatabaseColumn loadDatabaseColumn(Table table, Column column) {
        try {
            Statement statement = connection.createStatement();
            statement.execute("DESCRIBE " + table.getTableName());

            ResultSet resultSet = statement.getResultSet();

            String notNullValue = "";
            String columnType = "";
            String columnDefault = "";

            while (resultSet.next()) {
                if (resultSet.getString("Field").equals(column.getColumnName())) {
                    if ("NO".equals(resultSet.getString("Null"))) {
                        notNullValue = "NOT NULL";
                    }

                    columnType = resultSet.getString("Type");
                    columnDefault = resultSet.getString("Default");

                }
            }

            statement.close();

            return new DatabaseColumn(notNullValue, columnType, columnDefault);
        } catch (SQLException sqlException) {
            throw new CouldNotProcessException(sqlException);
        }
    }

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
                logger.warn("UUID not supported, creating CHAR(36) instead");
                return "CHAR " + getWithSizeOrDefault(column, 36);
        }

        throw new RuntimeException("Unknown type");
    }

    private static class DatabaseColumn {
        private final String notNullValue;
        private final String columnType;
        private final String columnDefault;

        public DatabaseColumn(String notNullValue, String columnType, String columnDefault) {
            this.notNullValue = notNullValue;
            this.columnType = columnType;
            this.columnDefault = columnDefault;
        }

        public String getNotNullValue() {
            return notNullValue;
        }

        public String getColumnType() {
            return columnType;
        }

        public String getColumnDefault() {
            return columnDefault;
        }
    }
}
