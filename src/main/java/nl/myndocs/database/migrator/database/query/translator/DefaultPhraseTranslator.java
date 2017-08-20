package nl.myndocs.database.migrator.database.query.translator;

import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.database.query.*;
import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static nl.myndocs.database.migrator.database.query.Phrase.ADD_COLUMN;

/**
 * Created by albert on 18-8-2017.
 */
public class DefaultPhraseTranslator implements PhraseTranslator, Database, AlterTable, AlterColumn {

    private final Connection connection;
    private String alterTableName;
    private String alterColumnName;

    protected String getAlterTableName() {
        return alterTableName;
    }

    protected String getAlterColumnName() {
        return alterColumnName;
    }

    @Override
    public AlterTable alterTable(String tableName) {
        alterTableName = tableName;
        return this;
    }

    @Override
    public AlterColumn alterColumn(String columnName) {
        alterColumnName = columnName;
        return this;
    }

    @Override
    public void changeType(Column.TYPE type, ChangeTypeOptions changeTypeOptions) {
        String alterColumnFormat = "ALTER TABLE %s ALTER COLUMN %s %s";

        String alterQuery = String.format(
                alterColumnFormat,
                alterTableName,
                alterColumnName,
                getNativeColumnDefinition(type, changeTypeOptions)
        );

        System.out.println(alterQuery);
        executeInStatement(alterQuery);
    }

    private Map<Phrase, Function<Query, String>> phrasesMap = new HashMap<>();

    public DefaultPhraseTranslator(Connection connection) {
        this.connection = connection;
        phrasesMap.put(Phrase.ALTER_TABLE, query -> "ALTER TABLE " + query.getTable().getTableName());
        phrasesMap.put(Phrase.ALTER_COLUMN, query -> "ALTER COLUMN " + query.getColumn().getColumnName());
        phrasesMap.put(Phrase.SET_DEFAULT, query -> "SET DEFAULT '" + query.getColumn().getDefaultValue().get() + "'");
        phrasesMap.put(Phrase.RENAME, query -> "RENAME TO " + query.getColumn().getRename().get());
        phrasesMap.put(Phrase.TYPE, query -> {
                    Column column = query.getColumn();

                    return getNativeColumnDefinition(
                            column.getType().get(),
                            new ChangeTypeOptions(column.getAutoIncrement(), column.getSize())
                    );
                }
        );
        phrasesMap.put(Phrase.DROP_COLUMN, query -> "DROP COLUMN " + query.getColumnName());
        phrasesMap.put(Phrase.DROP_FOREIGN_KEY, query -> "DROP CONSTRAINT " + query.getConstraintName());
        phrasesMap.put(Phrase.DROP_CONSTRAINT, query -> "DROP CONSTRAINT " + query.getConstraintName());
        phrasesMap.put(Phrase.ADD_CONSTRAINT, query -> {
            Constraint constraint = query.getConstraint();
            return String.format(
                    "ADD CONSTRAINT %s %s (%s)",
                    constraint.getConstraintName(),
                    getNativeConstraintType(constraint.getType().get()),
                    String.join(",", constraint.getColumnNames())
            );
        });
        phrasesMap.put(Phrase.ADD_FOREIGN_KEY, query -> {
            ForeignKey foreignKey = query.getForeignKey();
            StringBuilder alterForeignKeyQueryBuilder = new StringBuilder();

            alterForeignKeyQueryBuilder.append(
                    String.format(
                            "ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                            foreignKey.getConstraintName(),
                            String.join(",", foreignKey.getLocalKeys()),
                            foreignKey.getForeignTable(),
                            String.join(",", foreignKey.getForeignKeys())
                    )
            );

            if (foreignKey.getDeleteCascade().isPresent()) {
                alterForeignKeyQueryBuilder.append(" ON DELETE " + getNativeCascadeType(foreignKey.getDeleteCascade().get()));
            }

            if (foreignKey.getUpdateCascade().isPresent()) {
                alterForeignKeyQueryBuilder.append(" ON UPDATE " + getNativeCascadeType(foreignKey.getUpdateCascade().get()));
            }

            return alterForeignKeyQueryBuilder.toString();
        });
        phrasesMap.put(Phrase.CREATE_TABLE, query -> {
            Table table = query.getTable();

            List<String> createColumnQueries = table.getNewColumns()
                    .stream()
                    .map(column -> query.newCleanInstance()
                            .query(ADD_COLUMN)
                            .setColumn(column)
                            .getQueries()
                    )
                    .flatMap(queries -> Arrays.asList(queries).stream())
                    .collect(Collectors.toList());

            return String.format(
                    "CREATE TABLE %s (%s)",
                    table.getTableName(),
                    String.join(",", createColumnQueries)
            );
        });
        phrasesMap.put(Phrase.ADD_COLUMN, query -> {
            Column column = query.getColumn();

            boolean isAlterTable = query.getPhrases()
                    .get(0)
                    .equals(Phrase.ALTER_TABLE);

            return (isAlterTable ? "ADD COLUMN " : "") +
                    column.getColumnName() + " " +
                    getNativeColumnDefinition(
                            column.getType().get(),
                            new ChangeTypeOptions(column.getAutoIncrement(), column.getSize())
                    ) + " " +
                    getDefaultValue(column) + " " +
                    (column.getIsNotNull().orElse(false) ? "NOT NULL" : "") + " " +
                    (column.getPrimary().orElse(false) ? "PRIMARY KEY" : "") + " ";
        });
    }

    protected String getNativeCascadeType(ForeignKey.CASCADE cascade) {
        switch (cascade) {
            case RESTRICT:
            case SET_NULL:
            case SET_DEFAULT:
            case NO_ACTION:
            case CASCADE:
                return cascade.name().replace("_", " ");
        }
        throw new RuntimeException("Unknown type");
    }

    protected String getNativeConstraintType(Constraint.TYPE type) {
        switch (type) {
            case INDEX:
            case UNIQUE:
                return type.name();
        }

        throw new RuntimeException("Could not process native constraint type");
    }

    protected String getNativeColumnDefinition(Column.TYPE columnType) {
        return columnType.name();
    }

    protected String getNativeColumnDefinition(Column.TYPE columnType, ChangeTypeOptions changeTypeOptions) {
        if (!changeTypeOptions.getColumnSize().isPresent()) {
            return getNativeColumnDefinition(columnType);
        }

        return columnType.name() + "(" + changeTypeOptions.getColumnSize().get() + ")";
    }

    protected Function<Query, String> translatePhrase(Phrase phrase) {
        return phrasesMap.get(phrase);
    }

    @Override
    public String[] translatePhrases(Query query, Phrase... phrases) {

        StringBuilder stringBuilder = new StringBuilder();

        for (Phrase phrase : query.getPhrases()) {
            stringBuilder.append(translatePhrase(phrase).apply(query) + " ");
        }

        System.out.println(stringBuilder.toString());

        return new String[]{stringBuilder.toString()};
    }

    protected String getDefaultValue(Column column) {
        String quote = "";

        List<Column.TYPE> quotedTypes = Arrays.asList(
                Column.TYPE.CHAR,
                Column.TYPE.VARCHAR
        );
        if (quotedTypes.contains(column.getType().get())) {
            quote = "'";
        }

        return (column.getDefaultValue().isPresent() ? "DEFAULT " + quote + column.getDefaultValue().get() + quote + "" : "");
    }

    protected String buildQuery(Query query, Function<Query, String>... queryBuilders) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Function<Query, String> queryBuilder : queryBuilders) {
            stringBuilder.append(queryBuilder.apply(query) + " ");
        }

        return stringBuilder.toString();
    }

    protected String getWithSizeIfPresent(Column column) {
        if (column.getSize().isPresent()) {
            return "(" + column.getSize().get() + ")";
        }

        return "";
    }

    protected String getWithSizeOrDefault(Column column, int defaultSize) {
        return "(" + column.getSize().orElse(defaultSize) + ")";
    }

    protected void executeInStatement(String query) {
        executeInStatement(new String[]{query});
    }

    protected void executeInStatement(String[] queries) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            for (String query : queries) {
                statement.execute(query);
            }

        } catch (SQLException sqlException) {
            throw new CouldNotProcessException(sqlException);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
    }
}
