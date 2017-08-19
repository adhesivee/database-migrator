package nl.myndocs.database.migrator.database;

import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.database.query.PhraseTranslator;
import nl.myndocs.database.migrator.database.query.Query;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Table;
import nl.myndocs.database.migrator.validator.TableValidator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static nl.myndocs.database.migrator.database.query.Phrase.*;

/**
 * Created by albert on 17-8-2017.
 */
public class DatabaseCommandsImpl implements DatabaseCommands {

    private static final String ALTER_TABLE_ALTER_DEFAULT = "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s";

    private final Connection connection;
    private final PhraseTranslator phraseTranslator;

    public DatabaseCommandsImpl(Connection connection, PhraseTranslator phraseTranslator) {
        this.connection = connection;
        this.phraseTranslator = phraseTranslator;
    }

    @Override
    public void alterColumnDefault(Table table, Column column) {
        Query query = this.createQuery()
                .query(ALTER_TABLE, ALTER_COLUMN, SET_DEFAULT)
                .setTable(table)
                .setColumn(column);

        executeQuery(query);
    }

    protected void executeQuery(Query query) {
        try {
            executeInStatement(query.getQueries());
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    public void alterColumnName(Table table, Column column) {
        Query query = this.createQuery()
                .query(ALTER_TABLE, ALTER_COLUMN, RENAME)
                .setTable(table)
                .setColumn(column);

        executeQuery(query);
    }

    @Override
    public void alterColumnType(Table table, Column column) {
        Query query = this.createQuery()
                .query(ALTER_TABLE, ALTER_COLUMN, TYPE)
                .setTable(table)
                .setColumn(column);

        executeQuery(query);
    }

    @Override
    public void addColumnsWithCreateTable(Table table) {
        if (table.getNewColumns().size() > 0) {
            Query query = this.createQuery().query(CREATE_TABLE)
                    .setTable(table);

            executeQuery(query);
        }
    }

    @Override
    public void addColumnsWithAlterTable(Table table) {
        for (Column column : table.getNewColumns()) {
            Query query = this.createQuery()
                    .query(ALTER_TABLE, ADD_COLUMN)
                    .setTable(table)
                    .setColumn(column);

            executeQuery(query);
        }
    }

    @Override
    public void addForeignKey(Table table, ForeignKey foreignKey) {
        Query query = this.createQuery()
                .query(ALTER_TABLE, ADD_FOREIGN_KEY)
                .setTable(table)
                .setForeignKey(foreignKey);

        executeQuery(query);
    }

    @Override
    public void dropColumn(Table table, String columnName) {
        Query query = this.createQuery()
                .query(ALTER_TABLE, DROP_COLUMN)
                .setTable(table)
                .setColumnName(columnName);

        executeQuery(query);
    }

    @Override
    public void dropForeignKey(Table table, String constraintName) {
        Query query = this.createQuery()
                .query(ALTER_TABLE, DROP_FOREIGN_KEY)
                .setTable(table)
                .setConstraintName(constraintName);

        executeQuery(query);
    }

    @Override
    public void addConstraint(Table table, Constraint constraint) {
        Query query = this.createQuery()
                .query(ALTER_TABLE, ADD_CONSTRAINT)
                .setTable(table)
                .setConstraint(constraint);

        executeQuery(query);
    }

    @Override
    public void dropConstraint(Table table, String constraintName) {
        Query query = this.createQuery()
                .query(ALTER_TABLE, DROP_CONSTRAINT)
                .setTable(table)
                .setConstraintName(constraintName);

        executeQuery(query);
    }

    protected void executeInStatement(String query) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(query);
        statement.close();
    }

    protected void executeInStatement(String[] queries) throws SQLException {
        Statement statement = connection.createStatement();
        for (String query : queries) {
            statement.execute(query);
        }

        statement.close();
    }

    protected Query createQuery() {
        return new Query(phraseTranslator);
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public TableValidator getTableValidator() {
        return new TableValidator(connection);
    }
}
