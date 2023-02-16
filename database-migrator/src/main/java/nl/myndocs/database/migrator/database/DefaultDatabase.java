package nl.myndocs.database.migrator.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.database.exception.UnknownCascadeTypeException;
import nl.myndocs.database.migrator.database.query.AlterColumn;
import nl.myndocs.database.migrator.database.query.AlterPartition;
import nl.myndocs.database.migrator.database.query.AlterTable;
import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Index;
import nl.myndocs.database.migrator.definition.Index.TYPE;
import nl.myndocs.database.migrator.definition.Partition;
import nl.myndocs.database.migrator.definition.Table;

/**
 * Created by albert on 18-8-2017.
 */
public class DefaultDatabase implements Database, AlterTable, AlterPartition, AlterColumn {

    enum AlterMode {
        CREATE_TABLE,
        ALTER_TABLE,
        ALTER_PARTITION
    }

    private final Connection connection;
    protected Table currentTable;
    protected Column currentColumn;
    protected Partition currentPartition;
    protected AlterMode alterMode;
    protected String schema;

    public DefaultDatabase(Connection connection) {
        this.connection = connection;
    }

    public DefaultDatabase(Connection connection, String schema) {
        this.connection = connection;
        this.schema = schema;
    }

    @Override
    public void init() {
        // Nothing
    }

    @Override
    public void finish() {
        // Nothing
    }
    /**
     * @return the currentTable
     */
    protected Table getCurrentTable() {
        return currentTable;
    }
    /**
     * @return the currentColumn
     */
    protected Column getCurrentColumn() {
        return currentColumn;
    }

    protected String getAlterTableName() {
        return currentTable.getTableName();
    }

    protected String getAlterColumnName() {
        return currentColumn.getColumnName();
    }

    @Override
    public AlterTable alterTable(Table table) {
        currentTable = table;
        alterMode = AlterMode.ALTER_TABLE;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AlterPartition alterPartition(Partition partition) {
        currentPartition = partition;
        alterMode = AlterMode.ALTER_PARTITION;
        return this;
    }

    @Override
    public AlterColumn alterColumn(Column column) {
        currentColumn = column;
        return this;
    }

    @Override
    public void setDefault() {
        executeInStatement(setDefaultSQL(getAlterTableName(), getAlterColumnName(), getCurrentColumn().getDefaultValue()));
    }

    protected String setDefaultSQL(String tableName, String columnName, String defaultValue) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT '%s'",
                tableName,
                columnName,
                escapeString(defaultValue));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTable(Table table) {
        currentTable = table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finishTable(Table table) {
        currentTable = null;
        alterMode = null;
    }

    @Override
    public void createTable(Table table, Collection<Column> columns) {
        currentTable = table;
        alterMode = AlterMode.CREATE_TABLE;
        executeInStatement(createTableSQL(table.getTableName(), columns));
    }

    protected String createTableSQL(String tableName, Collection<Column> columns) {

        Collection<String> columnQueries = new ArrayList<>();
        for (Column columnOption : columns) {
            columnQueries.add(translateColumnOptions(columnOption));
        }

        return String.format("CREATE TABLE %s (%s)",
                    tableName,
                    String.join(",", columnQueries));
    }

    @Override
    public void addColumn(Column column) {
        executeInStatement(addColumnSQL(getAlterTableName(), column));
    }

    protected String addColumnSQL(String tableName, Column column) {
        return String.format("ALTER TABLE %s ADD COLUMN %s",
                tableName,
                translateColumnOptions(column));
    }

    @Override
    public void changeType() {
        executeInStatement(changeTypeSQL(getAlterTableName(), getCurrentColumn()));
    }

    protected String changeTypeSQL(String tableName, Column column) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s %s",
                tableName,
                getAlterColumnName(),
                getNativeColumnDefinition(column));
    }

    @Override
    public void dropColumn(String columnName) {
        executeInStatement(dropColumnSQL(getAlterTableName(), columnName));
    }

    protected String dropColumnSQL(String tableName, String columnName) {
        return String.format("ALTER TABLE %s DROP COLUMN %s",
                tableName,
                columnName);
    }

    @Override
    public void addConstraint(Constraint constraint) {
        executeInStatement(addConstraintSQL(getAlterTableName(), constraint));
    }

    protected String addConstraintSQL(String tableName, Constraint constraint) {

        String constraintName = constraint.getConstraintName();
        Collection<String> columnNames = constraint.getColumnNames();
        Constraint.TYPE type = constraint.getType();

        switch (type) {
        case PRIMARY_KEY:
            return String.format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
                    tableName,
                    constraintName,
                    String.join(",", columnNames));
        case UNIQUE:
            return String.format("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
                    tableName,
                    constraintName,
                    String.join(",", columnNames));
        case FOREIGN_KEY:

            String foreignTable = constraint.getForeignKey().getForeignTable();
            Collection<String> foreignNames = constraint.getForeignKey().getForeignKeys();
            StringBuilder fkb = new StringBuilder();
            fkb.append(
                    String.format(
                            "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                            tableName,
                            constraintName,
                            String.join(",", columnNames),
                            foreignTable,
                            String.join(",", foreignNames)
                    )
            );

            if (constraint.getForeignKey().getDeleteCascade() != null) {
                fkb.append(" ON DELETE " + getNativeCascadeType(constraint.getForeignKey().getDeleteCascade()));
            }

            if (constraint.getForeignKey().getUpdateCascade() != null) {
                fkb.append(" ON UPDATE " + getNativeCascadeType(constraint.getForeignKey().getUpdateCascade()));
            }

            return fkb.toString();
        case CHECK:
            return String.format("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
                    tableName,
                    constraintName,
                    constraint.getCheckExpression());
        default:
            break;
        }

        return null;
    }

    @Override
    public void dropConstraint(String constraintName) {
        executeInStatement(dropConstraintSQL(getAlterTableName(), constraintName));
    }

    protected String dropConstraintSQL(String tableName, String constraintName) {
        return String.format("ALTER TABLE %s DROP CONSTRAINT %s", tableName, constraintName);
    }

    @Override
    public void addIndex(Index index) {
        executeInStatement(addIndexSQL(getAlterTableName(), index.getIndexName(), index.getColumnNames(), index.getType()));
    }

    protected String addIndexSQL(String tableName, String indexName, Collection<String> columnNames, Index.TYPE type) {

        String sql = type == TYPE.UNIQUE
                ? "CREATE UNIQUE INDEX %s ON %s (%s)"
                : "CREATE INDEX %s ON %s (%s)";

        return String.format(sql, indexName, tableName, String.join(",", columnNames));
    }

    @Override
    public void dropIndex(String indexName) {
        executeInStatement(dropIndexSQL(indexName));
    }

    protected String dropIndexSQL(String indexName) {
        return String.format("DROP INDEX %s", indexName);
    }

    @Override
    public void rename() {
        executeInStatement(renameSQL(getAlterTableName(), getCurrentColumn().getRename()));
    }

    protected String renameSQL(String tableName, String rename) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s RENAME TO %s",
                        tableName,
                        getAlterColumnName(),
                        rename);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNull() {
        executeInStatement(setNullSQL(getAlterTableName(), getAlterColumnName()));
    }

    protected String setNullSQL(String tableName, String columnName) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
                tableName,
                columnName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNotNull() {
        executeInStatement(setNotNullSQL(getAlterTableName(), getAlterColumnName()));
    }

    protected String setNotNullSQL(String tableName, String columnName) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
                tableName,
                columnName);
    }

    @Override
    public boolean hasTable(String tableName) {
        try {

            final String schemaPattern = Objects.nonNull(schema) ? schema + "%" : null;
            final String namePattern = tableName + "%";
            final String[] typesFilter = new String[]{ "TABLE", "PARTITIONED TABLE" };

            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(null, schemaPattern, namePattern, typesFilter);

            boolean tableExists = false;
            while (tables.next()) {

                final String other = tables.getString("TABLE_NAME");
                if (tableName.equalsIgnoreCase(other)) {
                    tableExists = true;
                    break;
                }
            }

            return tableExists;
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    /**
     * @return the schema
     */
    @Override
    public String getInitialSchema() {
        return schema;
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

        throw new UnknownCascadeTypeException("Unknown type");
    }

    /*
     * Column type and size may be an invalid combination,
     * but this is up to the user, to avoid such a situation.
     */
    protected String getNativeColumnDefinition(Column column) {

        StringBuilder sb = new StringBuilder();
        switch (column.getType()) {
            case SMALL_INTEGER:
                sb.append("SMALLINT");
                break;
            case BIG_INTEGER:
                sb.append("BIGINT");
                break;
            case TIMESTAMPTZ:
                sb.append("TIMESTAMP");
                break;
            case UDT:
                sb.append(column.getUDT());
                break;
            default:
                sb.append(column.getType().name());
                break;
        }

        if (Objects.nonNull(column.getSize())) {
            sb.append("(")
              .append(column.getSize())
              .append(")");
        }

        return sb.toString();
    }

    protected String getDefaultValue(Column column) {

        String quote = "";
        List<Column.TYPE> quotedTypes = Arrays.asList(
                Column.TYPE.CHAR,
                Column.TYPE.VARCHAR,
                Column.TYPE.TEXT
        );

        if (quotedTypes.contains(column.getType())) {
            quote = "'";
        }

        return "DEFAULT " + quote + column.getDefaultValue() + quote;
    }

    protected void executeInStatement(String query) {
        executeInStatement(new String[]{query});
    }

    protected void executeInStatement(List<String> queries) {
        executeInStatement(queries.toArray(new String[queries.size()]));
    }

    protected void executeInStatement(String[] queries) {

        try (Statement statement = connection.createStatement()) {
            for (String query : queries) {
                statement.execute(query);
            }
        } catch (SQLException sqlException) {
            throw new CouldNotProcessException(sqlException);
        }
    }

    protected String translateColumnOptions(Column column) {

        StringBuilder cb = new StringBuilder(column.getColumnName())
                .append(" ")
                .append(getNativeColumnDefinition(column));

        if (Objects.nonNull(column.getDefaultValue())) {
            cb.append(" ")
              .append(getDefaultValue(column));
        }

        if (Objects.nonNull(column.getIsNotNull())) {
            cb.append(" ")
              .append("NOT NULL");
        }

        if (Objects.nonNull(column.getPrimary())) {
            cb.append(" ")
              .append("PRIMARY KEY");
        }

        return cb.toString();
    }

    protected String escapeString(String line) {
        return line.replaceAll("'", "''");
    }
}
