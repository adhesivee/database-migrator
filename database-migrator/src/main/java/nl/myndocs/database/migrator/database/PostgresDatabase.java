package nl.myndocs.database.migrator.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.Constraint.TYPE;
import nl.myndocs.database.migrator.definition.HashPartitionSpec;
import nl.myndocs.database.migrator.definition.Index;
import nl.myndocs.database.migrator.definition.ListPartitionSpec;
import nl.myndocs.database.migrator.definition.Partition;
import nl.myndocs.database.migrator.definition.PartitionSet;
import nl.myndocs.database.migrator.definition.RangePartitionSpec;
import nl.myndocs.database.migrator.definition.Table;

/**
 * Created by albert on 18-8-2017.
 */
public class PostgresDatabase extends DefaultDatabase {

    private static final String DEFAULT_POSTGRES_SCHEMA_NAME = "public";

    private String initialSchema;

    public PostgresDatabase(Connection connection, String schema) {
        super(connection, schema);
    }

    public PostgresDatabase(Connection connection) {
        super(connection);
    }

    @Override
    public void init() {

        String selectedSchema = schema != null ? schema : DEFAULT_POSTGRES_SCHEMA_NAME;
        String currentSchema = this.initialSchema = null;

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery("SELECT CURRENT_SCHEMA()")) {

            if (rs.next()) {
                currentSchema = rs.getString(1);
            }

        } catch (SQLException e) {
            throw new CouldNotProcessException("Failed to query current schema.", e);
        }

        if (selectedSchema.equals(currentSchema)) {
            return;
        }

        // Switch to requested schema
        initialSchema = currentSchema;
        final String[] initSQL = {
            String.format("CREATE SCHEMA IF NOT EXISTS %s", selectedSchema),
            String.format("SET SEARCH_PATH = %s", selectedSchema)
        };

        executeInStatement(initSQL);
    }

    @Override
    public void finish() {
        // Switch back to former schema of the connection
        if (Objects.nonNull(initialSchema)) {
            executeInStatement(String.format("SET SEARCH_PATH = %s", initialSchema));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTable(Table table) {

        if (!table.isPartitioned()) {
            super.updateTable(table);
            return;
        }

        currentTable = table;
        executeInStatement(updateTablePartitionedSQL(table));
    }

    protected List<String> updateTablePartitionedSQL(Table table) {
        return table.getPartitionStream()
                .map(p -> String.format("ALTER TABLE %s DETACH PARTITION %s", table.getTableName(), p.getPartitionName()))
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finishTable(Table table) {

        if (!table.isPartitioned()) {
            super.finishTable(table);
            return;
        }

        executeInStatement(finishTablePartitionedSQL(table));
    }

    protected List<String> finishTablePartitionedSQL(Table table) {
        return table.getPartitionStream()
                .map(p -> String.format("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES %s",
                        table.getTableName(),
                        p.getPartitionName(),
                        createPartitionSpecSQL(table.getPartitions(), p)))
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(Table table, Collection<Column> columns) {

        if (!table.isPartitioned()) {
            super.createTable(table, columns);
        } else {

            currentTable = table;
            alterMode = AlterMode.CREATE_TABLE;

            PartitionSet set = table.getPartitions();
            List<String> statements = new ArrayList<>((set.getSize() * 2) + 1);

            // 1. Create parent table
            statements.add(createTablePartitionedSQL(table.getTableName(), columns, set));

            // 2. Create children, but do not attach
            set.getPartitions().stream()
                .map(p -> createPartitionTablesSQL(table.getTableName(), set, p))
                .collect(Collectors.toCollection(() -> statements));

            // 3. Create sequences if needed
            Collection<Column> aiColumns = table.getNewColumns().stream()
                    .filter(c -> c.getAutoIncrement() != null && c.getAutoIncrement())
                    .collect(Collectors.toList());

            if (!aiColumns.isEmpty()) {
                set.getPartitions().stream()
                    .filter(p -> !p.isForeign())
                    .map(p -> createSequenceSpecSQL(p.getPartitionName(), aiColumns))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toCollection(() -> statements));
            }

            // 4. Immediately detach
            statements.addAll(updateTablePartitionedSQL(table));

            executeInStatement(statements);
        }
    }

    protected String createTablePartitionedSQL(String tableName, Collection<Column> columns, PartitionSet set) {
        String commonSQL = super.createTableSQL(tableName, columns);
        StringBuilder b = new StringBuilder(commonSQL)
                .append(" PARTITION BY ");

        switch (set.getType()) {
        case HASH:
            b.append("HASH");
            break;
        case LIST:
            b.append("LIST");
            break;
        case RANGE:
            b.append("RANGE");
            break;
        default:
            break;
        }

        b.append("(")
         .append(String.join(",", set.getKeyColumns()))
         .append(")");

        return b.toString();
    }

    protected String createPartitionTablesSQL(String parentName, PartitionSet set, Partition partition) {

        if (partition.isForeign()) {
            return String.format("CREATE FOREIGN TABLE %s PARTITION OF %s FOR VALUES %s SERVER %s%s",
                    partition.getPartitionName(),
                    parentName,
                    createPartitionSpecSQL(set, partition),
                    partition.getForeignNode(),
                    partition.getForeignOptions().isEmpty()
                        ? ""
                        : " OPTIONS (" +
                            partition.getForeignOptions().entrySet().stream()
                                .map(entry ->
                                    new StringBuilder()
                                        .append(entry.getKey())
                                        .append(" '")
                                        .append(entry.getValue())
                                        .append("'")
                                        .toString()
                                )
                                .collect(Collectors.joining(","))
                            + ")");
        }

        return String.format("CREATE TABLE %s PARTITION OF %s FOR VALUES %s",
                partition.getPartitionName(),
                parentName,
                createPartitionSpecSQL(set, partition));
    }

    protected String createPartitionSpecSQL(PartitionSet set, Partition partition) {

        switch (set.getType()) {
            case HASH:
                HashPartitionSpec hs = (HashPartitionSpec) partition.getPartitionSpec();
                return "WITH (MODULUS " + set.getPartitions().size() + ", REMAINDER " + hs.getRemainder() + ")";
            case LIST:
                ListPartitionSpec ls = (ListPartitionSpec) partition.getPartitionSpec();
                return "IN (" + String.join(",", ls.getValues()) + ")";
            case RANGE:
                RangePartitionSpec rs = (RangePartitionSpec) partition.getPartitionSpec();
                return "FROM (" + String.join(",", rs.getFrom()) + ") TO (" + String.join(",", rs.getTo()) + ")";
            default:
                break;
        }

        return null;
    }

    protected Collection<String> createSequenceSpecSQL(String tableName, Collection<Column> aiColumns) {

        return aiColumns.stream()
                .flatMap(c -> {
                    final String seqName = new StringBuilder("sq_")
                            .append(tableName)
                            .append("_")
                            .append(c.getColumnName())
                            .toString();

                    return Stream.of(
                        String.format("CREATE SEQUENCE %s AS %s OWNED BY %s.%s", seqName, getNativeColumnDefinition(c), tableName, c.getColumnName()),
                        super.setNotNullSQL(tableName, c.getColumnName()),
                        setDefaultSQL(tableName, c.getColumnName(), "NEXTVAL('" + seqName + "')"));
                    }
                )
                .collect(Collectors.toList());

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDefault() {

        super.setDefault();
        if (!currentTable.isPartitioned()) {
            return;
        }

        executeInStatement(setDefaultPartitionedSQL(currentTable, getCurrentColumn().getDefaultValue()));
    }

    protected List<String> setDefaultPartitionedSQL(Table table, String defaultValue) {
        return table.getPartitionStream()
                .map(p -> setDefaultSQL(p.getPartitionName(), getAlterColumnName(), defaultValue))
                .collect(Collectors.toList());
    }

    @Override
    protected String setDefaultSQL(String tableName, String columnName, String defaultValue) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", tableName, columnName, defaultValue);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void addColumn(Column column) {

        super.addColumn(column);
        if (!currentTable.isPartitioned()) {
            return;
        }

        List<String> statements = new ArrayList<>(addColumnPartitionedSQL(currentTable, column));

        if (column.getAutoIncrement() != null && column.getAutoIncrement()) {
            getCurrentTable().getPartitionStream()
                .map(p -> createSequenceSpecSQL(p.getPartitionName(), Collections.singletonList(column)))
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(() -> statements));
        }

        executeInStatement(statements);
    }

    protected List<String> addColumnPartitionedSQL(Table table, Column column) {
        return table.getPartitionStream()
                .map(p -> super.addColumnSQL(p.getPartitionName(), column))
                .collect(Collectors.toList());
    }

    @Override
    public void changeType() {

        executeInStatement(changeTypeSQL(getAlterTableName(), getCurrentColumn()));
        if (!currentTable.isPartitioned()) {
            return;
        }

        List<String> statements = new ArrayList<>(changeTypePartitionedSQL(currentTable, getCurrentColumn()));

        if (getCurrentColumn().getAutoIncrement() != null && getCurrentColumn().getAutoIncrement()) {
            getCurrentTable().getPartitionStream()
                .map(p -> createSequenceSpecSQL(p.getPartitionName(), Collections.singletonList(getCurrentColumn())))
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(() -> statements));
        }

        executeInStatement(statements);
    }

    protected List<String> changeTypePartitionedSQL(Table table, Column column) {
        return table.getPartitionStream()
                .map(p -> changeTypeSQL(p.getPartitionName(), column))
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String changeTypeSQL(String tableName, Column column) {
        return String.format("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
                getAlterTableName(),
                getAlterColumnName(),
                getNativeColumnDefinition(column));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void dropColumn(String columnName) {

        super.dropColumn(columnName);
        if (!currentTable.isPartitioned()) {
            return;
        }

        executeInStatement(dropColumnPartitionedSQL(currentTable, columnName));
    }

    protected List<String> dropColumnPartitionedSQL(Table table, String columnName) {
        return table.getPartitionStream()
                .map(p -> super.dropColumnSQL(p.getPartitionName(), columnName))
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addConstraint(Constraint constraint) {

        if (alterMode == AlterMode.ALTER_TABLE) {
            executeInStatement(addConstraintSQL(currentTable.getTableName(), constraint));
        } else if (alterMode == AlterMode.ALTER_PARTITION) {
            executeInStatement(addConstraintSQL(currentPartition.getPartitionName(), constraint));
        }
    }

    @Override
    protected String addConstraintSQL(String tableName, Constraint constraint) {

        String result = super.addConstraintSQL(tableName, constraint);
        if ((constraint.getType() == TYPE.PRIMARY_KEY || constraint.getType() == TYPE.UNIQUE)
          && constraint.getIncludeNames() != null && !constraint.getIncludeNames().isEmpty()) {
            result = new StringBuilder(result)
                    .append(String.format(" INCLUDE (%s)", String.join(",", constraint.getIncludeNames())))
                    .toString();
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void dropConstraint(String constraintName) {

        if (alterMode == AlterMode.ALTER_TABLE) {
            executeInStatement(dropConstraintSQL(currentTable.getTableName(), constraintName));
        } else if (alterMode == AlterMode.ALTER_PARTITION) {
            executeInStatement(dropConstraintSQL(currentPartition.getPartitionName(), constraintName));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addIndex(Index index) {

        if (alterMode == AlterMode.ALTER_TABLE) {
            executeInStatement(addIndexSQL(currentTable.getTableName(), index));
        } else if (alterMode == AlterMode.ALTER_PARTITION) {
            executeInStatement(addIndexSQL(currentPartition.getPartitionName(), index));
        }
    }

    protected String addIndexSQL(String tableName, Index index) {

        // CREATE [ UNIQUE ] INDEX [ CONCURRENTLY ] [ [ IF NOT EXISTS ] name ] ON [ ONLY ] table_name [ USING method ]
        //    ( { column_name | ( expression ) } [ COLLATE collation ] [ opclass [ ( opclass_parameter = value [, ... ] ) ] ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
        //    [ INCLUDE ( column_name [, ...] ) ]
        //    [ WITH ( storage_parameter [= value] [, ... ] ) ]
        //    [ TABLESPACE tablespace_name ]
        //    [ WHERE predicate ]

        // 1. Intro
        StringBuilder sqlb = new StringBuilder(
            String.format(index.getType() == Index.TYPE.UNIQUE
                ? "CREATE UNIQUE INDEX %s ON %s"
                : "CREATE INDEX %s ON %s", index.getIndexName(), tableName));

        // 2. Method
        switch (index.getType()) {
        case BTREE:
            sqlb.append(" USING BTREE").toString();
            break;
        case HASH:
            sqlb.append(" USING HASH").toString();
            break;
        case GIN:
            sqlb.append(" USING GIN").toString();
            break;
        case BRIN:
            sqlb.append(" USING BRIN").toString();
            break;
        case GIST:
            sqlb.append(" USING GIST").toString();
            break;
        case SP_GIST:
            sqlb.append(" USING SPGIST").toString();
            break;
        default:
            break;
        }

        // 3. Columns
        sqlb.append(String.format(" (%s)", String.join(",", index.getColumnNames())));

        // 4. Settings (collations, sorting, etc.)
        if (index.getSettings() != null && index.getSettings().length() > 0) {
            sqlb
                .append(" ")
                .append(index.getSettings());
        }

        // 5. Include
        if (index.getIncludeNames() != null && !index.getIncludeNames().isEmpty()) {
            sqlb.append(String.format(" INCLUDE (%s)", String.join(",", index.getIncludeNames())));
        }

        // 6. Storage options
        if (!index.getOptions().isEmpty()) {
            sqlb.append(" WITH ")
                .append(index.getOptions().entrySet().stream()
                        .map(entry -> entry.getKey() + (entry.getValue() != null && entry.getValue().length() > 0 ? " = " + entry.getValue() : ""))
                        .collect(Collectors.joining(", ")));

        }

        // 7. Conditional indexing predicate.
        if (index.getCondition() != null && index.getCondition().length() > 0) {
            sqlb.append(" WHERE ")
                .append(index.getCondition());
        }

        return sqlb.toString();
    }

    @Override
    public void rename() {

        executeInStatement(renameSQL(getAlterTableName(), getCurrentColumn().getRename()));
        if (!currentTable.isPartitioned()) {
            return;
        }

        executeInStatement(renamePartitionedSQL(currentTable, getCurrentColumn().getRename()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String renameSQL(String tableName, String rename) {
        return String.format("ALTER TABLE %s RENAME %s TO %s",
                tableName,
                getAlterColumnName(),
                rename);
    }

    protected List<String> renamePartitionedSQL(Table table, String rename) {
        return table.getPartitionStream()
                .map(p -> renameSQL(p.getPartitionName(), rename))
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNull() {

        super.setNull();
        if (!currentTable.isPartitioned()) {
            return;
        }

        executeInStatement(setNullPartitionedSQL(currentTable, getAlterColumnName()));
    }

    protected List<String> setNullPartitionedSQL(Table table, String columnName) {
        return table.getPartitionStream()
                .map(p -> super.setNullSQL(p.getPartitionName(), columnName))
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNotNull() {

        super.setNotNull();
        if (!currentTable.isPartitioned()) {
            return;
        }

        executeInStatement(setNotNullPartitionedSQL(currentTable, getAlterColumnName()));
    }

    protected List<String> setNotNullPartitionedSQL(Table table, String columnName) {
        return table.getPartitionStream()
                .map(p -> super.setNotNullSQL(p.getPartitionName(), columnName))
                .collect(Collectors.toList());
    }

    @Override
    protected String getNativeColumnDefinition(Column column) {

        switch (column.getType()) {
            case BIG_INTEGER:
                if (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement() && !getCurrentTable().isPartitioned()) {
                    return "BIGSERIAL";
                }
                break;
            case SMALL_INTEGER:
                if (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement() && !getCurrentTable().isPartitioned()) {
                    return "SMALLSERIAL";
                }
                break;
            case INTEGER:
                if (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement() && !getCurrentTable().isPartitioned()) {
                    return "SERIAL";
                }
                break;
            case TIMESTAMPTZ:
                return column.getType().name();
            case BLOB:
                return "BYTEA";
            case CLOB:
                return "TEXT";
            default:
                break;
        }

        return super.getNativeColumnDefinition(column);
    }
}
