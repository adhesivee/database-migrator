package nl.myndocs.database.migrator.database;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

    public PostgresDatabase(Connection connection, String schema) {
        super(connection, schema);
        init();
    }

    public PostgresDatabase(Connection connection) {
        super(connection);
        init();
    }

    private void init() {

        String selectedSchema = schema != null ? schema : DEFAULT_POSTGRES_SCHEMA_NAME;
        final String[] initSQL = {
            String.format("CREATE SCHEMA IF NOT EXISTS %s", selectedSchema),
            String.format("SET SEARCH_PATH = %s", selectedSchema)
        };

        executeInStatement(initSQL);
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
            PartitionSet set = table.getPartitions();
            List<String> statements = new ArrayList<>((set.getSize() * 2) + 1);

            // 1. Create parent table
            statements.add(createTablePartitionedSQL(table.getTableName(), columns, set));

            // 2. Create children, but do not attach
            set.getPartitions().stream()
                .map(p -> createPartitionTablesSQL(table.getTableName(), set, p))
                .collect(Collectors.toCollection(() -> statements));

            // 3. Immediately detach
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
        return String.format("CREATE TABLE %s PARTITION OF %s FOR VALUES %s",
                partition.getPartitionName(),
                parentName,
                createPartitionSpecSQL(set, partition));
    }

    protected String createPartitionSpecSQL(PartitionSet set, Partition partition) {

        switch (set.getType()) {
            case HASH:
                HashPartitionSpec hs = (HashPartitionSpec) partition.getPartitionSpec();
                return "WITH (MODULUS " + set.getPartitions().size() + ", REMAINDER " + hs.getReminder() + ")";
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
                .map(p -> super.setDefaultSQL(p.getPartitionName(), getAlterColumnName(), defaultValue))
                .collect(Collectors.toList());
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

        executeInStatement(addColumnPartitionedSQL(currentTable, column));
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

        executeInStatement(changeTypePartitionedSQL(currentTable, getCurrentColumn()));
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

    protected List<String> changeTypePartitionedSQL(Table table, Column column) {
        return table.getPartitionStream()
                .map(p -> changeTypeSQL(p.getPartitionName(), column))
                .collect(Collectors.toList());
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

        StringBuilder sqlb = new StringBuilder(
            String.format(index.getType() == Index.TYPE.UNIQUE
                ? "CREATE UNIQUE INDEX %s ON %s"
                : "CREATE INDEX %s ON %s", index.getIndexName(), tableName));

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

        sqlb.append(String.format(" (%s)", String.join(",", index.getColumnNames())));
        if (index.getIncludeNames() != null && !index.getIncludeNames().isEmpty()) {
            sqlb.append(String.format(" INCLUDE (%s)", String.join(",", index.getIncludeNames())));
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
                if (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement()) {
                    return "BIGSERIAL";
                }
                break;
            case SMALL_INTEGER:
                if (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement()) {
                    return "SMALLSERIAL";
                }
                break;
            case INTEGER:
                if (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement()) {
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
