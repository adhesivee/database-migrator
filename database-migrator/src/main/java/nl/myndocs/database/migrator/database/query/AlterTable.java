package nl.myndocs.database.migrator.database.query;

import nl.myndocs.database.migrator.database.query.option.ColumnOptions;
import nl.myndocs.database.migrator.database.query.option.ForeignKeyOptions;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.Index;

import java.util.Collection;

/**
 * Created by albert on 20-8-2017.
 */
public interface AlterTable {
    AlterColumn alterColumn(String columnName);

    void addColumn(ColumnOptions columnOption);

    void dropColumn(String columnName);

    void addForeignKey(String constraintName, String foreignTable, Collection<String> localKeys, Collection<String> foreignKeys, ForeignKeyOptions foreignKeyOptions);

    void dropForeignKey(String constraintName);

    @Deprecated
    /**
     * {@link AlterTable#addIndex(String, Collection, Index.TYPE)}
     */
    void addConstraint(String constraintName, Collection<String> columnNames, Constraint.TYPE type);

    @Deprecated
    /**
     * {@link AlterTable#dropIndex(String)}
     */
    void dropConstraint(String constraintName);

    void addIndex(String indexName, Collection<String> columnNames, Index.TYPE type);

    void dropIndex(String indexName);
}
