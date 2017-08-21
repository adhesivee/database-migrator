package nl.myndocs.database.migrator.database.query;

import nl.myndocs.database.migrator.database.query.option.ForeignKeyOptions;
import nl.myndocs.database.migrator.definition.Constraint;

import java.util.Collection;

/**
 * Created by albert on 20-8-2017.
 */
public interface AlterTable {
    AlterColumn alterColumn(String columnName);

    void dropColumn(String columnName);

    void addForeignKey(String constraintName, String foreignTable, Collection<String> localKeys, Collection<String> foreignKeys, ForeignKeyOptions foreignKeyOptions);

    void dropForeignKey(String constraintName);

    void addConstraint(String constraintName, Collection<String> columnNames, Constraint.TYPE type);

    void dropConstraint(String constraintName);
}
