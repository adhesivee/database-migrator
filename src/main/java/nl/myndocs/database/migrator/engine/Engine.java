package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Table;
import nl.myndocs.database.migrator.validator.TableValidator;

/**
 * Created by albert on 17-8-2017.
 */
public interface Engine {
    void alterColumnName(Table table, Column column);

    void alterColumnDefault(Table table, Column column);

    void alterColumnType(Table table, Column column);

    void addColumnsWithCreateTable(Table table);

    void addColumnsWithAlterTable(Table table);

    void addForeignKey(Table table, ForeignKey foreignKey);

    void dropForeignKey(Table table, String constraintName);

    void dropColumn(Table table, String columnName);

    void addConstraint(Table table, Constraint constraint);

    void dropConstraint(Table table, String constraintName);

    TableValidator getTableValidator();
}
