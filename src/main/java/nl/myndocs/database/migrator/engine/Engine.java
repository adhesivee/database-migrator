package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by albert on 17-8-2017.
 */
public interface Engine {
    String getNativeColumnDefinition(Column column);


    // @TODO: This should not be here, should be in the processor
    void changeColumnName(Connection connection, Table table, Column column) throws SQLException;

    // @TODO: This should not be here, should be in the processor
    void changeColumnDefault(Connection connection, Table table, Column column) throws SQLException;

    void changeColumnType(Connection connection, Table table, Column column) throws SQLException;

    String getDropForeignKeyTerm();

    String getAlterColumnTerm();

    String getAlterTypeTerm();

    String getDropConstraintTerm();
}
