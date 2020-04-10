package nl.myndocs.database.migrator.database.query;

import java.sql.Connection;
import java.util.Collection;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Partition;
import nl.myndocs.database.migrator.definition.Table;

/**
 * Created by albert on 20-8-2017.
 */
public interface Database {

    void init();

    void finish();

    void createTable(Table table, Collection<Column> columns);

    void updateTable(Table table);

    AlterTable alterTable(Table table);

    AlterPartition alterPartition(Partition partition);

    boolean hasTable(String tableName);

    void finishTable(Table table);

    Connection getConnection();

    String getInitialSchema();
}
