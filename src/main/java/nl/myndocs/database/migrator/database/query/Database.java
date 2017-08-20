package nl.myndocs.database.migrator.database.query;

import nl.myndocs.database.migrator.database.query.option.ColumnOptions;

import java.util.Collection;

/**
 * Created by albert on 20-8-2017.
 */
public interface Database {
    void createTable(String tableName, Collection<ColumnOptions> columnOptions);

    AlterTable alterTable(String tableName);
}
