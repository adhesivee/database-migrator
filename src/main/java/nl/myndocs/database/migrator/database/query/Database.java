package nl.myndocs.database.migrator.database.query;

/**
 * Created by albert on 20-8-2017.
 */
public interface Database {
    AlterTable alterTable(String tableName);
}
