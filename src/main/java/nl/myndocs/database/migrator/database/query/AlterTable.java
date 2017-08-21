package nl.myndocs.database.migrator.database.query;

/**
 * Created by albert on 20-8-2017.
 */
public interface AlterTable {
    AlterColumn alterColumn(String columnName);

    void dropColumn(String columnName);
}
