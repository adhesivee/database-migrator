package nl.myndocs.database.migrator.database.query;

/**
 * Created by albert on 20-8-2017.
 */
public interface AlterColumn {
    void changeType();

    void setDefault();

    void rename();

    void setNull();

    void setNotNull();
}
