package nl.myndocs.database.migrator.engine.query;

public enum Phrase {
    CREATE_TABLE,
    ALTER_TABLE,
    ALTER_COLUMN,
    SET_DEFAULT,
    RENAME,
    TYPE,
    ADD_FOREIGN_KEY,
    DROP_FOREIGN_KEY,
    ADD_CONSTRAINT,
    DROP_CONSTRAINT,
    DROP_COLUMN,
    ADD_COLUMN
}
