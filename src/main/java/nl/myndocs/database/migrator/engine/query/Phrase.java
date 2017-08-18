package nl.myndocs.database.migrator.engine.query;

public enum Phrase {
    ALTER_TABLE,
    ALTER_COLUMN,
    SET_DEFAULT,
    RENAME,
    TYPE,
    ADD_FOREIGN_KEY,
    DROP_FOREIGN_KEY,
    ADD_CONSTRAINT,
    DROP_CONSTRAINT,
    DROP_COLUMN
}
