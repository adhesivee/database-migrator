package nl.myndocs.database.migrator.database.exception;

public class UnknownDatabaseTypeException extends RuntimeException {
    public UnknownDatabaseTypeException() {
    }

    public UnknownDatabaseTypeException(String message) {
        super(message);
    }

    public UnknownDatabaseTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownDatabaseTypeException(Throwable cause) {
        super(cause);
    }
}
