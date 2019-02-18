package nl.myndocs.database.migrator.database.exception;

public class UnknownDatabaseTypeException extends RuntimeException {
    /**
     * GSVUID.
     */
    private static final long serialVersionUID = 1481760392317309260L;

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
