package nl.myndocs.database.migrator.database.exception;

/**
 * Created by albert on 17-8-2017.
 */
public class CouldNotProcessException extends RuntimeException {
    public CouldNotProcessException() {
    }

    public CouldNotProcessException(String message) {
        super(message);
    }

    public CouldNotProcessException(String message, Throwable cause) {
        super(message, cause);
    }

    public CouldNotProcessException(Throwable cause) {
        super(cause);
    }
}
