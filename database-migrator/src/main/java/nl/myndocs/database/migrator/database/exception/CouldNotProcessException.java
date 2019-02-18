package nl.myndocs.database.migrator.database.exception;

/**
 * Created by albert on 17-8-2017.
 */
public class CouldNotProcessException extends RuntimeException {
    /**
     * Generated SVUID.
     */
    private static final long serialVersionUID = 5855580779498154800L;

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
