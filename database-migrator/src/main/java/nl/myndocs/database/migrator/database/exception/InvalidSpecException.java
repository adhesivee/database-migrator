package nl.myndocs.database.migrator.database.exception;

/**
 * @author Mikhail Mikhailov
 * Invalid spec supplied.
 */
public class InvalidSpecException extends RuntimeException {

    /**
     * GSVUID.
     */
    private static final long serialVersionUID = 8585082599124194604L;

    /**
     * Constructor.
     */
    public InvalidSpecException() {
        super();
    }

    /**
     * Constructor.
     * @param message
     */
    public InvalidSpecException(String message) {
        super(message);
    }

    /**
     * Constructor.
     * @param cause
     */
    public InvalidSpecException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     * @param message
     * @param cause
     */
    public InvalidSpecException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public InvalidSpecException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
