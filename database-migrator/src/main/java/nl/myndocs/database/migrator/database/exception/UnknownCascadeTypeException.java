package nl.myndocs.database.migrator.database.exception;

/**
 * @author Mikhail Mikhailov
 *
 */
public class UnknownCascadeTypeException extends RuntimeException {
    /**
     * GSVUID.
     */
    private static final long serialVersionUID = 3485669440207000564L;

    /**
     * Constructor.
     */
    public UnknownCascadeTypeException() {
        super();
    }

    /**
     * Constructor.
     * @param message
     */
    public UnknownCascadeTypeException(String message) {
        super(message);
    }

    /**
     * Constructor.
     * @param cause
     */
    public UnknownCascadeTypeException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     * @param message
     * @param cause
     */
    public UnknownCascadeTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public UnknownCascadeTypeException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
