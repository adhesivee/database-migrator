package nl.myndocs.database.migrator.util;

/**
 * Created by albert on 15-9-2017.
 */
public class Assert {
    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }
}
