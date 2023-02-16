package nl.myndocs.database.migrator.definition;

/**
 * @author Mikhail Mikhailov
 *
 */
public abstract class PartitionSpec {

    /**
     * Constructor.
     */
    public PartitionSpec() {
        super();
    }

    public abstract PartitionSet.TYPE getType();

    public static PartitionSpec of (int remainder) {
        return new HashPartitionSpec(remainder);
    }

    public static PartitionSpec of (String... values) {
        return new ListPartitionSpec(values);
    }

    public static PartitionSpec of (String[] from, String[] to) {
        return new RangePartitionSpec(from, to);
    }
}
