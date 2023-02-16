package nl.myndocs.database.migrator.definition;

import nl.myndocs.database.migrator.definition.PartitionSet.TYPE;

/**
 * @author Mikhail Mikhailov
 *
 */
public class HashPartitionSpec extends PartitionSpec {

    private final int remainder;
    /**
     * Constructor.
     */
    protected HashPartitionSpec(int remainder) {
        super();
        this.remainder = remainder;
    }

    /**
     * @return the remainder
     */
    public int getRemainder() {
        return remainder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TYPE getType() {
        return PartitionSet.TYPE.HASH;
    }
}
