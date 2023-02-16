package nl.myndocs.database.migrator.definition;

import nl.myndocs.database.migrator.definition.PartitionSet.TYPE;

/**
 * @author Mikhail Mikhailov
 *
 */
public class RangePartitionSpec extends PartitionSpec {

    private final String[] from;

    private final String[] to;

    /**
     * Constructor.
     */
    protected RangePartitionSpec(String[] from, String[] to) {
        super();
        this.from = from;
        this.to = to;
    }

    /**
     * @return the from
     */
    public String[] getFrom() {
        return from;
    }

    /**
     * @return the to
     */
    public String[] getTo() {
        return to;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TYPE getType() {
        return PartitionSet.TYPE.RANGE;
    }
}
