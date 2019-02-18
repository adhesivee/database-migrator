package nl.myndocs.database.migrator.definition;

import nl.myndocs.database.migrator.definition.PartitionSet.TYPE;

/**
 * @author Mikhail Mikhailov
 *
 */
public class HashPartitionSpec extends PartitionSpec {

    private final int reminder;
    /**
     * Constructor.
     */
    protected HashPartitionSpec(int remined) {
        super();
        this.reminder = remined;
    }

    /**
     * @return the reminder
     */
    public int getReminder() {
        return reminder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TYPE getType() {
        return PartitionSet.TYPE.HASH;
    }
}
