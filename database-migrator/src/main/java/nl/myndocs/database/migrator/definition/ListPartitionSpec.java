package nl.myndocs.database.migrator.definition;

import nl.myndocs.database.migrator.definition.PartitionSet.TYPE;

/**
 * @author Mikhail Mikhailov
 *
 */
public class ListPartitionSpec extends PartitionSpec {

    private final String[] values;
    /**
     * Constructor.
     */
    protected ListPartitionSpec(String[] values) {
        super();
        this.values = values;
    }

    /**
     * @return the values
     */
    public String[] getValues() {
        return values;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TYPE getType() {
        return PartitionSet.TYPE.LIST;
    }

}
