package nl.myndocs.database.migrator.processor;

import java.util.ArrayList;
import java.util.Collection;

import nl.myndocs.database.migrator.definition.Migration;

/**
 * @author Mikhail Mikhailov
 * Simple extensible class to pass parameters and also to collect migrations, applied with this run.
 */
public class MigrationContext {

    private Collection<Migration> applied = new ArrayList<>();
    /**
     * Constructor.
     */
    public MigrationContext() {
        super();
    }
    /**
     * @return the applied
     */
    public Collection<Migration> getApplied() {
        return applied;
    }

}
