package nl.myndocs.database.migrator.database.query;

import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.Index;

public interface AlterPartition {
    void addConstraint(Constraint constraint);

    void dropConstraint(String constraintName);

    void addIndex(Index index);

    void dropIndex(String indexName);
}
