package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import nl.myndocs.database.migrator.database.exception.InvalidSpecException;

/**
 * @author Mikhail Mikhailov
 * PartitionSet type.
 */
public class PartitionSet {
    public enum TYPE {
        HASH, LIST, RANGE
    }
    private final TYPE type;

    private final Map<String, Partition> partitions;

    private final List<String> keyColumns;
    /**
     * Constructor.
     */
    public PartitionSet(Builder builder) {

        super();

        Objects.requireNonNull(builder.type, "type must not be null");
        Objects.requireNonNull(builder.partitions, "partitions must not be null");
        if (builder.partitions.isEmpty()) {
            throw new InvalidSpecException("partitions must not be empty");
        }

        Objects.requireNonNull(builder.keyColumns, "keyColumns must not be null");
        if (builder.keyColumns.isEmpty()) {
            throw new InvalidSpecException("keyColumns must not be empty");
        }

        this.type = builder.type;
        this.partitions = builder.partitions;
        this.keyColumns = builder.keyColumns;
    }
    /**
     * @return the type
     */
    public TYPE getType() {
        return type;
    }

    public Collection<Partition> getPartitions() {
        return partitions == null ? Collections.emptySet() : partitions.values();
    }

    /**
     * @return the keyColumns
     */
    public Collection<String> getKeyColumns() {
        return keyColumns;
    }

    public boolean isEmpty() {
        return partitions == null || partitions.isEmpty();
    }

    public int getSize() {
        return partitions == null || partitions.isEmpty() ? 0 : partitions.size();
    }

    public static class Builder {
        private TYPE type;

        private Map<String, Partition> partitions;

        private List<String> keyColumns = new ArrayList<>();

        public Builder(PartitionSet.TYPE type) {
            super();
            this.type = type;
        }

        public Builder keyColumn(String column) {
            this.keyColumns.add(column);
            return this;
        }

        public Builder keyColumns(Collection<String> columns) {
            this.keyColumns.addAll(columns);
            return this;
        }

        public Builder keyColumns(String... columns) {
            this.keyColumns.addAll(Arrays.asList(columns));
            return this;
        }

        public Builder partition(Partition partition) {

            Objects.requireNonNull(partition, "partition must not be null");

            if (this.partitions == null) {
                this.partitions =  new HashMap<>();
            }

            this.partitions.put(partition.getPartitionName(), partition);
            return this;
        }

        public Builder partitions(Supplier<Collection<Partition>> partitionSupplier) {

            Objects.requireNonNull(partitionSupplier, "partition supplier must not be null");

            if (this.partitions == null) {
                this.partitions =  new HashMap<>();
            }

            for (Partition p : partitionSupplier.get()) {
                this.partitions.put(p.getPartitionName(), p);
            }
            return this;
        }

        public PartitionSet build() {
            return new PartitionSet(this);
        }
    }
}
