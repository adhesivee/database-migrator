package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import nl.myndocs.database.migrator.database.exception.InvalidSpecException;

public class Index {
    public enum TYPE {
        DEFAULT, // Leave it to the DB to choose the right/default type
        BTREE,
        HASH,
        UNIQUE,
        GIN,
        BRIN,
        GIST,
        SP_GIST,
        FULL_TEXT,
        SPATIAL
    }

    private String indexName;
    private TYPE type;
    private Collection<String> columnNames;
    private Collection<String> includeNames;

    private Index(Builder builder) {

        Objects.requireNonNull(builder.indexName, "indexName must not be null");
        Objects.requireNonNull(builder.type, "type must not be null");
        Objects.requireNonNull(builder.columnNames, "columnNames must not be null");

        if (builder.columnNames.isEmpty()) {
            throw new InvalidSpecException("columnNames must not be empty");
        }

        indexName = builder.indexName;
        type = builder.type;
        columnNames = builder.columnNames;
        includeNames = builder.includeNames;
    }

    public String getIndexName() {
        return indexName;
    }

    public TYPE getType() {
        return type;
    }

    public Collection<String> getColumnNames() {
        return columnNames;
    }

    /**
     * @return the includeNames
     */
    public Collection<String> getIncludeNames() {
        return includeNames;
    }

    public static class Builder {
        private String indexName;
        private TYPE type;
        private Collection<String> columnNames = new ArrayList<>();
        private Collection<String> includeNames = new ArrayList<>();

        public Builder(String indexName, TYPE type, Collection<String> columnNames) {
            this.indexName = indexName;
            this.type = type;
            this.columnNames = columnNames;
        }

        public Builder(String indexName, TYPE type) {
            this.indexName = indexName;
            this.type = type;
        }

        public Builder columns(String... columnNames) {
            this.columnNames.addAll(Arrays.asList(columnNames));
            return this;
        }

        public Builder columns(Collection<String> columnNames) {
            this.columnNames.addAll(columnNames);
            return this;
        }

        public Builder include(String... includeNames) {
            this.includeNames.addAll(Arrays.asList(includeNames));
            return this;
        }

        public Builder include(Collection<String> includeNames) {
            this.includeNames.addAll(includeNames);
            return this;
        }

        public Index build() {
            return new Index(this);
        }
    }
}
