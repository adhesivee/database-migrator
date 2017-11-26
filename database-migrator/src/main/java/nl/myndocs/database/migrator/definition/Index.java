package nl.myndocs.database.migrator.definition;

import nl.myndocs.database.migrator.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class Index {
    public enum TYPE {
        INDEX, UNIQUE, PRIMARY_KEY
    }

    private String indexName;
    private TYPE type;
    private Collection<String> columnNames = new ArrayList<>();

    private Index(Builder builder) {
        indexName = builder.getIndexName();
        type = builder.getType();
        columnNames = builder.getColumnNames();
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

    public static class Builder {
        private String indexName;
        private TYPE type;
        private Collection<String> columnNames = new ArrayList<>();

        public Builder(String indexName, TYPE type, Collection<String> columnNames) {
            Assert.notNull(indexName, "indexName must not be null");
            Assert.notNull(type, "type must not be null");
            Assert.notNull(columnNames, "columnNames must not be null");

            this.indexName = indexName;
            this.type = type;
            this.columnNames = columnNames;
        }

        public Builder columns(String... columnNames) {
            this.columnNames = Arrays.asList(columnNames);

            return this;
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

        public Index build() {
            return new Index(this);
        }
    }
}
