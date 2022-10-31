package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    private boolean unique;
    private String condition;
    private Map<String, String> options;
    private String settings;

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
        unique = builder.unique;
        condition = builder.condition;
        options = builder.options;
        settings = builder.settings;
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

    /**
     * @return the unique
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * @return the condition
     */
    public String getCondition() {
        return condition;
    }

    /**
     * @return the options
     */
    public Map<String, String> getOptions() {
        return Objects.isNull(options) ? Collections.emptyMap() : options;
    }

    /**
     * @return the settings
     */
    public String getSettings() {
        return settings;
    }

    public static class Builder {
        private String indexName;
        private TYPE type;
        private Collection<String> columnNames = new ArrayList<>();
        private Collection<String> includeNames = new ArrayList<>();
        private boolean unique;
        private String condition;
        private Map<String, String> options;
        private String settings;

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

        public Builder unique(boolean unique) {
            this.unique = unique;
            return this;
        }

        public Builder condiiton(String condition) {
            this.condition = condition;
            return this;
        }

        public Builder option(String name, String value) {
            return options(Collections.singletonMap(name, value));
        }

        public Builder options(Map<String, String> options) {

            if (options != null && !options.isEmpty()) {
                if (this.options == null) {
                    this.options = new HashMap<>();
                }

                this.options.putAll(options);
            }
            return this;
        }

        public Builder settings(String settings) {
            this.settings = settings;
            return this;
        }

        public Index build() {
            return new Index(this);
        }
    }
}
