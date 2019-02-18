package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * @author Mikhail Mikhailov
 * Raw content.
 */
public class Raw {
    private final Collection<String> rawSQL;
    /**
     * Constructor.
     */
    private Raw(Raw.Builder builder) {
        super();
        this.rawSQL = builder.rawSQL;
    }

    /**
     * @return the rawSQL
     */
    public Collection<String> getRawSQL() {
        return rawSQL;
    }

    public static class Builder {

        private Collection<String> rawSQL = new ArrayList<>();
        private Consumer<Raw> consumer;

        public Builder(Consumer<Raw> consumer) {
            super();
            this.consumer = consumer;
        }

        public Raw.Builder sql(String sql) {
            rawSQL.add(sql);
            return this;
        }
        // TODO
        /*
        public Raw.Builder sql(InputStream sql) {
            rawSQL.add(sql);
            return this;
        }
        */
        public Raw build() {
            return new Raw(this);
        }

        public void save() {
            consumer.accept(build());
        }
    }
}
