package nl.myndocs.database.migrator.definition;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.processor.MigrationContext;

/**
 * @author Mikhail Mikhailov
 * Raw content.
 */
public class Raw {
    private final Collection<RawSqlHolder> rawSQL;
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
    public Collection<RawSqlHolder> getRawSQL() {
        return rawSQL;
    }

    public static class Builder {

        private Collection<RawSqlHolder> rawSQL = new ArrayList<>();
        private Consumer<Raw> consumer;

        public Builder(Consumer<Raw> consumer) {
            super();
            this.consumer = consumer;
        }

        public Raw.Builder sql(String... sql) {
            return sql(null, sql);
        }

        public Raw.Builder sql(Predicate<MigrationContext> check, String... sql) {

            for (int i = 0; sql != null && i < sql.length; i++) {

                if (Objects.isNull(sql[i]) || sql[i].length() == 0) {
                    continue;
                }

                rawSQL.add(new RawSqlHolder(check, null, sql[i]));
            }
            return this;
        }

        public Raw.Builder sql(Function<MigrationContext, Collection<String>> sql) {
            rawSQL.add(new RawSqlHolder(null, sql, null));
            return this;
        }

        public Raw.Builder sql(InputStream... is) {
            return sql(null, is);
        }

        public Raw.Builder sql(Predicate<MigrationContext> check, InputStream... is) {

            for (int i = 0; is != null && i < is.length; i++) {

                if (Objects.isNull(is[i])) {
                    continue;
                }

                try (InputStreamReader isr = new InputStreamReader(is[i], StandardCharsets.UTF_8)) {

                    StringBuilder output = new StringBuilder(8192);
                    char[] buf = new char[8192];
                    int count = -1;
                    while ((count = isr.read(buf, 0, buf.length)) != -1) {
                        output.append(buf, 0, count);
                    }

                    rawSQL.add(new RawSqlHolder(check, null, output.toString()));
                } catch (IOException ioe) {
                    throw new CouldNotProcessException("I/O error while reading input stream for raw SQL.", ioe);
                }

            }

            return this;
        }

        public Raw build() {
            return new Raw(this);
        }

        public void save() {
            consumer.accept(build());
        }
    }

    public static class RawSqlHolder {

        private final Predicate<MigrationContext> condition;
        private final Function<MigrationContext, Collection<String>> generator;
        private final String value;
        /**
         * Constructor.
         * Either condition or generator can be specified, but not both.
         * @param condition the condition
         * @param generator the generator
         * @param value the plain value
         */
        RawSqlHolder(
                Predicate<MigrationContext> condition,
                Function<MigrationContext, Collection<String>> generator,
                String value) {
            this.condition = condition;
            this.generator = generator;
            this.value = value;
        }

        public Predicate<MigrationContext> getCondition() {
            return condition;
        }

        /**
         * @return the generator
         */
        public Function<MigrationContext, Collection<String>> getGenerator() {
            return generator;
        }

        public String getValue() {
            return value;
        }

        public boolean isConditional() {
            return condition != null;
        }

        public boolean isGenerated() {
            return generator != null;
        }

        @Override
        public String toString() {
            return (isConditional() ? "conditional" : "not conditional") + ", " +
                   (isGenerated() ?  "generated" : "plain") + ", [" +
                   value + "]";
        }
    }
}
