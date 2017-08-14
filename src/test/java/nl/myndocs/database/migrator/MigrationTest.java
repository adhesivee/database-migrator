package nl.myndocs.database.migrator;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Migration;
import org.junit.Test;

/**
 * Created by albert on 13-8-2017.
 */
public class MigrationTest {

    @Test
    public void testMigration() {
        Migration.Builder builder = new Migration.Builder();

        builder.table("some_table")
                .addColumn("id", Column.TYPE.INTEGER, column -> column.primary(true))
                .addColumn("name", Column.TYPE.VARCHAR, column -> {});

    }
}