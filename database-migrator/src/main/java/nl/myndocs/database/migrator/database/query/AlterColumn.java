package nl.myndocs.database.migrator.database.query;

import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.definition.Column;

/**
 * Created by albert on 20-8-2017.
 */
public interface AlterColumn {
    void changeType(Column.TYPE type, ChangeTypeOptions changeTypeOptions);

    void setDefault(String defaultValue);

    void rename(String rename);
}
