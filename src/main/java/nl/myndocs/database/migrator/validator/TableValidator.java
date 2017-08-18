package nl.myndocs.database.migrator.validator;

import nl.myndocs.database.migrator.engine.exception.CouldNotProcessException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableValidator {
    private final Connection connection;

    public TableValidator(Connection connection) {
        this.connection = connection;
    }

    public boolean tableExists(String tableName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});

            boolean tableExists = false;
            while (tables.next()) {
                if (tableName.equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
                    tableExists = true;
                }
            }

            return tableExists;
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }
}
