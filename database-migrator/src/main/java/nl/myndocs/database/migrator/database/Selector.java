package nl.myndocs.database.migrator.database;

import nl.myndocs.database.migrator.database.exception.UnknownDatabaseTypeException;
import nl.myndocs.database.migrator.database.query.Database;

import java.sql.Connection;
import java.sql.SQLException;

public class Selector {
    private static final String DERBY_PRODUCT_NAME = "Apache Derby";
    private static final String H2_PRODUCT_NAME = "H2";
    private static final String HSQL_PRODUCT_NAME = "HSQL Database Engine";
    private static final String MYSQL_PRODUCT_NAME = "MySQL";
    private static final String POSTGRES_PRODUCT_NAME = "PostgreSQL";

    public Database loadFromConnection(Connection connection) {
        try {
            switch (connection.getMetaData().getDatabaseProductName()) {
                case DERBY_PRODUCT_NAME: return new DerbyDatabase(connection);
                case H2_PRODUCT_NAME: return new H2Database(connection);
                case MYSQL_PRODUCT_NAME: return new MySQLDatabase(connection);
                case HSQL_PRODUCT_NAME: return new HyperSQLDatabase(connection);
                case POSTGRES_PRODUCT_NAME: return new PostgresDatabase(connection);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new UnknownDatabaseTypeException();
    }
}
