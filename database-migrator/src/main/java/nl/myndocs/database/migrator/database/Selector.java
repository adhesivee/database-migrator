package nl.myndocs.database.migrator.database;

import java.sql.Connection;
import java.sql.SQLException;

import nl.myndocs.database.migrator.database.exception.UnknownDatabaseTypeException;
import nl.myndocs.database.migrator.database.query.Database;

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
                default: break; // SONAR
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new UnknownDatabaseTypeException();
    }
    /**
     * Restricts operations to a particular schema.
     * @param connection the connection
     * @param schema the schema name
     * @return database instance
     */
    public Database loadFromConnection(Connection connection, String schema) {
        try {
            switch (connection.getMetaData().getDatabaseProductName()) {
                case DERBY_PRODUCT_NAME: return new DerbyDatabase(connection);
                case H2_PRODUCT_NAME: return new H2Database(connection);
                case MYSQL_PRODUCT_NAME: return new MySQLDatabase(connection);
                case HSQL_PRODUCT_NAME: return new HyperSQLDatabase(connection);
                case POSTGRES_PRODUCT_NAME: return new PostgresDatabase(connection, schema);
                default: break; // SONAR
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new UnknownDatabaseTypeException();
    }
}
