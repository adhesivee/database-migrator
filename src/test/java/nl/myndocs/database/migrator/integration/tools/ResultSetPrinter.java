package nl.myndocs.database.migrator.integration.tools;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by albert on 15-8-2017.
 */
public class ResultSetPrinter {
    public void print(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            System.out.format("%1$-24s", metaData.getColumnLabel(i));
        }
        System.out.println();

        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.format("%1$-24s", resultSet.getString(i));
            }
            System.out.println();
        }
    }
}
