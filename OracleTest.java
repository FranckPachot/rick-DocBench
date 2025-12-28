import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.Connection;

public class OracleTest {
    public static void main(String[] args) throws Exception {
        PoolDataSource ds = PoolDataSourceFactory.getPoolDataSource();
        ds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        ds.setURL("jdbc:oracle:thin:@localhost:1521/FREEPDB1");
        ds.setUser("translator");
        ds.setPassword("translator123");
        ds.setInitialPoolSize(1);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            System.out.println("Connected successfully!");

            OracleRDBMSClient client = new OracleRDBMSClient();
            OracleDatabase db = client.getDatabase(conn);
            System.out.println("Got SODA database handle");

            // Try to create a collection
            OracleCollection coll = db.openCollection("test_collection");
            if (coll == null) {
                System.out.println("Collection doesn't exist, creating...");
                coll = db.admin().createCollection("test_collection");
                System.out.println("Collection created successfully!");
            } else {
                System.out.println("Collection already exists");
            }

            conn.commit();
        }
    }
}
