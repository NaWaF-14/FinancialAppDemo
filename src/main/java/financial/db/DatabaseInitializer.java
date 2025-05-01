package financial.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseInitializer {
    private final String dbUrl;
    private final String dbName;
    private final String dbUser;
    private final String dbPassword;

    public DatabaseInitializer(
            String dbUrl,
            String targetDbName,
            String dbUser,
            String dbPassword
    ) {
        this.dbUrl = dbUrl;
        this.dbName = targetDbName;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    public void initialize() {
        createDatabaseIfNotExists();
        createTablesIfNotExists();
    }

    private void createDatabaseIfNotExists() {
        String sql = "CREATE DATABASE " + dbName;
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            System.out.println("Created database " + dbName);
        } catch (SQLException e) {
            throw new RuntimeException("Error creating database", e);
        }
    }

    private void createTablesIfNotExists() {
        // Connect to the DB
        String database = dbUrl.replace("/postgres", "/" + dbName);
        String ddl = """
            CREATE TABLE IF NOT EXISTS trade_results (
                result_id   UUID PRIMARY KEY,
                order_id    UUID NOT NULL,
                trader_id   VARCHAR(50),
                company      VARCHAR(10),
                type        VARCHAR(4),
                quantity    INTEGER,
                price       DOUBLE PRECISION,
                success     BOOLEAN,
                reason      TEXT,
                event_time  TIMESTAMP
            );
            """;

        try (Connection conn = DriverManager.getConnection(database, dbUser, dbPassword);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(ddl);
            System.out.println("Ensured trade_results table exists");
        } catch (SQLException e) {
            throw new RuntimeException("Error creating tables", e);
        }
    }
}
