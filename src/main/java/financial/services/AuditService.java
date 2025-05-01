package financial.services;

import financial.models.TradeResult;

import java.sql.*;

public class AuditService {
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public AuditService(String dbUrl, String dbUser, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }

    public void save(TradeResult r) {
        String sql = """
            INSERT INTO trade_results
            (result_id, order_id, trader_id, company, type, quantity, price, success, reason, event_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setObject(1, r.resultId());
            ps.setObject(2, r.order().orderId());
            ps.setString(3, r.order().traderId());
            ps.setString(4, r.order().company());
            ps.setString(5, r.order().type().name());
            ps.setInt(6, r.order().quantity());
            ps.setDouble(7, r.order().price());
            ps.setBoolean(8, r.success());
            ps.setString (9, r.reason());
            ps.setTimestamp(10, Timestamp.from(r.timestamp()));

            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to save trade result", e);
        }
    }
}
