package financial.models;

import java.time.Instant;
import java.util.UUID;

public record TradeOrder(
        UUID orderId,
        String traderId,
        String company,
        OrderType type,
        int quantity,
        double price,
        Instant timestamp
) {}