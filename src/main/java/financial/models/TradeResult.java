package financial.models;

import java.time.Instant;
import java.util.UUID;

public record TradeResult(
        UUID resultId,
        TradeOrder order,
        boolean success,
        String reason,
        Instant timestamp
) {}
