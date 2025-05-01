package financial.models;

import java.time.Instant;

public record Quote(
        String company,
        double price,
        Instant timestamp
) {}
