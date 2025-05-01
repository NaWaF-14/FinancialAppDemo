package financial.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import financial.models.OrderType;
import financial.models.Quote;
import financial.models.TradeOrder;
import financial.models.TradeResult;
import financial.services.KafkaService;

import java.time.Instant;
import java.util.UUID;

public class TraderActor extends AbstractBehavior<Quote> {

    private final KafkaService kafkaService;
    ActorRef<TradeResult> auditActor;
    private final String traderId;
    private final double buyThreshold;
    private final double sellThreshold;
    private final int quantity;

    private TraderActor(
            ActorContext<Quote> context,
            KafkaService kafkaService,
            ActorRef<TradeResult> auditActor,
            String traderId,
            double buyThreshold,
            double sellThreshold,
            int quantity
    ) {
        super(context);
        this.kafkaService = kafkaService;
        this.auditActor = auditActor;
        this.traderId = traderId;
        this.buyThreshold = buyThreshold;
        this.sellThreshold = sellThreshold;
        this.quantity = quantity;

        // Consume quotes from quote topic
        kafkaService.consume("quotes", "quotes-" + traderId, Quote.class, context.getSelf()::tell);
        context.getLog().info("TraderActor[{}] started: buy≤{} sell≥{}",
                traderId, buyThreshold, sellThreshold);
    }

    public static Behavior<Quote> create(
            KafkaService kafkaService,
            ActorRef<TradeResult> auditActor,
            String traderId,
            double buyThreshold,
            double sellThreshold,
            int quantity
    ) {
        return Behaviors.setup(ctx ->
                new TraderActor(ctx, kafkaService, auditActor,
                        traderId, buyThreshold, sellThreshold, quantity)
        );
    }

    @Override
    public Receive<Quote> createReceive() {
        return newReceiveBuilder()
                .onMessage(Quote.class, this::onQuote)
                .build();
    }

    private Behavior<Quote> onQuote(Quote quote) {
        double price = quote.price();
        OrderType type = null;

        if (price <= buyThreshold) {
            type = OrderType.BUY;
        } else if (price >= sellThreshold) {
            type = OrderType.SELL;
        }

        if (type != null) {
            TradeOrder order = new TradeOrder(
                    UUID.randomUUID(),
                    traderId,
                    quote.company(),
                    type,
                    quantity,
                    price,
                    Instant.now()
            );

            TradeResult result = new TradeResult(
                    UUID.randomUUID(),
                    order,
                    true,
                    "",
                    Instant.now()
            );

            auditActor.tell(result);
            getContext().getLog().info(
                    "Trader[{}] {} {}@{} qty={} → audited",
                    traderId, type, quote.company(), price, quantity
            );
        }

        return this;
    }
}
