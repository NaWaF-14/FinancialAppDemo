package financial.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import financial.models.TradeResult;
import financial.services.AuditService;

public class AuditActor extends AbstractBehavior<TradeResult> {

    private final AuditService auditService;

    private AuditActor(
            ActorContext<TradeResult> context,
            AuditService auditService
    ) {
        super(context);
        this.auditService = auditService;
        context.getLog().info("AuditActor started");
    }

    public static Behavior<TradeResult> create(AuditService auditService) {
        return Behaviors.setup(context ->
                new AuditActor(context, auditService)
        );
    }

    @Override
    public Receive<TradeResult> createReceive() {
        return newReceiveBuilder()
                .onMessage(TradeResult.class, this::onTradeResult)
                .build();
    }

    private Behavior<TradeResult> onTradeResult(TradeResult result) {
        try {
            auditService.save(result);
            getContext().getLog().info(
                    "Audited order {} (success={})",
                    result.order().orderId(),
                    result.success()
            );
        } catch (Exception e) {
            getContext().getLog().error(
                    "Failed to audit {}: {}",
                    result.order().orderId(),
                    e.getMessage(),
                    e
            );
        }
        return this;
    }
}
