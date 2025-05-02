package financial;

import akka.actor.typed.ActorSystem;
import financial.actors.AuditActor;
import financial.actors.QuoteGeneratorActor;
import financial.actors.TraderActor;
import financial.db.DatabaseInitializer;
import financial.models.Quote;
import financial.models.TradeResult;
import financial.services.AuditService;
import financial.services.KafkaService;

import java.util.List;

public class Main {
    public static void main(String[] args) {

        // DB Initializer
        new DatabaseInitializer(
                "jdbc:postgresql://localhost:5432/postgres",
                "trading_app",
                "postgres",
                "secret"
        ).initialize();

        // Kafka service
        KafkaService kafkaService = new KafkaService(
                "{host_ip}:29092"
                );

        // Audit service
        AuditService auditService = new AuditService(
                "jdbc:postgresql://localhost:5432/trading_app",
                "postgres",
                "secret"
        );

        // Audit actor
        ActorSystem<TradeResult> auditSystem = ActorSystem.create(
                AuditActor.create(auditService),
                "AuditSystem"
        );

        // Quote generator actor
        ActorSystem<QuoteGeneratorActor.Command> quoteGenSystem = ActorSystem.create(
                QuoteGeneratorActor.create(
                        kafkaService,
                        List.of("C-1", "C-2", "C-3", "C-4", "C-5"),
                        "quotes"
                ),
                "QuoteGeneratorSystem"
        );

        // Traders actor
        ActorSystem<Quote> traderA = ActorSystem.create(
                TraderActor.create(
                        kafkaService,
                        auditSystem,
                        "trader-A",
                        70.0,
                        130.0,
                        5
                ),
                "TraderA-System"
        );

        ActorSystem<Quote> traderB = ActorSystem.create(
                TraderActor.create(
                        kafkaService,
                        auditSystem,
                        "trader-B",
                        60.0,
                        140.0,
                        3
                ),
                "TraderB-System"
        );

        // Terminates each actor and closes kafka producer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            quoteGenSystem.terminate();
            traderA.terminate();
            traderB.terminate();
            auditSystem.terminate();
            kafkaService.close();
        }));

        // Keep main thread alive
        quoteGenSystem
                .getWhenTerminated()
                .toCompletableFuture()
                .join();
    }
}
