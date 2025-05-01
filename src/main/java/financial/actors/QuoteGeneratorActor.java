package financial.actors;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import financial.models.Quote;
import financial.services.KafkaService;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;

public class QuoteGeneratorActor extends AbstractBehavior<QuoteGeneratorActor.Command> {

    public interface Command {}

    public enum Tick implements Command { INSTANCE }

    private final KafkaService kafkaService;
    private final List<String> companies;
    private final String topic;
    private final Random random = new Random();

    private QuoteGeneratorActor(
            ActorContext<Command> context,
            TimerScheduler<Command> timers,
            KafkaService kafkaService,
            List<String> companies,
            String topic
    ) {
        super(context);
        this.kafkaService = kafkaService;
        this.companies = companies;
        this.topic  = topic;

        // Schedule a recurring Tick every 5 seconds
        timers.startTimerAtFixedRate(Tick.INSTANCE, Duration.ofSeconds(5));
        context.getLog().info("QuoteGeneratorActor started, publishing to '{}'", topic);
    }

    public static Behavior<Command> create(
            KafkaService kafkaService,
            List<String> companies,
            String topic
    ) {
        return Behaviors.setup(context ->
                Behaviors.withTimers(timers ->
                        new QuoteGeneratorActor(context, timers, kafkaService, companies, topic)
                )
        );
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Tick.class, this::onTick)
                .build();
    }

    private Behavior<Command> onTick(Tick tick) {
        for (String company : companies) {
            double price = 50 + random.nextDouble() * 100;
            Quote quote = new Quote(company, price, Instant.now());

            // Push to quote queue
            kafkaService.produce(topic, company, quote);
            getContext().getLog().info("Published quote {} @ {}", company, price);
        }
        return this;
    }
}
