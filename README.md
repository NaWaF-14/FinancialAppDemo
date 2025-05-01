# Akka Financial App


A simple **fake financial trading simulation** built with **Java 24**, **Akka Typed**, **Kafka**, and **PostgreSQL**. 
This console application generates quotes for fictional companies, runs traders with configurable strategies, and audits all trade results to a database.

## Getting Started

Follow these steps to set up and run the application locally.

### Prerequisites

- **Java 24** (JDK installed and JAVA_HOME set)
- **Maven** (for building the fat JAR)
- **Docker & Docker Compose** (to run Kafka, ZooKeeper, and PostgreSQL)

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd akka-financial-app
```

### 2. Build the Project

```bash
mvn clean package
```

This produces a runnable fat JAR at `target/akka-financial-app-0.1.0.jar`.

### 3. Start Dependencies

In the project root, run:

```bash
docker-compose up -d
```

This will start:
- ZooKeeper on port 2181
- Kafka broker on port 9092
- PostgreSQL on port 5432
- Kafka UI on port 8080

Verify services are healthy:
```bash
docker-compose ps
``` 

### 4. Run the Application

```bash
java -jar target/akka-financial-app-0.1.0.jar
```

You should see logs indicating:
- Quotes being published every 5s
- Trader-A and Trader-B decisions and audits
- Audit persistence confirmations

### 5. Verify Data

Connect to Postgres to inspect the `trade_results` table:

```bash
psql -h localhost -U postgres -d trading_app
SELECT * FROM trade_results LIMIT 10;
```

### 6. Stopping

To stop the app and dependencies:
```bash
# In the JVM:
# Ctrl+C or kill the Java process

# Stop containers
cd path/to/project
Docker-compose down
```