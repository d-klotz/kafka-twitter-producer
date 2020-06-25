<h1 align="center">Twitter Kafka Producer</h1>

This application streams real time tweets by any topic(s) using <a href="https://github.com/twitter/hbc">Twitter Hosebird Client</a> that is build on kafka.

## :electric_plug: Requirements

- Java 8
- Maven

# :closed_lock_with_key: Initital Instructions
Clone this repository and install all dependencies.

```shell
# Install all dependencies using Maven
$ mvn install
```

- Create a <a href="https://developer.twitter.com/en/apps">Twitter app</a> and generate the necessary secret keys.
- Include the generated keys in the Producer class.

# :collision: Run the Application

In order to send tweets to the producer we have to start Zookeeper and Kafka.

```shell
# Start Zookeeper
$ zookeeper-server-start.bat config\zookeeper.properties
```

```shell
# Start Kafka
$ kafka-server-start.bat config\server.properties
```

```shell
# Create a topic
$ kafka-topics.bat --zookeeper 127.0.01:2181 --create --topic <YOUT TOPIC HERE> -partitions 6 --replication-factor 1
```

Now you can start the application.

<hr />

### <a href="http://linkedin.com/in/danielfelipeklotz">Contact me on LinkedIn</a>
