package com.klotz.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Producer {

    Logger logger = LoggerFactory.getLogger(Producer.class.getName());

    //Include your twitter secrets here. Read the README for more information.
    private final String CONSUMER_KEY = "1WhxJ38bu8xzEadWh4Je7xggp";
    private final String CONSUMER_SECRET = "3fZx6J3CZKgrjITS4vds5zhZoOdQwoboHHmNsKKEUKZzUpVkfI";
    private final String TOKEN = "2909527319-f5sBuZzDtcEBadfGUIjzFFMubHSPS8jIFlEfuQv";
    private final String SECRET = "q4CW669ejcsVAoWrSlXCFYcapvFSaIkKShekj8Zvws4uq";

    //You can set whatever terms you want to stream
    private final List<String> TRACKED_TERMS = Lists.newArrayList("kafka");

    public static void main(String[] args) {
        new Producer().run();
    }

    public void run() {
        logger.info("Setting up ...");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();

        //Exiting the application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping the Application...");
            logger.info("Shutting down Twitter Client...");
            client.stop();
            logger.info("Closing Producer...");
            producer.close();
            logger.info("done.");
        }));


        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if(msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();;
        hosebirdEndpoint.trackTerms(TRACKED_TERMS);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET,
                TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("dklotz-kafka-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                         // optional: use this if you want to process client events

        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //safe properties
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");//avoids the consumption of the same request twice in case of network errors
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // all means that all replicas inside a partition have received the request
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); // max number of retries in case of network error
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // controls how many producer requests can be made to a broker in paralel


        // high throughput properties (at the expense of a bit latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //20ms of delay introduced to the producer
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //When there are too many request not send yet, kafka sends it via batch



        //creates a kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
