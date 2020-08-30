package kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

	private static final String TOPIC_NAME = "first_topic";

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getName());
		String bootstrapServers = "127.0.0.1:9092";

		// create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);

		// create a producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME,
				"hello world from Programme");

		// send data - asynchronous
		try {
			RecordMetadata metadata = producer.send(record).get();
			System.out.println(String.format("Publish to Topic: %s, partition: %s", TOPIC_NAME, metadata.partition()));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("Record Send: asynchronous");

		// flush data
		producer.flush();
		// flush and close producer
		producer.close();

	}
}
