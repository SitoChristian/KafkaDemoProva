package com.cs.kafkaDemoProva.main;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithShutdown {

	private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String groupId = "my-java-application";
		String topic = "demo_java";
		
		//creazione della proprietà del producer
		Properties prop = new Properties();
		
		//connessione al cluster cloud
		prop.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		prop.setProperty("security.protocol", "SASL_SSL");
		prop.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"tu6rpSFipIhEBukZu2eZN\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ0dTZycFNGaXBJaEVCdWtadTJlWk4iLCJvcmdhbml6YXRpb25JZCI6NzA2NzYsInVzZXJJZCI6ODE3OTYsImZvckV4cGlyYXRpb25DaGVjayI6ImNjOWVlZjI1LWI3MzYtNDkzYS05M2YyLTk1NDIyNTVkZTYyYSJ9fQ.QfKNZI0bk9Na2EwWEHLhzzJxBqA0rF7u-MmpD96JfYA\";");
		prop.setProperty("sasl.mechanism", "PLAIN");
		
		//set proprietà del producer
		prop.setProperty("key.deserializer", StringDeserializer.class.getName());
		prop.setProperty("value.deserializer", StringDeserializer.class.getName());
		prop.setProperty("group.id", groupId);
		prop.setProperty("auto.offset.reset", "earliest");
		
		//creazione del consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
		
		//get thread principale
		final Thread mainThread = Thread.currentThread();
			
			
		//aggiunta del gancio(hook) per eseguire lo spegnimento
		Runtime.getRuntime().addShutdownHook( new Thread() {
			public void run() {
				
				log.info("Rilevato uno spegnimento, usciamo chiamando consumer.wakeup()...");
				consumer.wakeup();
				log.info("| consumer.wakeup()");
				

				try {
					mainThread.join();
					log.info("| maintThread.join()");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					log.error("Eccezione: ",e);
				} catch (Exception e) {
					log.error("Eccezione: ",e);
				}
			}
		});
		

		try {	
			//subscribe topic
			consumer.subscribe(Arrays.asList(topic));
			
			int conta = 0;
			//pooling per dati
			while (true) {
				
				conta++;
				
				log.info("Polling");
				
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				
				if(conta>5) {
					log.info("AVVIO SPEGNIMENTO");
					System.exit(0);
				}
				
				for(ConsumerRecord<String, String> record: records) {
					
					log.info("Key: " + record.key() + ", Value: " + record.value());
					log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
			

				}
				
			}
			
		} catch	(WakeupException e) {
			log.info("Consumer in spegnimento");
		} catch (Exception e) {
			log.error("Eccezzione inaspettata nel consumer", e);
		} finally {
			consumer.close(); //spegne il consumer
			log.info("Il consumer si è spetto");
		}
		

	}

}
