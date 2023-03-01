package com.cs.kafkaDemoProva.main;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
	
	public static void main(String[] args) {
		
		log.info("Starting ProducerDemoWithCallback");
		
		//creazione della proprietà del producer
		Properties prop = new Properties();
		
		//connessione al cluster cloud
		prop.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		prop.setProperty("security.protocol", "SASL_SSL");
		prop.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"tu6rpSFipIhEBukZu2eZN\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJ0dTZycFNGaXBJaEVCdWtadTJlWk4iLCJvcmdhbml6YXRpb25JZCI6NzA2NzYsInVzZXJJZCI6ODE3OTYsImZvckV4cGlyYXRpb25DaGVjayI6ImNjOWVlZjI1LWI3MzYtNDkzYS05M2YyLTk1NDIyNTVkZTYyYSJ9fQ.QfKNZI0bk9Na2EwWEHLhzzJxBqA0rF7u-MmpD96JfYA\";");
		prop.setProperty("sasl.mechanism", "PLAIN");
		
		//set proprietà del producer
		prop.setProperty("key.serializer", StringSerializer.class.getName());
		prop.setProperty("value.serializer", StringSerializer.class.getName());
		prop.setProperty("batch.size", "400");
		
		//creazione dell'oggeto producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
		
		for(int j = 0; j < 10; j++) {
			
			for(int i = 0; i < 30; i++) {
				
				//creazione del produttore
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello Word " + i);
				
				//send data - send permette di inviare i dati in modo asincrono
				producer.send(producerRecord, new Callback() {
					
					//viene eseguito ogni volta che un record viene inviato con successo o viene lanciata un'eccezione.
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						// TODO Auto-generated method stub
						
						if(exception == null)	{
							
							log.info("Analisi metadata: ");
							log.info("Topic: " + metadata.topic());
							log.info("Partition: " + metadata.partition());
							log.info("Offset: " + metadata.offset());
							log.info("Timestamp: " + metadata.timestamp());
							
						}	else	{
							
							log.error("Errore: ", exception);
						
						}
					}
				});
				
			}
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
			
		//dice al produttore di inviare tutti i dati e al termine di aspettare la chiusura -- modo sincrono
		producer.flush();
		
		producer.close();

	}

}
