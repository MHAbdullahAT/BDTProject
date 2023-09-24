package cs523.Project;




import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;



public class CSVKafkaProducer {

	private static String KafkaBrokerEndpoint = "localhost:9092";
    private static String KafkaTopic = "demo";
    private static String CsvFile = "test.csv";
    
  
    private Producer<String, String> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws URISyntaxException {
        CSVKafkaProducer kafkaProducer = new CSVKafkaProducer();
        kafkaProducer.PublishMessages();
        System.out.println("Producing job completed");
    }

    
    
    private void PublishMessages() throws URISyntaxException{
    	
        final Producer<String, String> csvProducer = ProducerProperties();
        
        try{
        	URI uri = getClass().getClassLoader().getResource(CsvFile).toURI();
            Stream<String> FileStream = Files.lines(Paths.get(uri));
            
            final int[] lineCounter = {0};
            FileStream.forEach(line -> {
            	System.out.println(line);
            	
                final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
                        KafkaTopic, UUID.randomUUID().toString(), line);

                csvProducer.send(csvRecord, (metadata, exception) -> {
                    if(metadata != null){
                        System.out.println("Sent: "+ csvRecord.key()+" | "+ csvRecord.value());
                    }
                    else{
                        System.out.println("Error sending: "+ csvRecord.value());
                    }
                });
                
                lineCounter[0]++;

                if (lineCounter[0] % 10 == 0) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
