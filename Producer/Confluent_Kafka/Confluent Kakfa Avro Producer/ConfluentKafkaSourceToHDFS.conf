// import all requires packages for kafka producer , avro and java util.
import java.util.Properties;
port org.apache.avro.Schema;import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AvroProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String topic="topic_Name";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");	 //kafka broker ID
		props.put("schema.registry.url","http://localhost:8081"); // location of avro schema file
		props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
	
		String schemaString = "{\"namespace\": \"customerManagement.avro\","
		+ "\"type\": \"record\", " 
		+ "\"name\": \"Customer\"," 
		+ "\"fields\": [" 
		+  "{\"name\": \"id\", \"type\": \"int\"},"
		+  "{\"name\": \"name\", \"type\": \"string\"}," 
		+  "{\"name\": \"email\", \"type\": [\"null\",\"string\"], \"default\":\"null\" }" + 
		"]}";
   
/*A producer is instantiated by providing a set of key-value pairs as configuration.. Values can be either strings or Objects of the appropriate type
*/
		Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);     
		Schema.Parser parser = new Schema.Parser();    
		Schema schema = parser.parse(schemaString);    
		for (int nCustomers = 0; nCustomers < 500; nCustomers++) { 
			String name = "exampleCustomer" + nCustomers;  
			String email = "example " + nCustomers + "@example.com" ;
			GenericRecord customer = new GenericData.Record(schema); 
			
			customer.put("id", nCustomers);      
			customer.put("name", name); 
			customer.put("email", email);   
			ProducerRecord<String, GenericRecord> data =new ProducerRecord<String, GenericRecord>(topic, name, customer);  
			producer.send(data);   
			System.out.println("Sending data");
			  }
	}

}
