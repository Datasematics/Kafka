import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.EnumCounters.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class AvroProducer {
public static void main(String[] args) {
	String topic="samp";
	Properties props = new Properties();
	//bootstrap.servers is the host and port to our Kafka server
	props.put("bootstrap.servers", "localhost:9092");
	//avro serializer for apache kafka
	//key.serializer is the name of the class to serialize the key of the messages (messages have a key and value, but even though the key is optional, a serializer needs to be provided)
	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	//the value was defined as being a String, we will now use byte[]. Also, we need to switch from the StringSerializer to the ByteArraySerializer (this change only applies to the value, we can keep the StringSerializer for the key).
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	// props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	//Defining a schema
	String schemaString = "{\"namespace\": \"customerManagement.avro\","
				+ "\"type\": \"record\", " 
				+ "\"name\": \"Customer\"," 
				+ "\"fields\": [" 
				+  "{\"name\": \"id\", \"type\": \"int\"},"
				+  "{\"name\": \"name\", \"type\": \"string\"}," 
		+  "{\"name\": \"email\", \"type\": [\"null\",\"string\"], \"default\":\"null\" }" + 
		"]}";   
	Producer<String, byte[]> producer = new KafkaProducer<>(props);   
//schema is instantiated
	Schema.Parser parser = new Schema.Parser();    
	Schema schema = parser.parse(schemaString);	
	Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		for (int nCustomers = 0; nCustomers < 500; nCustomers++) { 
			String name = "exampleCustomer" + nCustomers;  
			String email = "example " + nCustomers + "@example.com" ;
			GenericRecord customer = new GenericData.Record(schema); 
			customer.put("id", nCustomers);      
			customer.put("name", name); 
			customer.put("email", email);   
			byte[] bytes = recordInjection.apply(customer);			
//Then comes the time to send messages. Messages are of type ProducerRecord with generic parameters (type of the key and type of the value). We specify the name of the topic and the value, thus omitting the key. The message is then sent using the send method of the KafkaProducer
		ProducerRecord<String, byte[]> data =new ProducerRecord<>(topic, bytes);  
		producer.send(data);   
		System.out.println("Sending data");
		 }
	}
}
