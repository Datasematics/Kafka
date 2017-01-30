import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
public class Kafka_cassandra {
	private static Cluster cluster;
    	private static Session session;
	 public static void main(String[] args) throws IOException {
		 Properties props = new Properties();
	 props.put("bootstrap.servers", "localhost:9092");
	 props.put("group.id", "Mosquitoo1");
	 props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");  
	props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
	
	 props.put("schema.registry.url", "http://localhost:8081"); 
	 props.put("auto.offset.reset", "earliest");
	 String topic = "IOT_New";
	 KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	 consumer.subscribe(Collections.singletonList(topic));
	 System.out.println("Reading topic:" + topic);
	 while (true) { 	 
		 ConsumerRecords<String, String> records = consumer.poll(100); 
		for (ConsumerRecord<String, String> record: records) {		 
		JSONObject obj=new JSONObject(record.value());
	              JSONArray array=obj.getJSONArray("values");
	              Iterator it=array.iterator();
	              while(it.hasNext()){
	                     JSONObject obj1=(JSONObject)it.next();		 
	                     String timestamp= obj1.get("t").toString();
	                     String id= obj1.get("id").toString();
	                     String value= obj1.get("v").toString();
	                     System.out.println(id+" : "+value+" :"+timestamp);
		                   insert_into_cassandra(timestamp,id,value); 
	              }		
	 }    	 
	 }
	 }
	 
	 public static void insert_into_cassandra(String timestamp,String id,String value)
	 {	 
           long ts = Long.parseLong(timestamp);
           String query1 = "INSERT INTO IOT (timestamp, value, id)"
                            + " VALUES("+ ts +","+ id +","+ value +");" ;                
	         Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	         Session session = cluster.connect("myks");
	         session.execute(query1);	           
	         System.out.println("Data created");
	 }
}
