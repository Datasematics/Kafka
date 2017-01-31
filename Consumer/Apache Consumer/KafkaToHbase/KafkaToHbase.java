import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.EnumCounters.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import kafka.consumer.Consumer;

public class KafkaToHbase {	  	
public static final String MASTER_IP = "localhost";
public static final String ZOOKEEPER_PORT = "2181";
public static void main(String[] args) throws IOException,SQLException,ClassNotFoundException {
	 Properties props = new Properties();
	 props.put("bootstrap.servers", "localhost:9092");
	 props.put("group.id", "mosquitoo");
	props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	 props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	 props.put("schema.registry.url", "http://localhost:8081"); 
	 props.put("auto.offset.reset", "earliest");
	 String topic = "hbas";
	 KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	consumer.subscribe(Collections.singletonList(topic));
	System.out.println("Reading topic:" + topic);
	 try{
	 while (true) {  
	ConsumerRecords<String, String> records = consumer.poll(10); 
	for (ConsumerRecord<String, String> record: records) {
		System.out.println("Reading topic:");
	         String val=record.value().toString();
	       //  String[] ddd=val.split(",");
           String id = val;	
	         System.out.println(val);
	         insert_into_hbase(id);
		 }   
	}
 } 
 catch(Exception e){
       e.printStackTrace();
  }finally {
       consumer.close();
    }
 }
 public static void insert_into_hbase(String id) throws ClassNotFoundException, SQLException, IOException
	 {	 
	      // Instantiating configuration class
      	Configuration con = HBaseConfiguration.create();
        con.set("hbase.zookeeper.quorum", MASTER_IP);
        con.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
      	// Instantiating HbaseAdmin class
     	 HBaseAdmin admin = new HBaseAdmin(con);
   	   // Instantiating table descriptor class
     	 HTableDescriptor tableDescriptor = new
      	HTableDescriptor(TableName.valueOf(id));
      	// Adding column families to table descriptor
     	 tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      	tableDescriptor.addFamily(new HColumnDescriptor("professional"));
      	// Execute the table through admin
      	admin.createTable(tableDescriptor);
    	 System.out.println(" Table created ");
	}	
	}
