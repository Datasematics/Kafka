import java.sql.*;
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
import kafka.consumer.Consumer;
public class KafkaToMysql {
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://localhost/database_name";
	   static final String USER = "root";
	   static final String PASS = "cloudera";
 public static void main(String[] args) throws IOException,SQLException,ClassNotFoundException {
	Properties props = new Properties();
	 props.put("bootstrap.servers", "localhost:9092");
	 props.put("group.id", "mysqll");
	props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");	 
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	 props.put("schema.registry.url", "http://localhost:8081"); 
	 props.put("auto.offset.reset", "earliest");
	 String topic = "kh1";	 
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
	    			  insert_into_mysql(id);
	     			 }   
			}
 }		 		 
	catch(Exception e){
             e.printStackTrace();
    	 }finally {
            	consumer.close();
   		 }
	 }
	 
	public static void insert_into_mysql(String id) throws ClassNotFoundException, SQLException
	 {
		 Connection conn = null;
		 Statement stmt = null;
		 Class.forName("com.mysql.jdbc.Driver");
		 System.out.println("Connecting to a selected database...");
		 conn = DriverManager.getConnection(DB_URL, USER, PASS);
		 System.out.println("Connected database successfully...");
		 System.out.println("Creating statement...");
		 stmt = conn.createStatement();
		 String sql = "INSERT INTO table_name (id) VALUE ('"+id+"')";		      
         stmt.executeUpdate(sql);
	 }
}
