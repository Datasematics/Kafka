import java.sql.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.EnumCounters.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import kafka.consumer.Consumer;

public class KafkaToHive {	  
private static String driverName = "org.apache.hive.jdbc.HiveDriver";
public static void main(String[] args) throws IOException,SQLException,ClassNotFoundException {	 Properties props = new Properties();
	 props.put("bootstrap.servers", "localhost:9092");
	 props.put("group.id", "mosquitoo");
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
	     String id = val;	
	     System.out.println(val);
	     insert_into_hive(id);
			 }   
		     }
		 } 
		 catch(Exception e){
             			e.printStackTrace();
     		}finally {
            		consumer.close();
    	}
 }
public static void insert_into_hive(String id) throws ClassNotFoundException, SQLException
 {		 
	 try {
                          Class.forName(driverName);
                    } catch (ClassNotFoundException e) {
                             e.printStackTrace();
                         	System.exit(1);
                        }  
Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
Statement stmt = con.createStatement();
 stmt.execute("drop table if exists " + id);
 stmt.execute("create table " + id + " (key int, value string)");
 String sql = ("show tables");
 ResultSet res = stmt.executeQuery(sql);
 if (res.next()) {
        	System.out.println(res.getString(1));
	 }
}
}
