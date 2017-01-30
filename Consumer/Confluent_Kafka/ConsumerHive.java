    //import all necessary packages required of this project 
    import java.io.IOException;
    import java.util.Arrays;
    import java.util.Collections;
    import java.util.Properties;
    import org.apache.avro.Schema;
    import org.apache.avro.generic.GenericRecord;
    import org.apache.avro.io.BinaryDecoder;
    import org.apache.avro.io.DatumReader;
    import org.apache.avro.io.DecoderFactory;
    import org.apache.avro.specific.SpecificDatumReader;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.kafka.clients.consumer.ConsumerRecord;
    import org.apache.kafka.clients.consumer.ConsumerRecords;
    import org.apache.kafka.clients.consumer.KafkaConsumer;
    import java.sql.SQLException;
    import java.sql.Connection;
    import java.sql.ResultSet;
    import java.sql.Statement;
    import java.sql.DriverManager;
    
     public class ConsumerHive {
		
	 //Initialize JDBC driver for hive 
		private static  String driverName = "org.apache.hive.jdbc.HiveDriver";
	

		 public static void main(String[] args) throws IOException,SQLException,ClassNotFoundException {	
		
//configure kakfa properties
		 Properties props = new Properties();
                 props.put("bootstrap.servers", "localhost:9092");
                // kakfa group id this case must be changed everytime when you execute this program 
props.put("group.id", "group_id");
                 props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                 props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");

//Avro schema registry  url location                
 props.put("schema.registry.url", "http://localhost:8081");
                 props.put("auto.offset.reset", "earliest");
                 String topic = "ronaldo";
                 KafkaConsumer<String,GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
                 consumer.subscribe(Collections.singletonList(topic));
                 System.out.println("Reading topic:" + topic);
                
		 while (true) {

                         ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                         for (ConsumerRecord<String, GenericRecord> record: records) {

// parsing all the values to string type 
                                 Integer id =Integer.parseInt( record.value().get(0).toString());
				 String name=record.value().get(1).toString();
                                 String email=record.value().get(2).toString();
				 System.out.println("hello");
				 
				 String path = "'" + id + "'" + "," + "'" + name + "'" + "," + "'" + email + "'";
                                 System.out.println(path);
	
//call a function to insert into Hive function 			 
insert_to_Hive(path);

			}
				consumer.commitSync();
  		}
		
}

	public static void insert_to_Hive (String path)  throws ClassNotFoundException,SQLException
	{	

		 try {
                        Class.forName(driverName);
                        } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
			System.out.println("catching exception");
                        System.exit(1);
                }
		
// configure hive connection 
     Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
		
		//first creates table in hive and insert into hive table
		Statement stmt = con.createStatement();
    		String tableName = "test_table";
    		stmt.execute("drop table if exists " + tableName);
    		stmt.execute("create table " + tableName + " (key int, value string)");
    		// show tables
    		// String sql = "show tables '" + tableName + "'";
    		String sql = ("show tables");
    		ResultSet res = stmt.executeQuery(sql);
    		if (res.next()) {
       		 System.out.println(res.getString(1));
      		

		Statement stmt = con.createStatement();
                String tableName = "hive_table1";
                String sql = " INSERT INTO TABLE hive_table1 values" + "(" + path + ")";
		//String sql = "load data local inpath '" + path + "'  into table " + tableName;
                System.out.println("Running" + sql);
                stmt.execute(sql);
                System.out.println("inserted into Hive");
}	}
		
  
