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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AvroConsumerHbase {
public static void main(String[] args) throws IOException {
Properties props = new Properties(); 
props.put("bootstrap.servers", "localhost:9092");    //kafka broker IP you are consumer from
props.put("group.id", "CountryCounter");	     // by default group.id is “group_id”
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); 
props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");  
props.put("schema.registry.url", "http://localhost:8081");   // schema registry for Avro file
String topic = "topic_name";
KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props); 
consumer.subscribe(Collections.singletonList(topic));
System.out.println("Reading topic:" + topic);
while (true) {
ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
for (ConsumerRecord<String, GenericRecord> record: records) {
Integer id=Integer.parseInt( record.value().get(0).toString());
String name=record.value().get(1).toString();
String email=record.value().get(2).toString();

inset_to_Hbase(id, name, email);	
System.out.println(id);
}
consumer.commitSync();
}
}
public static  void inset_to_Hbase(int  id,String name,String email) throws IOException{
byte[] COL_FAMILY=Bytes.toBytes("cf1");
byte[] ID_COL=Bytes.toBytes("Id");
byte[] EMAIL_COL=Bytes.toBytes("Email");

Configuration conf=HBaseConfiguration.create();
HTable table=new HTable(conf, Bytes.toBytes("Avro_hbase")); 
Put put=new  Put(Bytes.toBytes(name));
put.add(COL_FAMILY, ID_COL, Bytes.toBytes(id));
put.add(COL_FAMILY, EMAIL_COL, Bytes.toBytes(email));
table.put(put);
System.out.println("record inserted");
//table.close();

}
}
