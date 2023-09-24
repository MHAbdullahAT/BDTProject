package cs523.Project;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import kafka.serializer.StringDecoder;

class News{
	public News(int id, String date, String headline, String category){
		this.id=id;
		this.date = date;
		this.headline = headline;
		this.category = category;
	}
	public int id;
	public String date;
	public String headline;
	public String category;
}

public class SparkKafkaConsumer {
    private static final String TABLE_NAME = "news";
    private static final String CF_DEFAULT = "nw";
    
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("sparkkafkaproject").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(10000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("demo");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		  
		final int[] id = {1};

	        // Define keywords to filter for
	        final String[] keywords = {"hacking", "breach", "hacked", "security", "cybersecurity", "vulnerability"};

	        // Establish a connection to HBase
	        Configuration hbaseConfig = HBaseConfiguration.create();
	        Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
	        Admin admin = hbaseConnection.getAdmin();

	        recreateTable(admin);
	        
		  directKafkaStream.foreachRDD(rdd -> {

		  System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
			  if(rdd.count() > 0) {
				  List<News> toAddData = new ArrayList<News>();
				rdd.collect().forEach(rawRecord -> {
					  String record = rawRecord._2();
					  StringTokenizer st = new StringTokenizer(record,",");
					  
					  StringBuilder sb = new StringBuilder(); 
					  while(st.hasMoreTokens()) {
						String date = st.nextToken();
						String headline = st.nextToken();
						String category = st.nextToken();
						for (String keyword : keywords) {
			                if (headline.contains(keyword)) {
			                	sendCurlRequest(headline);
			                	break;
			                }
			            }
						sb.append(date).append(headline).append(category);
						System.out.println(sb.toString()); 
						toAddData.add(new News(id[0]++,date,headline,category));
					  }
				  });
				try {
					insertData(hbaseConnection,toAddData);
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println("All records OUTER MOST :"+toAddData.size()); 
			  }
		  });
		 
		ssc.start();
		ssc.awaitTermination();
	}
	
    private static void sendCurlRequest(String content) {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("curl", "-d", content, "projectbdt.ddns.net:9090/BDT");
            Process process = processBuilder.start();
            
            // Read the cURL command's output
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                System.err.println("cURL request failed with exit code: " + exitCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void recreateTable(Admin admin) throws Exception {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));

        System.out.println("reCreating table.... ");

        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
    
    private static void insertData(Connection connection, List<News> data) throws Exception {
        Table tbl = connection.getTable(TableName.valueOf(TABLE_NAME));

        for (int i = 0; i < data.size(); i++) {
            News news = data.get(i);
            Put put1 = createPut(news.id, news.date, news.headline, news.category);
            tbl.put(put1);
        }

        tbl.close();
    }
    
    private static Put createPut(int id, String date, String headline, String category) {
        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("Date"), Bytes.toBytes(date));
        put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("Headline"), Bytes.toBytes(headline));
        put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("Category"), Bytes.toBytes(category));
        return put;
    }
}



