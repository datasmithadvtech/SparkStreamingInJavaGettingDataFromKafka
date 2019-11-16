
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.derby.impl.sql.catalog.SYSTABLEPERMSRowFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.in.encryption.LANEncryption;

import ch.hsr.geohash.GeoHash;
import io.fabric8.kubernetes.api.model.Time;
import scala.Tuple2;

/**
 * @version 1
 * @author Prabhakar kumar Ojha
 *
 */

public class SparkStreamingApp {

	public static JavaSparkContext sparkContext;

	public static void main(String[] args) throws InterruptedException, IOException {

//checking how much memory it's taking
		try {
			long si = Runtime.getRuntime().totalMemory();
			System.out.print(si + "this is taking");

// LOG WILL NOT COME

//			Logger.getLogger("org").setLevel(Level.OFF);
//			Logger.getLogger("akka").setLevel(Level.OFF);

//Connection with yml file

			InputStream input = new FileInputStream(new File("params.yml"));
			Yaml yaml = new Yaml(new Constructor(InputParams.class));
			InputParams config = (InputParams) yaml.load(input);

//Connection code of kafka

			Map<String, Object> kafkaParams = new HashMap<>();
			kafkaParams.put("bootstrap.servers", "ipAddress:9092");
			kafkaParams.put("key.deserializer", StringDeserializer.class);
			kafkaParams.put("value.deserializer", StringDeserializer.class);
			kafkaParams.put("group.id", "0");
			kafkaParams.put("auto.offset.reset", "earliest");
			kafkaParams.put("enable.auto.commit", false);
			kafkaParams.put("zookeeper.connect", "ip:2181");
			kafkaParams.put("metadata.broker.list", "ip:9092");

			Collection<String> topics = Arrays.asList("DpDataTopicName");

//Spark connection code

			SparkConf sparkConf = new SparkConf();
			sparkConf.setMaster("local[2]");
			sparkConf.setAppName("WordCountingAppWithCheckpoint");
			sparkConf.set("spark.Serializer", "org.apache.spark.Serializer.KryoSerializer");

//Streaming registration 

			JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

//Converting sparkStreaming in to sparkContext

			sparkContext = streamingContext.sparkContext();

			Broadcast<String> ROW_KEY_B = sparkContext.broadcast(config.getRowKey());
			Broadcast<ArrayList<HashMap<String, String>>> ROW_VALUES_B = sparkContext.broadcast(config.getRowValues());

			// streamingContext.checkpoint("./.checkpoint");

//Taking data from here which will come in 300 batch
			LANEncryption ln = new LANEncryption();

			JavaDStream<String> messages = KafkaUtils
					.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
							ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams))
					.mapToPair(record -> new Tuple2<>(record.key(), record.value())).map(tuple2 -> tuple2._2());
			
			byte[] Decryptmessages = ln.decrypt(Bytes.toBytes(messages.toString()));

//			JavaDStream<String> encry=	messages.map(new org.apache.spark.api.java.function.Function<String, String>() {
//
//				/**
//				 * 
//				 */
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public String call(String arg0) throws Exception {
//					byte[] Decryptmessages=ln.decrypt(Bytes.toBytes(messages.toString()));
//					return ln.decrypt(Bytes.toBytes(messages.toString())).toString();
//				}
//			});
//			System.out.println("yfvdsihvh fhysf hg ");

			/*****************************************************
			 * commented code
			 *************************************************************/
//1
			/**********************************************************************************************************************************************************/
			// 2 /** [CODE REMOVED FROM HARE] **/
			/**********************************************************************************************************************************************************/
//3
			/*****************************************************
			 * commented code
			 *************************************************************/

//Iterating all msg by chunk of 300

			messages.foreachRDD(new VoidFunction<JavaRDD<String>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(JavaRDD<String> data1) throws Exception {
					data1.foreach(new VoidFunction<String>() {
						private static final long serialVersionUID = 1L;
						private HTable ht;

						@Override
						public void call(String data) throws Exception {

							JsonArray json = new JsonParser().parse(data.toString()).getAsJsonArray();

//Inserting into Hbase

							insert(json);

						}

						@SuppressWarnings("deprecation")
						private void insert(JsonArray json) throws IOException {

							for (int i = 0; i < json.size(); i++) {
//Configuration code of Hbase
								Configuration configuration = null;
								try {
									configuration = HBaseConfiguration.create();

									configuration.set("hbase.zookeeper.quorum", config.getQuorum());
									configuration.set("hbase.zookeeper.property.clientPort", config.getPort());
									HBaseAdmin.checkHBaseAvailable(configuration);
									System.out.print("------------------HBase is running!------------------");
								} catch (Exception ce) {
									ce.printStackTrace();
								}

								ht = new HTable(configuration, "tablename");

								String key = "";
								SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//							Long l = new Long(json.get(i).getAsJsonObject().get("timeStamp").toString());
								Long l = System.currentTimeMillis();
								String ts1 = sdf.format(l);
								String geohash;
								try {

									geohash = GeoHash.geoHashStringWithCharacterPrecision(
											Double.valueOf(json.get(i).getAsJsonObject().get("latitude").toString()),
											Double.valueOf(json.get(i).getAsJsonObject().get("longitude").toString()),
											5);
								} catch (Exception e) {
									geohash = "exception";
								}
								try {
									key = key
											+ geohash + ":" + json.get(i).getAsJsonObject().get(ROW_KEY_B.value())
													.toString().substring(1, 10)
											+ ":" + ts1.substring(0, 4) + ":" + ts1.substring(5, 7);

								} catch (Exception e) {

								}
								System.out.println(key);

								Put put = new Put(Bytes.toBytes(key), l);

								for (HashMap<String, String> val : ROW_VALUES_B.value()) {
									String[] cq = val.get("qualifier").toString().split(":");
									try {
										if (cq[1].equals("Position")) {
											Tuple2<Double, Double> tup = new Tuple2<Double, Double>(
													Double.valueOf(
															json.get(i).getAsJsonObject().get("latitude").toString()),
													Double.valueOf(
															json.get(i).getAsJsonObject().get("longitude").toString()));
											put.add(Bytes.toBytes(cq[0]), Bytes.toBytes(cq[1]),
													Bytes.toBytes(tup.toString()));

										} else {

											put.add(Bytes.toBytes(cq[0]), Bytes.toBytes(cq[1]),
													Bytes.toBytes(json.get(i).getAsJsonObject().get(val.get("value"))
															.toString().substring(1, 10)));

										}
									} catch (Exception e) {

									}

								}
								if (put != null)
									continue;
								ht.put(put);
								ht.close();
							}
						}
					});
				}

			});

			System.out.println("Streaming will start Now...\n\n");
			streamingContext.start();
			streamingContext.awaitTermination();
		} catch (Exception e) {
		}
	}

}
