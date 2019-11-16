package spark_s;

import java.io.IOException;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Serializable;
import scala.Tuple2;

/**Java_spark_hbase this class is responsible to take or read data from hbase.
 * as a client.
 * but it is not working properly
 * @ author prabhakar kumar ojha
 */
public class Java_Spark_hbase {
	
	/*
	 * app name and jsc initializing here
	 */
	private static String appName = "Hbase";
	private static JavaSparkContext jsc;

	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		SparkConf sparkConf = new SparkConf();
	    sparkConf.setMaster("local[10]").setAppName(appName);
	
		jsc = new JavaSparkContext(sparkConf);
		
		System.out.println("****************************Hbase Details*********************************");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "ipAddress");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		System.out.println("hbase is running");
		System.out.println("****************************Hbase Details*********************************");
		
		Scan scan = new Scan().setMaxVersions();
		scan.addFamily(Bytes.toBytes("Dynamic Data"));
		scan.addColumn(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("p"));
		scan.addColumn(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("c"));
		scan.addColumn(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("s"));

		String scanToString = "";
		try {
			ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
			scanToString = Base64.encodeBytes(proto.toByteArray());
		} catch (IOException io) {
			System.out.println(io);
		}
		
		
		String tableName = "test";
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set(TableInputFormat.SCAN, scanToString);
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(conf,TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
		   
		    hBaseRDD.foreach(prabhakar->{
			 NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> allVersions=prabhakar._2().getMap();
			 for(Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> entry:allVersions.entrySet())
			 {
			   System.out.println(new String(entry.getKey())); 
				for(Entry<byte[],NavigableMap<Long,byte[]>> entry1:entry.getValue().entrySet())
				{
					int i=0;
					for(Entry<Long,byte[]> entry2:entry1.getValue().entrySet())
					{
					System.out.println(new String(entry1.getKey())+"  "+sdf.format(entry2.getKey())+"->"+new String(entry2.getValue()));
						i++;
					}
					 System.out.println(i);
				}
			 }
		});
	}
}
