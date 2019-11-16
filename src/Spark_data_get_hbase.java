import java.io.IOException;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.log4j.Level;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.SQLContext;


import scala.Tuple2;



/**
 * this is working properly but taking more time to start
 * @author P.K.OJHA
 *
 */
public class Spark_data_get_hbase implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws IOException {
		// log off
//		Logger.getLogger("org").setLevel(Level.OFF);
//		Logger.getLogger("akka").setLevel(Level.OFF);

		// Define spark context
		SparkConf conf = new SparkConf().setAppName("getdatafromhbase").setMaster("local[*]").set("spark.executor.memory","15g");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		@SuppressWarnings("deprecation")
		SQLContext sc = new SQLContext(jsc);
		sc.clearCache();
System.gc();
		// create connection with HBase
		Configuration config = null;

		try {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "ip address");
			config.set("hbase.zookeeper.property.clientPort", "2181");
			HBaseAdmin.checkHBaseAvailable(config);
			System.out.println("Hbase .............is...........running");
		} catch (MasterNotRunningException e) {

			System.out.println("HBase is not running!");
			e.printStackTrace();
			System.exit(1);

		} catch (Exception ce) {

			ce.printStackTrace();
		}

		@SuppressWarnings("deprecation")
		Scan scan = new org.apache.hadoop.hbase.client.Scan();

		//scan.addFamily(Bytes.toBytes("Dynamic Data"));

		org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan proto = ProtobufUtil.toScan(scan);

		String scanToString = Base64.encodeBytes(proto.toByteArray());
		config.set(TableInputFormat.INPUT_TABLE, "tableName");
		config.set(TableInputFormat.SCAN, scanToString);
		JavaPairRDD<ImmutableBytesWritable, Result> rdd = jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class,
				Result.class);
		List<Tuple2<ImmutableBytesWritable, Result>> rddList = rdd.collect();
		for (int i = 0; i < rddList.size(); i++) {

			Tuple2<ImmutableBytesWritable, Result> t2 = rddList.get(i);

			Iterator<Cell> it = t2._2().listCells().iterator();

			while (it.hasNext()) {

				Cell c = it.next();

				String family = Bytes.toString(CellUtil.cloneFamily(c));

				String qualifier = Bytes.toString(CellUtil.cloneQualifier(c));

				String value = Bytes.toString(CellUtil.cloneValue(c));

				Long tm = c.getTimestamp();

				System.out.println(
						" Family=" + family + " Qualifier=" + qualifier + " Value=" + value + " TimeStamp=" + tm);
			}

		}

		jsc.stop();

	}

}
