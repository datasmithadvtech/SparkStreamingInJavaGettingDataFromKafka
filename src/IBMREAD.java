import java.text.SimpleDateFormat;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.optimizer.stats.annotation.StatsRulesProcFactory.TableScanStatsRule;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.deploy.history.config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.jruby.RubyProcess.Sys;

import scala.Tuple2;

/**
 * This project is not working properly 
 * @author P.K.OJHA
 *
 */
public class IBMREAD {
	private static final String Stringdata = "";

	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		// define Spark Context
		SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		Configuration config = null;
		try {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "ipaddress");
			config.set("hbase.zookeeper.property.clientPort", "2181");
			HBaseAdmin.checkHBaseAvailable(config);
			System.out.println("Hbase .............is...........running");

		} catch (MasterNotRunningException e) {
			System.out.println("--------------------HBase is not running!");
			System.exit(1);
		} catch (Exception ce) {
			ce.printStackTrace();
		}

		System.out.println("--------------------SET TABLES!");
		config.set(TableInputFormat.INPUT_TABLE, "test");
		config.set(TableInputFormat.SCAN_MAXVERSIONS, Stringdata);

		System.out.println("--------------------Creating hbase rdd!");

		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(config, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		hBaseRDD.foreach(prabhakar -> {

			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersions = prabhakar._2()
					.getMap();
			for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : allVersions.entrySet()) {
				System.out.println(new String(entry.getKey()));
				for (Entry<byte[], NavigableMap<Long, byte[]>> entry1 : entry.getValue().entrySet()) {

					for (Entry<Long, byte[]> entry2 : entry1.getValue().entrySet()) {
						System.out.println(new String(entry1.getKey()) + "  " + sdf.format(entry2.getKey()) + "->"
								+ new String(entry2.getValue()));

					}

				}
			}

		});

	}
}
