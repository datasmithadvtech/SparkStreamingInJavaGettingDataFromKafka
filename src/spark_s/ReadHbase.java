package spark_s;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class ReadHbase {

	private static String appName = "Hbase";
	private static JavaSparkContext jsc;

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException {

//		Logger.getLogger("org").setLevel(Level.OFF);
//		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkConf sparkConf = new SparkConf();

		sparkConf.setMaster("local[*]").setAppName(appName).set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.driver.memory","4g");
		sparkConf.set("spark.executor.memory","100g");
		jsc = new JavaSparkContext(sparkConf);

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.11.130");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		System.out.println("hbase is running");
		System.out.println("****************************Hbase Details*********************************");

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("Dynamic Data"));
		scan.addColumn(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("s"));
		scan.addColumn(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("p"));
		scan.addColumn(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("c"));
		System.gc();
		String scanToString = "";
		try {
			ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
			scanToString = Base64.encodeBytes(proto.toByteArray());
		} catch (IOException io) {
			System.out.println(io);
		}
		for (int i = 0; i < 2; i++) {
			try {
				String tableName = "Grid_Movement_Analysis";
				conf.set(TableInputFormat.INPUT_TABLE, tableName);
				conf.set(TableInputFormat.SCAN, scanToString);

				JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class,
						ImmutableBytesWritable.class, Result.class);

				JavaPairRDD<String, List<String>> hBaseRDD1 =hBaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, List<String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, List<String>> call(Tuple2<ImmutableBytesWritable, Result> results) {

						List<String> list = new ArrayList<String>();
						byte[] spee = results._2().getValue(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("speed"));
						byte[] po = results._2().getValue(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("Position"));
						byte[] co = results._2().getValue(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("course"));
						list.add(String.valueOf(Bytes.toString(spee)));
						list.add(String.valueOf(Bytes.toString(po)));
						list.add(String.valueOf(Bytes.toString(co)));
						return new Tuple2<String, List<String>>(Bytes.toString(results._1().get()), list);
					}
				});

				List<Tuple2<String, List<String>>> cart = hBaseRDD1.collect();
				for (int i1 = 0; i1 < cart.size(); i1++) {

					Tuple2<String, List<String>> t2 = cart.get(i1);

					Iterator<Cell> it = ((Result) t2._2()).listCells().iterator();

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


			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}

}