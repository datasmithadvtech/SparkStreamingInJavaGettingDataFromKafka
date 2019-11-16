package spark_s;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;



import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * This project is responsible for getting filter data from HBase table using GET class
 * @author P. K. OJHA
 *
 */
public class Hbase_demoData {
	public static void main(String[] args) throws IOException, Exception{
		
		Configuration config = HBaseConfiguration.create();
		
		config.set("hbase.zookeeper.quorum","ipAddress");
		config.set("hbase.zookeeper.property.clientPort","2181");

		HTable table = new HTable(config, "Tablename");


		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		 Get q= new Get(Bytes.toBytes("key"));
		 q.setMaxVersions();
//SingleColumnValueFilter sc=new SingleColumnValueFilter(Bytes.toBytes("Dynamic Data"), Bytes.toBytes("speed"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("0.1")));
	//sc.setFilterIfMissing(true);
		 
	//q.setFilter(sc);

		 Result row= table.get(q);
                 
		
		 NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> allVersions=row.getMap();
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
				 System.out.println("All Number of Row: "+i);
			}
		 }
		
		

	}
	
}
    

