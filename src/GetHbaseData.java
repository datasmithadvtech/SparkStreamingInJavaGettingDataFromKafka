import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;


/**
 * This Code is responsible to get data from HBase from a Track_Trajectory table 
 * using Scan clas in between some time range.
 * @author P.k.OJHA
 * 
 *
 */
public class GetHbaseData {
	public static void main(String[] args) throws IOException, ServiceException, InterruptedException {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "Ip address");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		HBaseAdmin.checkHBaseAvailable(configuration);
		System.out.println("hbaseis running");
		@SuppressWarnings("deprecation")
		HTable table = new HTable(configuration, "tableName");
	   // Get get =new Get(Bytes.toBytes("Key"));
	    
		Scan scan=new Scan();
		scan.setMaxVersions();
		scan.setTimeRange(1434978329000l, 1434978330000l);
		
		ResultScanner re=table.getScanner(scan);
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 
		 
		for (Result res : re) {
				
			 NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> allVersions=res.getMap();
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
			re.close();

	}

}
