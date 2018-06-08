package tony.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;


/**
 * 利用coprocesser维护二级索引
 */

public class InverIndexCoprocessor extends BaseRegionObserver {

	static Table table = null;
	static {
		Configuration conf = HBaseConfiguration.create();

		Connection connection ;
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("guanzhu"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

		byte[] row = put.getRow();
		Cell cell = put.get("f1".getBytes(), "from".getBytes()).get(0);
		Put putIndex = new
				Put(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
		putIndex.addColumn("f1".getBytes(), "from".getBytes(), row);
		table.put(putIndex);
		table.close();

	}
}