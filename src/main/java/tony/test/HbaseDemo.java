package tony.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HbaseDemo {

	/* create config and connection. */
	static Configuration conf = HBaseConfiguration.create();
	static Connection connect = null;
	static {
		try {
			connect = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/* test create table. */
	public static void createTable(String tableName, String[] family)
			throws Exception {
		Admin admin = connect.getAdmin();

		TableName tn = TableName.valueOf(tableName);
		HTableDescriptor desc = new HTableDescriptor(tn);
		for (int i = 0; i < family.length; i++) {
			desc.addFamily(new HColumnDescriptor(family[i]));
		}

		if (admin.tableExists(tn)) {
			System.out.println("table Exists!");
			System.exit(0);
		} else {
			admin.createTable(desc);
			System.out.println("create table Success!");
		}
	}

	/* put data into table. */
	public static void addData(String rowKey, String tableName,
							   String[] column1, String[] value1, String[] column2, String[] value2)
			throws IOException {
        /* get table. */
		TableName tn = TableName.valueOf(tableName);
		Table table = connect.getTable(tn);

        /* create put. */
		Put put = new Put(Bytes.toBytes(rowKey));
		HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();

		for (int i = 0; i < columnFamilies.length; i++) {
			String f = columnFamilies[i].getNameAsString();
			if (f.equals("article")) {
				for (int j = 0; j < column1.length; j++) {
					put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
				}
			}
			if (f.equals("author")) {
				for (int j = 0; j < column2.length; j++) {
					put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
				}
			}
		}

        /* put data. */
		table.put(put);
		System.out.println("add data Success!");
	}

	/* get data. */
	public static void getResult(String tableName, String rowKey)
			throws IOException {
        /* get table. */
		Table table = connect.getTable(TableName.valueOf(tableName));

		Get get = new Get(Bytes.toBytes(rowKey));

		Result result = table.get(get);
		for (Cell cell : result.listCells()) {
			System.out.println("------------------------------------");
			System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
			System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
			System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
			System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
			System.out.println("timest: " + cell.getTimestamp());
		}
	}

	/* scan table. */
	public static void getResultScan(String tableName) throws IOException {
		Scan scan = new Scan();
		ResultScanner rs = null;
		Table table = connect.getTable(TableName.valueOf(tableName));
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				for (Cell cell : r.listCells()) {
					System.out.println("------------------------------------");
					System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
					System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
					System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
					System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
					System.out.println("timest: " + cell.getTimestamp());
				}
			}
		} finally {
			rs.close();
		}
	}

	/* range scan table. */
	public static void getResultScan(String tableName, String start_rowkey,
									 String stop_rowkey) throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(start_rowkey));
		scan.setStopRow(Bytes.toBytes(stop_rowkey));
		ResultScanner rs = null;
		Table table = connect.getTable(TableName.valueOf(tableName));
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				for (Cell cell : r.listCells()) {
					System.out.println("------------------------------------");
					System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
					System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
					System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
					System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
					System.out.println("timest: " + cell.getTimestamp());
				}
			}
		} finally {
			rs.close();
		}
	}

	/* get column data. */
	public static void getResultByColumn(String tableName, String rowKey,
										 String familyName, String columnName) throws IOException {
        /* get table. */
		Table table = connect.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
		Result result = table.get(get);

		for (Cell cell : result.listCells()) {
			System.out.println("------------------------------------");
			System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
			System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
			System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
			System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
			System.out.println("timest: " + cell.getTimestamp());
		}
	}

	/* update. */
	public static void updateTable(String tableName, String rowKey,
								   String familyName, String columnName, String value)
			throws IOException {
		Table table = connect.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
				Bytes.toBytes(value));
		table.put(put);
		System.out.println("update table Success!");
	}

	/* get multi-version data. */
	public static void getResultByVersion(String tableName, String rowKey,
										  String familyName, String columnName) throws IOException {
		Table table = connect.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		get.setMaxVersions(5);
		Result result = table.get(get);
		for (Cell cell : result.listCells()) {
			System.out.println("------------------------------------");
			System.out.println("timest: " + cell.getSequenceId());
			System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
			System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
			System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
			System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
			System.out.println("timest: " + cell.getTimestamp());
		}
	}

	/* delete column. */
	public static void deleteColumn(String tableName, String rowKey,
									String falilyName, String columnName) throws IOException {
		Table table = connect.getTable(TableName.valueOf(tableName));
		Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
		deleteColumn.addColumns(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
		table.delete(deleteColumn);
		System.out.println(falilyName + ":" + columnName + "is deleted!");
	}

	/* delete row. */
	public static void deleteAllColumn(String tableName, String rowKey)
			throws IOException {
		Table table = connect.getTable(TableName.valueOf(tableName));
		Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
		table.delete(deleteAll);
		System.out.println("all columns are deleted!");
	}

	/* delete table. */
	public static void deleteTable(String tableName) throws IOException {
		Admin admin = connect.getAdmin();
		admin.disableTable(TableName.valueOf(tableName));
		admin.deleteTable(TableName.valueOf(tableName));
		System.out.println(tableName + "is deleted!");
	}

	public static void  main (String [] agrs) throws Exception {

		try {

			//String tableName = "blog2";
			//String[] family = { "article", "author" };
			//createTable(tableName, family);


			//String[] column1 = { "title", "content", "tag" };
			//String[] value1 = {
					//"Head First HBase",
					//"HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
					//"Hadoop,HBase,NoSQL" };
			//String[] column2 = { "name", "nickname" };
			//String[] value2 = { "nicholas", "lee" };
			//addData("rowkey1", tableName, column1, value1, column2, value2);
			//addData("rowkey2", tableName, column1, value1, column2, value2);
			//addData("rowkey3", tableName, column1, value1, column2, value2);


			//getResultScan(tableName, "rowkey4", "rowkey5");


			//getResultScan(tableName, "rowkey4", "rowkey5");

            /* get data. */
			getResult("user_info", "1");

			//getResultByColumn(tableName, "rowkey1", family[1], "name");

			//updateTable(tableName, "rowkey1", family[1], "name", "bin");

			//getResultByColumn(tableName, "rowkey1", family[1], "name");

			//getResultByVersion(tableName, "rowkey1", family[1], "name");

			//deleteColumn(tableName, "rowkey1", family[1], "nickname");

			//deleteAllColumn(tableName, "rowkey1");

			//deleteTable(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}