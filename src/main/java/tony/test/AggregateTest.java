package tony.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * 通过coprocessor进行聚合函数运算
 * Created by tony on 2018/6/7.
 */
public class AggregateTest {
    public static void main(String[] args) throws Throwable {

        Configuration conf = HBaseConfiguration.create();

        /*String coprocessClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";

        Connection connect = ConnectionFactory.createConnection(conf);

        Admin admin = connect.getAdmin();

        TableName tableName = TableName.valueOf("user_info");

        admin.disableTable(tableName);

        HTableDescriptor htd = admin.getTableDescriptor(tableName);

        htd.addCoprocessor(coprocessClassName);

        admin.modifyTable(tableName, htd);

        admin.enableTable(tableName);*/ //添加coprocessor

        System.out.println(max("info","score",conf));
    }

    public static long rowCount(Configuration configuration) {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        //scan.setStopRow(Bytes.toBytes("1"));
        //scan.addFamily(Bytes.toBytes(family));
        scan.setFilter(new FirstKeyOnlyFilter());
        long rowCount = 0;
        try {
            rowCount = ac.rowCount(TableName.valueOf("user"), new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
        }
        return rowCount;
    }

    /** 求和*/
    public static long sum(String family, String qualifier,Configuration configuration) throws Throwable {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        long sum = ac.sum(TableName.valueOf("user"), new LongColumnInterpreter(), scan);//数据类型一定要与hbase中类型一致，否则会计算错误
        return sum; //通过shell放入的数据都为string类型
    }

    /** 求最大值*/
    public static double max(String family, String qualifier,Configuration configuration) throws Throwable {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        double sum = ac.max(TableName.valueOf("user"), new DoubleColumnInterpreter(), scan);//数据类型一定要与hbase中类型一致，否则会计算错误
        return sum;
    }

    /** 求最小值*/
    public static long min(String family, String qualifier,Configuration configuration) throws Throwable {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        long sum = ac.min(TableName.valueOf("user"), new LongColumnInterpreter(), scan);//数据类型一定要与hbase中类型一致，否则会计算错误
        return sum;
    }

    /** 求平均值*/
    public static double avg(String family, String qualifier,Configuration configuration) {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        double avg = 0;
        try {
            avg = ac.avg(TableName.valueOf("user"), new DoubleColumnInterpreter(), scan);//数据类型一定要与hbase中类型一致，否则会计算错误
        } catch (Throwable e) {
        }
        return avg;
    }
}
