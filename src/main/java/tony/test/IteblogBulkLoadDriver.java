package tony.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 预分区配合BulkLoad进行数据导入
 * reduce数量取决于Region数量
 * 数据rowKey按字典序排序
 */
public class IteblogBulkLoadDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final String SRC_PATH= "hdfs://cdh120:8020/testHbase";
        final String DESC_PATH= "hdfs://cdh120:8020/testOut";
        Configuration conf = HBaseConfiguration.create();

        Job job= Job.getInstance(conf);
        job.setJarByClass(IteblogBulkLoadDriver.class);
        job.setMapperClass(IteblogBulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        Connection connect = ConnectionFactory.createConnection(conf);

        Table table = connect.getTable(TableName.valueOf("user_info"));

        HFileOutputFormat2.configureIncrementalLoad(job,table,connect.getRegionLocator(TableName.valueOf("user_info")));

        FileInputFormat.addInputPath(job,new Path(SRC_PATH));
        FileOutputFormat.setOutputPath(job,new Path(DESC_PATH));
          
        System.exit(job.waitForCompletion(true)?0:1);
    }
}