package tony.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * mr生成Hfile之后，将文件导入Hbase即可
 */
public class LoadIncrementalHFileToHBase {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        LoadIncrementalHFiles loder = new LoadIncrementalHFiles(conf);
        loder.doBulkLoad(new Path("hdfs://cdh120:8020/testOut"),new HTable(conf,"user_info"));
    }
}