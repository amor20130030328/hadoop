package com.gy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        //1.获取文件系统
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"amor");

        //创建目录
        fileSystem.mkdirs(new Path("/hadoop/sanguo/suguo"));

        fileSystem.close();

    }

    @Test
    public void uploadTest() throws Exception {
        //1.获取文件系统
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication","2");

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "amor");

        //上传文件
        fileSystem.copyFromLocalFile(new Path("D:\\source\\hadoop_test\\src\\main\\resources\\log4j.properties"),new Path("/hadoop/sanguo/suguo"));
        fileSystem.close();
    }

    @Test
    public void testDownload() throws Exception{
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "amor");

        fileSystem.copyToLocalFile(new Path("/hadoop/sanguo/suguo"),new Path("D:\\source\\hadoop_test\\src\\main\\temp\\"));
        fileSystem.close();
    }

    @Test
    public void testDelete() throws  Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "amor");

        fileSystem.delete(new Path("/hadoop/sanguo/suguo/log4j.properties"),true);
        fileSystem.close();

    }

    @Test
    public void testReName() throws  Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "amor");

        fileSystem.rename(new Path("/hadoop/sanguo/suguo/log4j.properties"),new Path("/hadoop/sanguo/suguo/log.properties"));
        fileSystem.close();
    }
}
