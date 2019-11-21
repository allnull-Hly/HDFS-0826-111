package com.inspur.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HdfsClient {

    private Configuration conf;
    private FileSystem fs;


    @Before
    public void first() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs:/hadoop102:9000"), conf, "me");

    }





    //测试FileSystem
//    public void testFileSystem

    //测试文件上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {

        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");

        //2.上传文件
        fs.copyFromLocalFile(new Path("E:/windows/aaa.txt"),new Path("/"));

        //3.关闭资源
        fs.close();
        System.out.println("程序执行完毕！！！");

    }


    //文件下载
    @Test
    public void testLoad() throws URISyntaxException, IOException, InterruptedException {

        //1.获取配置文件
        Configuration conf = new Configuration();
        //2.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");
        //下载文件
        fs.copyToLocalFile(false,new Path("/input"),new Path("e:/output"),false);

        //3.关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {

        //1.获取配置文件
        Configuration conf = new Configuration();
        //2.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");

        //3.删除文件
        boolean delete = fs.delete(new Path("/input/aaa.txt"), false);

        //4.关闭资源
        fs.close();
    }

    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {

        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");

        //2.更改文件名
        fs.rename(new Path("/input/bbb.txt"),new Path("/input/b.txt"));

        //3.关闭资源
        fs.close();
    }

    @Test
    public void testListFile() throws URISyntaxException, IOException, InterruptedException {


        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");

        //2.查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/hadoop-2.7.2.tar.gz"), false);

        //迭代查看
        while (listFiles.hasNext()){
            LocatedFileStatus next = listFiles.next();
            System.out.println(next.getBlockLocations());
            System.out.println(next.getLen());
            System.out.println(next.getPath());
            System.out.println(next.getReplication());
            System.out.println(next.getPermission());
            for (BlockLocation blockLocation : next.getBlockLocations()) {
                System.out.println(blockLocation.getLength());
                System.out.println(blockLocation.getNames());
                System.out.println(blockLocation.getHosts());
            }
            System.out.println("-----------------------------");

        }
        //关闭资源
        fs.close();
    }


    @Test
    public void testIO() throws URISyntaxException, IOException, InterruptedException {

        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");

        //2.创建输入流
        FileInputStream fis = new FileInputStream(new File("E:/windows/io.txt"));

        //3.创建输出流
        FSDataOutputStream fos = fs.create(new Path("/input1"));

        //4.流对拷
        IOUtils.copyBytes(fis,fos,conf);

        //5.关闭资源
        fos.close();
        fis.close();
        fs.close();

    }

    //测试IO流文件下载
    @Test
    public void testDownIO() throws IOException, URISyntaxException, InterruptedException {

        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");

        //2.创建输入流 hdfs端
        FSDataInputStream in = fs.open(new Path("/tmpfile"));

        //3.创建输出流
        FileOutputStream out = new FileOutputStream(new File("e:/windows/TMPFILE"));

        //4.流对拷
        IOUtils.copyBytes(in,out,conf);

        //5.关闭资源
        out.close();
        in.close();
        fs.close();
    }

    //定位读取文件 分块读取hdfs上的文件
    @Test
    public void testFileSeek() throws URISyntaxException, IOException, InterruptedException {

        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");

        //2.获取输入流
        FSDataInputStream in = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        //3.获取输出流
        FileOutputStream out = new FileOutputStream(new File("e:/windows/hadoop-2.7.2.tar.gz01"));

        //4.流的拷贝
        byte[] bytes = new byte[1024];

        for (int i = 0; i < 1024 * 128; i++) {
            in.read(bytes);
            out.write(bytes);
        }

        //5.关闭资源
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        fs.close();
    }

        @Test
    public void testFileSeek2() throws URISyntaxException, IOException, InterruptedException {

        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");

        //2.获取输入流
        FSDataInputStream in = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        //3.定位输入数据位置
            in.seek(1024*128*1024);

        //4.创建输出流
            FileOutputStream out = new FileOutputStream(new File("e:/windows/hadoop-2.7.2.tar.gz02"));

        //5.流对拷
            IOUtils.copyBytes(in,out,conf);

            //6.关闭资源
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        fs.close();
    }


    //文件合并
    @Test
    public void testMerge() throws URISyntaxException, IOException, InterruptedException {

        //1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "me");

        //合并文件
//        fs.ap

    }




}




