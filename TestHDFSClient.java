
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.FileInputStream;


public class TestHDFSClient{
    public static void main(String[] args) throws Exception {


/**
        Configuration conf = new Configuration();

        //这里指定使用的是 hdfs文件系统
        //conf.set("fs.defaultFS", "hdfs://hadoop-namenode:9000");
        conf.set("fs.defaultFS", "hdfs://10.0.85.107:9000/");
        //hdfs://hadoop-namenode:9000/

        //通过这种方式设置java客户端身份
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(conf);
        //或者使用下面的方式设置客户端身份
        //FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"root");

        // fs.create(new Path("/helloByJava")); //创建一个目录

        //文件下载到本地 如果出现0644错误或找不到winutils.exe,则需要设置windows环境和相关文件.
        //fs.copyToLocalFile(new Path("/zookeeper.out"), new Path("D:\\test\\examplehdfs"));


        //使用Stream的形式操作HDFS，这是更底层的方式
        FSDataOutputStream outputStream = fs.create(new Path("/4.txt"), true); //输出流到HDFS
        FileInputStream inputStream = new FileInputStream("D:/test/examplehdfs/3.txt"); //从本地输入流。
        IOUtils.copy(inputStream, outputStream); //完成从本地上传文件到hdfs
        System.out.println("文件上传成功！");

        fs.close();
 **/
/**
        long startTime=System.currentTimeMillis(); //获取开始时间
        for (int i=0;i<10;i++){
            int a=i*i;
            int b=i*i*i;
            System.out.println("i="+i+",i*i="+a+",i*i*i="+b);
        }
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
        long t1=System.nanoTime(); //获取开始时间
        for (int i=0;i<10;i++){
            int a=i*i;
            int b=i*i*i;
            System.out.println("i="+i+",i*i="+a+",i*i*i="+b);
        }
        long t2=System.nanoTime(); //获取结束时间
        System.out.println("程序运行时间： "+(t2-t1)+"ms");
**/
/**
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init();
        hdfsClient.deletefd("/input");
        hdfsClient.writeFlie("D:/test/1m.txt","/input/1m.txt");
 **/
/**
        long startTime=new Date().getTime(); //获取开始时间
        System.out.println(startTime);
        try {
            Thread.currentThread().sleep(3000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long endTime=new Date().getTime(); //获取结束时间
        System.out.println(endTime);
        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
 **/

        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init("3");
        hdfsClient.deletefd("/input");
//        String dst="D:/test/1KB.txt";
//        String contents=hdfsClient.readLocalFile(dst);
//        hdfsClient.writeFlie("/input/1KBhdfs.txt",contents);

    }
}