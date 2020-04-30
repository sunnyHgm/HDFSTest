
import java.io.*;
import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class HdfsClient {

    FileSystem fs = null;

    @Before
    public void init(String num) throws Exception {


        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "hdfs://node-1:9000");
        //修改当前文件的副本数量
        conf.set("dfs.replication", num);
        /**
         * 参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、然后是jar中默认配置
         */
        // 获取一个hdfs的访问客户端
        try {
            fs = FileSystem.get(new URI("hdfs://10.0.85.107:9000/"), conf, "root");
            System.out.println("init()成功,已连接hdfs客户端Client");
        }
        catch (Exception e) {
            System.out.println("init()失败,hdfs客户端Client连接异常");
        }

    }

    /**
     * 往hdfs上传文件
     *
     * @throws Exception
     */
    @Test
    public void testAddFileToHdfs() throws Exception {

        // 要上传的文件所在的本地路径

        // 要上传到hdfs的目标路径*/
        Path src = new Path("D:/test/upload/readme.txt");
        Path dst = new Path("/Test/readme.txt");
        fs.copyFromLocalFile(src, dst);

        fs.close();
    }

    /**
     * 从hdfs中复制文件到本地文件系统
     *
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Test
    public void testDownloadFileToLocal() throws IllegalArgumentException, IOException {

        // fs.copyToLocalFile(new Path("/mysql-connector-java-5.1.28.jar"), new
        // Path("d:/"));
        fs.copyToLocalFile(false, new Path("/2.txt"), new Path("D:/test/download"), true);
        fs.copyToLocalFile(false, new Path("/4.txt"), new Path("D:/test/download"), true);
        fs.close();

    }

    /**
     * 目录操作
     *
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {

        // 创建目录
        //fs.mkdirs(new Path("/a1/b1/c1"));

        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true ，删除所有子文件夹
        fs.delete(new Path("/Test"), true);

        // 重命名文件或文件夹
        //fs.rename(new Path("/a1"), new Path("/a2"));

    }

    /**
     * 查看目录信息，只显示文件
     *
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    @Test
    public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {


        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {

            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }

            }

            System.out.println("--------------打印的分割线--------------");

        }

    }



    /**
     * 查看文件及文件夹信息
     *
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    @Test
    public void testListAll() throws FileNotFoundException, IllegalArgumentException, IOException {
        //可以右击方法名，Run 测试一下。
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        String flag = "";
        for (FileStatus fstatus : listStatus) {

            if (fstatus.isFile()) {
                flag = "f-- ";
            } else {
                flag = "d-- ";
            }
            System.out.println(flag + fstatus.getPath().getName());
            System.out.println(fstatus.getPermission());

        }

    }

    /**
     * 给定文件名和文件内容，创建hdfs文件
     *
     * @param dst
     * @param contents
     * @throws IOException
     */

    @Test
    public void createFile(String dst, String contents) throws IOException {
        /**
         Configuration conf = new Configuration();
         Path dstPath = new Path(dst);
         FileSystem fs = dstPath.getFileSystem(conf);
         **/
        Path dstPath = new Path(dst);
        byte[] content = contents.getBytes("UTF-8");
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(content);
        outputStream.close();
        //System.out.println("create file " + dst + " success!");
        //fs.close();
    }

    @Test
    public void writeFlie(String dst,String  contents) throws IOException {
        /**
         Configuration conf = new Configuration();
         Path dstPath = new Path(dst);
         FileSystem fs = dstPath.getFileSystem(conf);
         **/
        Path dstPath = new Path(dst);
        byte[] content = contents.getBytes("UTF-8");
        FSDataOutputStream outputStream = fs.create(dstPath,true);
        outputStream.write(content);
        outputStream.close();
        //System.out.println("create file " + aimdst + " success!");
        //fs.close();

    }


    /**
     * 读取hdfs文件内容，并在控制台打印出来
     *
     * @param filePath
     * @throws IOException
     */
    @Test
    public void readFile(String filePath,String localPath) throws IOException {
        /**
         Configuration conf = new Configuration();
         Path srcPath = new Path(filePath);
         FileSystem fs = null;
         URI uri;
         **/
        Path srcPath = new Path(filePath);
        //InputStream in = null;
        FSDataInputStream in= fs.open(new Path(filePath));
        FileOutputStream out= new FileOutputStream(new File(localPath));
        try {
            //in = fs.open(srcPath);
            //IOUtils.copyBytes(in, System.out, 4096, false);
            IOUtils.copyBytes(in,out,4096,false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 目录操作
     *
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void deletefd(String fdPath) throws IllegalArgumentException, IOException {
        Path srcPath = new Path(fdPath);
        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true ，删除所有子文件夹
        fs.delete(srcPath, true);
        //System.out.println("hdfs分布式文件系统中"+srcPath+"已删除！");
    }


    @After
    public void testAfter(){
        System.out.println("This is the end!!");
    }

    @Test
    public  String readLocalFile(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        StringBuffer sbf = new StringBuffer();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                sbf.append(tempStr);
            }
            reader.close();
            return sbf.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return sbf.toString();
    }


}