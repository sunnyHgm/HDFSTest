import java.util.ArrayList;
import java.util.List;

public class ReadOneTest {
    public static void main(String[] args) throws Exception {
        // fdst

/**
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init();
        List<Long> list=new ArrayList<Long>();
        for (int i = 0 ; i < 10 ; i++){

            Long startTime=System.currentTimeMillis();
            hdfsClient.readFile("/write/1.txt");
            long endTime=System.currentTimeMillis();
            System.out.println("文件第"+(i+1)+"次读取时间： "+(endTime-startTime)+"ms");
            list.add(endTime-startTime);
        }
        System.out.println(list);
        long min=list.get(0);
        long max=list.get(0);
        int sum=0;
        float ave=0;
        for(int i=0;i<list.size() ;i++) {
            if(min>list.get(i))
                min = list.get(i);
            if(max<list.get(i))
                max = list.get(i);
            sum = sum + list.get(i).intValue();
        }
        ave = sum/list.size();
        System.out.println("最短时间：" + min +"ms");
        System.out.println("最长时间：" + max +"ms");
        System.out.println("平均时间：" + ave +"ms");
 **/

/**
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init();
        List<Long> list=new ArrayList<Long>();
        for (int i = 0 ; i < 10 ; i++){
            long startTime=System.currentTimeMillis();
            for (int j = 0; j < 10 ; j++){
                hdfsClient.readFile(String.format("/write/%d.txt", j+1));
            }
            long endTime=System.currentTimeMillis();
            System.out.println("/--------------------------------------/");
            System.out.println("10个文件第"+(i+1)+"次读取时间： "+(endTime-startTime)+"ms");
            list.add(endTime-startTime);
        }
        System.out.println(list);
        long min=list.get(0);
        long max=list.get(0);
        int sum=0;
        float ave=0;
        for(int i=0;i<list.size() ;i++) {
            if(min>list.get(i))
                min = list.get(i);
            if(max<list.get(i))
                max = list.get(i);
            sum = sum + list.get(i).intValue();
        }
        ave = sum/list.size();
        System.out.println("最短时间：" + min +"ms");
        System.out.println("最长时间：" + max +"ms");
        System.out.println("平均时间：" + ave +"ms");
        System.out.println("读取10个文件平均每个文件耗时"+ave/10+"ms");
**/
/**
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init();
        List<Long> list=new ArrayList<Long>();
        for (int i = 0 ; i < 10 ; i++){
            long startTime=System.currentTimeMillis();
            for (int j = 0; j < 100 ; j++){
                hdfsClient.readFile(String.format("/write/%d.txt",j+1));
            }
            long endTime=System.currentTimeMillis();
            System.out.println("100个文件第"+(i+1)+"次读取时间： "+(endTime-startTime)+"ms");
            list.add(endTime-startTime);
        }
        System.out.println(list);
        long min=list.get(0);
        long max=list.get(0);
        int sum=0;
        float ave=0;
        for(int i=0;i<list.size() ;i++) {
            if(min>list.get(i))
                min = list.get(i);
            if(max<list.get(i))
                max = list.get(i);
            sum = sum + list.get(i).intValue();
        }
        ave = sum/list.size();
        System.out.println("最短时间：" + min +"ms");
        System.out.println("最长时间：" + max +"ms");
        System.out.println("平均时间：" + ave +"ms");
        System.out.println("读取100个文件平均每个文件耗时"+ave/100+"ms");

**/
        String num="1";//读取文件时设置副本数没有任何意义，默认为1
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init(num);
        List<Long> list=new ArrayList<Long>();
        String filePath="/input/hdfs-"+args[0];
        for (int i = 0 ; i < 12 ; i++){
            long startTime=System.currentTimeMillis();
            String localPath="/data/readdata/read.txt";
            hdfsClient.readFile(filePath,localPath);
            long endTime=System.currentTimeMillis();
            System.out.println("文件"+args[0]+"第"+(i+1)+"次读取时间： "+(endTime-startTime)+"ms");
            list.add(endTime-startTime);
        }
        System.out.println(list);
        long min=list.get(0);
        long max=list.get(0);
        int sum=0;
        for(int i=0;i<list.size() ;i++) {
            if(min>list.get(i))
                min = list.get(i);
            if(max<list.get(i))
                max = list.get(i);
            sum = sum + list.get(i).intValue();
        }
        float ave = sum/list.size();
        long fsum=sum-max-min;
        float fave =fsum/(list.size()-2);
        System.out.println("最短时间：" + min +"ms"+"舍去");
        System.out.println("最长时间：" + max +"ms"+"舍去");
        System.out.println("读取文件"+args[0]+"1个,舍弃前取平均时间：" + ave +"ms"+ "合计约"+ ave/1000 +"s");
        System.out.println("读取文件"+args[0]+"1个,舍弃后取平均时间：" + fave +"ms"+ "合计约"+ fave/1000 +"s");


    }
}
