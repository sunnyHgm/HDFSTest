import java.util.ArrayList;
import java.util.List;

public class ReadMoreTest {
    public static void main(String[] args) throws Exception {
        // dst count
        String num="1";//读取文件时设置副本数没有任何意义，默认为1
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init(num);
        List<Long> list=new ArrayList<Long>();
        String localPath="/data1/readdata/read.txt";
        int count=Integer.parseInt(args[1]);
        for (int i = 0 ; i < 7 ; i++){
            long startTime=System.currentTimeMillis();
            for (int j = 0; j < count ; j++){
                String filePath="/input/hdfs-"+(j+1)+"-"+args[0];
                hdfsClient.readFile(filePath,localPath);
            }
            long endTime=System.currentTimeMillis();
            System.out.println(args[1]+"个文件第"+(i+1)+"次读取时间： "+(endTime-startTime)+"ms");
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
        System.out.println("读取文件"+args[0]+args[1]+"个,舍弃前取平均时间：" + ave +"ms"+ "合计约"+ ave/1000 +"s");
        System.out.println("读取文件"+args[0]+args[1]+"个,舍弃后取平均时间：" + fave +"ms"+ "合计约"+ fave/1000 +"s");

    }
}
