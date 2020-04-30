import java.util.ArrayList;
import java.util.List;

public class WriteOneTest {


    public static void main(String[] args) throws Exception {
        // dst num
/**
         HdfsClient hdfsClient=new HdfsClient();
         hdfsClient.init();
         List<Long> list=new ArrayList<Long>();
         hdfsClient.deletefd("/write");
         for (int i = 0 ; i < 10 ; i++){
            hdfsClient.deletefd("/write/1.txt");

            long startTime=System.currentTimeMillis();
            hdfsClient.createFile("/write/1.txt","It is a sample test text only for test write or read!");
            long endTime=System.currentTimeMillis();

            System.out.println("文件第"+(i+1)+"次写入时间： "+(endTime-startTime)+"ms");
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
            hdfsClient.deletefd("/write");
            long startTime=System.currentTimeMillis();
            for (int j = 0; j < 10 ; j++){
                hdfsClient.createFile(String.format("/write/%d.txt",j+1),"It is a sample test text only for test write or read!");
            }
            long endTime=System.currentTimeMillis();
            System.out.println("10个文件第"+(i+1)+"次写入时间： "+(endTime-startTime)+"ms");
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
        System.out.println("写入10个文件平均每个文件耗时"+ave/10+"ms");
 **/
/**
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init();
        List<Long> list=new ArrayList<Long>();
        for (int i = 0 ; i < 10 ; i++){
            hdfsClient.deletefd("/write");
            long startTime=System.currentTimeMillis();
            for (int j = 0; j < 100 ; j++){
                hdfsClient.createFile(String.format("/write/%d.txt",j+1), String.format("**%d**It is a sample test text only for test write or read!", j+1));
            }
            long endTime=System.currentTimeMillis();
            System.out.println("100个文件第"+(i+1)+"次写入时间： "+(endTime-startTime)+"ms");
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
        System.out.println("写入100个文件平均每个文件耗时"+ave/100+"ms");
 **/
/**
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init();
        List<Long> list=new ArrayList<Long>();
        //hdfsClient.deletefd("/input");
        for (int i = 0 ; i < 12 ; i++){
            String dst="D:/test/100KB.txt";
            String contents=hdfsClient.readLocalFile(dst);
            long startTime=System.currentTimeMillis();
            hdfsClient.writeFlie("/input/100KBhdfs.txt",contents);
            long endTime=System.currentTimeMillis();
            System.out.println("文件"+dst+"第"+(i+1)+"次写入时间： "+(endTime-startTime)+"ms");
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
        long fsum=sum-max-min;
        float fave =fsum/(list.size()-2);
        System.out.println("最短时间：" + min +"ms"+"舍去");
        System.out.println("最长时间：" + max +"ms"+"舍去");
        System.out.println("舍弃前取平均时间：" + ave +"ms");
        System.out.println("舍弃后取平均时间：" + fave +"ms");
 **/
// dst num
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init(args[1]);
        List<Long> list=new ArrayList<Long>();
        //hdfsClient.deletefd("/input");
        for (int i = 0 ; i < 12 ; i++){
            String dstbase="/data/testdata/";
            String dstt=dstbase+args[0];
            String contents=hdfsClient.readLocalFile(dstt);
            String aimdstbase="/input/hdfs-";
            String aimdstt=aimdstbase+args[0];
            long startTime=System.currentTimeMillis();
            hdfsClient.writeFlie(aimdstt,contents);
            long endTime=System.currentTimeMillis();
            System.out.println("文件"+args[0]+"第"+(i+1)+"次写入时间： "+(endTime-startTime)+"ms");
            list.add(endTime-startTime);
        }
        System.out.println(list);
        long min=list.get(0);
        long max=list.get(0);
        int sum =0;
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
        System.out.println("最短时间：" + min +"ms--"+"舍去");
        System.out.println("最长时间：" + max+ "ms--"+"舍去");
        System.out.println("写入文件"+args[0]+"1个,舍弃前取平均时间：" + ave +"ms"+ "合计约"+ ave/1000 +"s");
        System.out.println("写入文件"+args[0]+"1个,舍弃后取平均时间：" + fave +"ms"+ "合计约"+ fave/1000 +"s");


/**
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init();
        List<Long> list=new ArrayList<Long>();
        //hdfsClient.deletefd("/input");
        String dst="/data/testdata/100MB.txt";
        String contents=hdfsClient.readLocalFile(dst);
        for (int i = 0 ; i < 6 ;i++){
            long startTime=System.currentTimeMillis();
            for ( int j =0 ; j < 10; j ++){
                //System.out.println(i+1+"begin"+j+1);
                hdfsClient.writeFlie(String.format("/input/1KBhdfs-%d.txt",j+1 ),contents);
                //System.out.println(i+1+"end"+j+1);
            }
            //hdfsClient.writeFlie("/input/1KBhdfs-%d.txt" ,contents);
            long endTime=System.currentTimeMillis();
            System.out.println("10个相同文件"+dst+"第"+(i+1)+"次写入时间： "+(endTime-startTime)+"ms");
            list.add(endTime-startTime);
        }
        System.out.println(list);
        long min=list.get(0);
        long max=list.get(0);
        int sum =0;
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
        System.out.println("最短时间：" + min +"ms--"+"舍去");
        System.out.println("最长时间：" + max+ "ms--"+"舍去");
        //System.out.println("舍弃前取平均时间：" + ave +"ms");
        //System.out.println("舍弃后取平均时间：" + fave +"ms");
        System.out.println("舍弃前取平均时间：" + ave/1000 +"s");
        System.out.println("舍弃后取平均时间：" + fave/1000 +"s");
 **/
    }
}
