import java.util.ArrayList;
import java.util.List;

public class WriteMoreTest {
    public static void main(String[] args) throws Exception{
//dst count num
        HdfsClient hdfsClient=new HdfsClient();
        hdfsClient.init(args[2]);
        List<Long> list=new ArrayList<Long>();
        //hdfsClient.deletefd("/input");
        String dstbase="/data/testdata/";
        String dstt=dstbase+args[0];
        String contents=hdfsClient.readLocalFile(dstt);
        String aimdst="/input/hdfs-";
        int count=Integer.parseInt(args[1]);
        for (int i = 0 ; i < 12 ;i++){
            long startTime=System.currentTimeMillis();
            for ( int j =0 ; j < count; j ++){
                //System.out.println(i+1+"begin"+j+1);
                hdfsClient.writeFlie(aimdst+(j+1)+"-"+args[0],contents);
                //System.out.println(i+1+"end"+j+1);
            }
            //hdfsClient.writeFlie("/input/1KBhdfs-%d.txt" ,contents);
            long endTime=System.currentTimeMillis();
            System.out.println(args[1]+"个文件"+args[0]+"第"+(i+1)+"次写入时间： "+(endTime-startTime)+"ms");
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
        System.out.println("写入文件"+args[0]+args[1]+"个,舍弃前取平均时间：" + ave +"ms"+ "合计约"+ ave/1000 +"s");
        System.out.println("写入文件"+args[0]+args[1]+"个,舍弃后取平均时间：" + fave +"ms"+ "合计约"+ fave/1000 +"s");
//        System.out.println("舍弃前取平均时间：" + ave +"ms");
//        System.out.println("舍弃后取平均时间：" + fave +"ms");
//        System.out.println("舍弃前取平均时间：" + ave/1000 +"s");
//        System.out.println("舍弃后取平均时间：" + fave/1000 +"s");
    }
}
