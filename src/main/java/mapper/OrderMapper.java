package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class OrderMapper extends Mapper<LongWritable, Text, Text, Text>{
    String temp="000001";
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] split = input.split("\t");

        if (split[8].equals(temp)){
            String timeString = split[12].substring(8, 12);
            int time = Integer.parseInt(timeString); // 将字符串转换为整数
            if ((930 <= time && time < 1130) || (1300 <= time && time < 1457)) {
                //          o   TransactTime    Price       Size            Side            OrderType
                String ans = "o\t"+split[12]+"\t"+split[10]+"\t"+split[11]+"\t"+split[13]+"\t"+split[14];
                context.write(new Text((split[7])), new Text(ans));



            }
        }

    }
//0 tradedate
//1 OrigTime
//2 SendTime
//3 recvtime
//4 dbtime
//5 ChannelNo
//6 MDStreamID
//7 ApplSeqNum
//8 SecurityID
//9 SecurityIDSource
//10 Price
//11 OrderQty
//12 TransactTime
//13 Side
//14 OrderType
//15 ConfirmID
//16 Contactor
//17 ContactInfo
//18 ExpirationDays
//19 ExpirationType
}