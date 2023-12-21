package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class TradeMapper extends Mapper<LongWritable, Text, Text, Text> {
    String temp = "000001";
    String targetIndex = "144107";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] split = input.split("\t");
//        String price = split[12].split(".")[1];
//        price = price.substring(0,1);
//        if (price.charAt(1) == '0')price = price.substring(0);
//        split[12] = split[12].split(".")[0]+price;
//        if (split.length!=16){
//            System.out.println(input);
//            System.out.println("123123123");
//            if (split[10].contains(targetIndex)||split[11].contains(targetIndex)) {
//                System.out.print("abc222");
//                System.out.println(input);
//            }
//        }
        if (split[10].contains(targetIndex)||split[11].contains(targetIndex)) {
            System.out.print("abc222");
            System.out.println(input);
        }

        if (split[8].equals(temp)) {
            if (split[10].equals(targetIndex) || split[11].equals(targetIndex)) System.out.print("abc111");
            String timeString = split[15].substring(8, 12);
            int time = Integer.parseInt(timeString); // 将字符串转换为整数
            if ((930 <= time && time < 1130) || (1300 <= time && time < 1457)) {
                if (split[10].equals("targetIndex") || split[11].equals("targetIndex")) System.out.print("abcd");
                if (split[14].equals("4")) { //撤单
                    if (split[10].equals("targetIndex") || split[11].equals("targetIndex")) System.out.print("abcdefg");
                    String ans = "t\t" + "1\t" + split[15]+"\t" +split[12]+"\t"+split[13]+"\t";//t 1 tradeTime Price TradeQty

                    if (split[10].equals("0")) {
                        ans = ans.concat("2");//代表方向是卖出
                        context.write(new Text(split[11]), new Text(ans));


                    } else {
                        ans = ans.concat("1");//代表方向是买入
                        context.write(new Text(split[10]), new Text(ans));


                    }
                } else { //非撤单
                    String ans = "t\t" + "2\t" + split[12] + "\t" + split[13];//t 2 Price Size
                    context.write(new Text(split[10]), new Text("b\t" + ans));//买方
                    context.write(new Text(split[11]), new Text("s\t" + ans));//卖方


                }


            }
        }
//        0 tradedate
//        1 OrigTime
//        2 SendTime
//        3 recvtime
//        4 dbtime
//        5 ChannelNo
//        6 MDStreamID
//        7 ApplSeqNum
//        8 SecurityID
//        9 SecurityIDSource
//        10 BidApplSeqNum
//        11 OfferApplSeqNum
//        12 Price
//        13 TradeQty
//        14 ExecType
//        15 tradetime

    }
}