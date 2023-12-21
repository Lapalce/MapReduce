package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ProjectReducer extends Reducer<Text, Text, Text, Text> {
    private static void to_write(Text key, Context context, String[] ans) {
        // 构造并返回最终格式化的时间戳字符串

        String input = ans[0];
        StringBuilder output = new StringBuilder();
        try {
            String year = input.substring(0, 4);
            String month = input.substring(4, 6);
            String day = input.substring(6, 8);
            String hour = input.substring(8, 10);
            String minute = input.substring(10, 12);
            String second = input.substring(12, 14);
            String millisecond = input.substring(14, 17);

            String time = String.format("%s-%s-%s %s:%s:%s.%s000", year, month, day, hour, minute, second, millisecond);
            key = new Text(time);


            for (int i = 1; i < ans.length; i++) {
                output.append(",").append(ans[i]);
            }
        } catch (Exception e) {
            System.out.println("目标信息出现" + key + "\t" + context);
        }
        try {
            context.write(key, new Text(output.toString()));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] ans = new String[8];//0：成交时间 1：价格price 2：数量size 3: 买卖方向side 4: 委托单类型orderType 5: id 6: Market_order_type 7:是否撤单

        String order = null;
        ArrayList<String> trade = new ArrayList<>();
        for (Text value : values) {  //分类存储order trade
            String[] split = value.toString().split("\t");
            if (split[0].equals("o")) order = value.toString();
            else trade.add(value.toString());
        }
        if (order != null) {
            Set<Integer> price = new HashSet<>();
            String[] orderInfo = order.split("\t");

            ans[0] = orderInfo[1];//委托时间
            ans[2] = orderInfo[3];//成交数量
            ans[3] = orderInfo[4];//买卖side
            ans[4] = orderInfo[5];//委托单类型orderType
            ans[5] = String.valueOf(key);//委托单id

//            if (ans[5].equals("144107")) {
//                System.out.println("abcdefg");
//                System.out.println(order);
//                for (String t : trade){
//                    System.out.println(t);
//                }
//            }

            ans[6] = "";//成交档位默认为空
            ans[7] = "2";//默认非撤单

            boolean flag;//是否撤单

            switch (orderInfo[5]) {
                case "1"://市价
                    ans[1] = "";//价格，市价单为空

                    flag = false;
                    if (!trade.isEmpty()) {
                        for (String value : trade) {
                            String[] split = value.split("\t");
                            if (split[1].equals("1"))//撤单
                            {
                                flag = true;
                                ans[7] = "1";
                                ans[0] = split[2];//时间

                            } else {
                                price.add(Integer.valueOf(split[2]));
                            }
                            if (!flag) {

                            }

                        }
                        ans[6] = String.valueOf(price.size());
                        if (flag) {

                            to_write(key, context, ans);//输出一次trade
                        } else {
                            to_write(key, context, ans);//输出一次order
                        }

                    }

                    break;
                case "2"://限价
                    ans[1] = orderInfo[2].substring(0, orderInfo[2].length() - 4);//价格
                    to_write(key, context, ans);
                    flag = false;
                    if (!trade.isEmpty()) {
                        for (String value : trade) {
                            String[] split = value.split("\t");
                            if (split[1].equals("1"))//撤单
                            {
                                flag = true;
                                ans[7] = "1";//撤单
                                ans[0] = split[2];//时间
                            }
                            if (!flag) {
                                ans[7] = "2";//非撤单
                                //ans[0] = orderInfo[1];//时间
                            }
                        }
                        to_write(key, context, ans);
                    }

                    break;
                case "U"://本方最优
                    ans[1] = "";
                    to_write(key, context, ans);
                    flag = false;
                    if (!trade.isEmpty()) {
                        for (String value : trade) {
                            String[] split = value.split("\t");
                            if (split[1].equals("1"))//撤单
                            {
                                flag = true;
                                ans[7] = "1";//撤单
                                ans[0] = split[2];//时间
                            }
                            if (!flag) {
                                ans[7] = "2";//非撤单
                                //ans[0] = orderInfo[1];
                            }
                        }
                        to_write(key, context, ans);
                    }
                    break;
            }
        } else {

            //if (trade.size()>1) System.out.println("下单时间不在连续竞价期间，他成交了，且撤单，但是在连续竞价期间有撤单行为");//已验证没有这种情况
            boolean flag1 = false;
            boolean flag2 = false;
            int index = -1;
            for (int i = 0; i < trade.size() ; i++) {
                String tradeLine = trade.get(i);
                String[] tradeInfo = tradeLine.split("\t");
                if (tradeInfo[2].equals("1")||tradeInfo[1].equals("1")) {
                    flag1 = true;
                    index = i;
                }
                if (tradeInfo[2].equals("2")||tradeInfo[1].equals("2")) flag2 = true;
            }

            if (flag1 && flag2) System.out.println("case1");
            if (flag1 && !flag2) System.out.println("case2");//只有撤回没有成交
            if (!flag1 && flag2) System.out.println("case3");//只有成交没有撤回
            if (!flag1 && !flag2) {System.out.println("case4");

            }
            //验证后发现都是case4
            //System.out.println("----------------------");
            if (flag1){
                String tradeLine = trade.get(index);
                String[] tradeInfo = tradeLine.split("\t");
                if (tradeInfo[1].equals("1")) {
                    ans[0] = tradeInfo[2];//撤单时间
                    ans[1] = "";//不填了
                    ans[2] = tradeInfo[4];//成交量size
                    try {
                        ans[3] = tradeInfo[5];//买卖side
                    } catch (ArrayIndexOutOfBoundsException e) {

                        ans[3] = "0";
                        System.out.println("try catch activated");
                        System.out.println(key);
                        System.out.println(tradeLine);
                    }
                    ans[4] = "";//不填了
                    ans[5] = String.valueOf(key);//委托单id
                    ans[6] = "";//成交档位暂时默认为空，但是这里存疑，万一他成交了，且撤单，我还填不填档位
                    ans[7] = "1";//默认非撤单
                }

                to_write(key, context, ans);
            }//有撤回就处理撤回，只写撤回信息，不写交易成功的信息


        }//此类数据为，下单时间不在连续竞价期间，但是在连续竞价期间有撤单行为
    }
}
