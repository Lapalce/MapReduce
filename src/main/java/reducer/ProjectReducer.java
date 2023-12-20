package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ProjectReducer extends Reducer<Text, Text, Text, Text> {
    private static void to_write(Text key, Context context, String[] ans){
        // 构造并返回最终格式化的时间戳字符串
        String input = ans[0];
        String year = input.substring(0, 4);
        String month = input.substring(4, 6);
        String day = input.substring(6, 8);
        String hour = input.substring(8, 10);
        String minute = input.substring(10, 12);
        String second = input.substring(12, 14);
        String millisecond = input.substring(14, 17);

        String time = String.format("%s-%s-%s %s:%s:%s.%s000", year, month, day, hour, minute, second, millisecond);
        key =new Text(time);

        StringBuilder output = new StringBuilder();
        for (int i = 1; i < ans.length; i++) {
            output.append(",").append(ans[i]);
        }

        try {
            context.write(key, new Text(output.toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
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
            ans[2] = orderInfo[3];//成交量size
            ans[3] = orderInfo[4];//买卖side
            ans[4] = orderInfo[5];//委托单类型orderType
            ans[5] = String.valueOf(key);//委托单id

            if (ans[5].equals("144107")) {
                System.out.println("abcdefg");
                System.out.println(order);
                for (String t : trade){
                    System.out.println(t);
                }
            }

            ans[6] = "";//成交档位默认为空
            ans[7] = "2";//默认非撤单

            boolean flag;//是否撤单

            switch (orderInfo[5]) {
                case "1"://市价
                    ans[1] = "";//价格，市价单为空
                    to_write(key, context, ans);//输出一次order
                    flag = false;
                    if (!trade.isEmpty()){
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
                                ans[7] = "2";
                                //ans[0] = orderInfo[1];//时间
                            }
                            ans[2] = (String.valueOf(price.size()));//成交价格数量
                        }
                        ans[6] = String.valueOf(price.size());
                        to_write(key, context, ans);//输出一次trade
                    }

                    break;
                case "2"://限价
                    ans[1] = orderInfo[2].substring(0, orderInfo[2].length() - 4);//价格
                    to_write(key, context, ans);
                    flag = false;
                    if (!trade.isEmpty()){
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
                    if (!trade.isEmpty()){
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
        }
    }
}
