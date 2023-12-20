import java.io.*;

import static com.ibm.dtfj.javacore.parser.j9.section.common.CommonPatternMatchers.equals;
import static sun.misc.Version.println;

class FilterOrder {

    public static void main(String[] args) {
        // 输入文件夹和输出文件的路径
        String inputFolderPath1 = "project_data/order/am_hq_order_spot.txt";
        String inputFolderPath2 = "project_data/order/pm_hq_order_spot.txt";

        String outputFilePath = "project_data/order/hq_order_spot_thinned.txt";

        File file_am = new File(inputFolderPath1);
        File file_pm = new File(inputFolderPath2);
        File[] listOfFiles = {file_am,file_pm};

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (File file : listOfFiles) {
                if (file.isFile()) {
                    System.out.println("Processing file: " + file.getName());

                    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            // 检查条件，如果满足则写入输出文件
                            if (meetsCondition(line)) {
                                writer.write(line);
                                writer.newLine(); // 添加换行符以分隔内容
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    // 示例条件检查方法，你可以根据需要修改这个方法
    private static boolean meetsCondition(String line) {
        String[] split = line.split("\t");
        String timeString = split[12].substring(8, 12);
        int time = Integer.parseInt(timeString); // 将字符串转换为整数
        //boolean flag = (930 <= time && time < 1130) || (1300 <= time && time < 1457) || true;
        //flag &&
        // 示例条件：如果行包含特定文字
        return  line.contains("000001")&&split[14].equals("1");  //平安银行&市价单&价格不等于0

    }
}


class FilterTrade {

    public static void main(String[] args) {
        // 输入文件夹和输出文件的路径
        String inputFolderPath1 = "project_data/trade/am_hq_trade_spot.txt";
        String inputFolderPath2 = "project_data/trade/pm_hq_trade_spot.txt";

        String outputFilePath = "project_data/trade/hq_trade_spot_thinned.txt";

        File file_am=new File(inputFolderPath1);
        File file_pm=new File(inputFolderPath2);
        File[] listOfFiles = {file_am,file_pm};

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (File file : listOfFiles) {
                if (file.isFile()) {
                    System.out.println("Processing file: " + file.getName());

                    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            // 检查条件，如果满足则写入输出文件
                            if (meetsCondition(line)) {
                                writer.write(line);
                                writer.newLine(); // 添加换行符以分隔内容
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    // 示例条件检查方法，你可以根据需要修改这个方法
    private static boolean meetsCondition(String line) {
        String[] split = line.split("\t");
        String execType = split[14];

        String timeString = split[15].substring(8, 12);
        int time = Integer.parseInt(timeString); // 将字符串转换为整数
        boolean flag = (930 <= time && time < 1130) || (1300 <= time && time < 1457) || true;
        if (execType.equals("4")) {}//把他输出到一个文档里
        // 示例条件：如果行包含特定文字
        return flag && line.contains("000001")&&execType.equals("F");

    }
}
class ReadFileAndPrint{
    public static void main(String[] args) {
        // 输入文件夹和输出文件的路径
        String inputOrderPath = "project_data/order/am_hq_order_spot_thinned.txt";
        String inputTradePath = "project_data/order/am_hq_order_spot_thinned.txt";
      //  String outputFilePath = "project_data/trade/pm_hq_trade_spot_thinned.txt";

        File fileOrder1 = new File(inputOrderPath);
        File fileOrder2 = new File(inputOrderPath);
        //  File[] listOfFiles = folder.listFiles();

        {

            if (fileOrder1.isFile()) {
                System.out.println("Processing file: " + fileOrder1.getName());

                try (BufferedReader br = new BufferedReader(new FileReader(fileOrder1))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // 检查条件，如果满足则写入输出文件

                        String[] split = line.split("\t");

                        // 确定需要的字段
                        String ApplSeqNum = split[7];
                        if (ApplSeqNum.equals("556890")) {
                            System.out.println(line);
                        }
                        if (ApplSeqNum.equals("531284")) {
                            System.out.println(line);
                        }if (ApplSeqNum.equals("531878")) {
                            System.out.println(line);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }



}

