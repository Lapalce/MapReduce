package driver;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DataSorter {


    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static void sortAndWriteFile(String inputFilePath, String outputFilePath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(inputFilePath));
        List<DataWithTimestamp> dataList = new ArrayList<>();

        for (String line : lines) {
            String timestampPart = line.split(",")[0].trim();
            // 仅截取到毫秒部分
            String fixedTimestampPart = timestampPart.substring(0, 23);
            String id = line.split(",")[5];
            LocalDateTime timestamp = LocalDateTime.parse(fixedTimestampPart, FORMATTER);
            dataList.add(new DataWithTimestamp(timestamp, line, id));
        }

        // 按时间戳排序
        Collections.sort(dataList, new Comparator<DataWithTimestamp>() {
            @Override
            public int compare(DataWithTimestamp d1, DataWithTimestamp d2) {
                int timestampComparison = d1.getTimestamp().compareTo(d2.getTimestamp());
                if (timestampComparison != 0) {
                    return timestampComparison;
                }
                // 如果时间戳相同，则根据编号进行比较
                return d1.getId().compareTo(d2.getId());
            }
        });

        // 将排序后的数据写入新文件
        List<String> sortedLines = new ArrayList<>();
        sortedLines.add("TIMESTAMP,PRICE,SIZE,BUY_SELL_FLAG,ORDER_TYPE,ORDER_ID,MARKET_ORDER_TYPE,CANCEL_TYPE");
        for (DataWithTimestamp data : dataList) {

            sortedLines.add(data.getOriginalData());

        }
        Files.write(Paths.get(outputFilePath), sortedLines, StandardCharsets.UTF_8);
    }

    static class DataWithTimestamp {
        private LocalDateTime timestamp;
        private String originalData;

        private String id;



        public DataWithTimestamp(LocalDateTime timestamp, String originalData, String id) {
            this.timestamp = timestamp;
            this.originalData = originalData;
            this.id = id;
        }

        public LocalDateTime getTimestamp() {
            return timestamp;
        }

        public String getOriginalData() {
            return originalData.replace("\t","");
        }

        public String getId() {
            return id;
        }
    }


    public static void main(String[] args) {
        String inputFilePath = "output/project/part-r-00000"; // MapReduce作业的输出路径
        String outputFilePath = "output/project/Output.txt"; // 排序后数据的存储路径

        try {
            sortAndWriteFile(inputFilePath, outputFilePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}