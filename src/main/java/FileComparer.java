import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.IOException;
import java.util.List;

public class FileComparer {
    public static void main(String[] args) {
        String pathToFile1 = "project_data/前30sOutput.txt"; // 标准答案文件路径
        String pathToFile2 = "output/project/Output.txt"; // 输出文件路径

        try {
            // 读取文件内容
            List<String> file1Lines = Files.readAllLines(Paths.get(pathToFile1));
            List<String> file2Lines = Files.readAllLines(Paths.get(pathToFile2));


            // 逐行比较
            for (int i = 0; i < file1Lines.size(); i++) {
                if (!file1Lines.get(i).equals(file2Lines.get(i))) {
                    System.out.printf("第 %d 行不同。\n", i + 1);
                    System.out.println("标准答案: " + file1Lines.get(i));
                    System.out.println("我的输出: " + file2Lines.get(i));
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}