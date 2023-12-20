package driver;

import mapper.OrderMapper;
import mapper.TradeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.ProjectReducer;


import java.io.IOException;



public class ProjectTest {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ProjectTest");

        job.setJarByClass(ProjectTest.class);

        // 设置第一个输入路径和对应的Map处理逻辑及输出类型
        //MultipleInputs.addInputPath(job, new Path("project_data/order/order_test.txt"), TextInputFormat.class, OrderMapper.class);

        MultipleInputs.addInputPath(job, new Path("project_data/order/am_hq_order_spot.txt"), TextInputFormat.class, OrderMapper.class);
        MultipleInputs.addInputPath(job, new Path("project_data/order/pm_hq_order_spot.txt"), TextInputFormat.class, OrderMapper.class);
        // 设置第二个输入路径和对应的Map处理逻辑及输出类型
        //MultipleInputs.addInputPath(job, new Path("project_data/trade/trade_test.txt"), TextInputFormat.class, TradeMapper.class);

        MultipleInputs.addInputPath(job, new Path("project_data/trade/pm_hq_trade_spot.txt"), TextInputFormat.class, TradeMapper.class);
        MultipleInputs.addInputPath(job, new Path("project_data/trade/pm_hq_trade_spot.txt"), TextInputFormat.class, TradeMapper.class);

        // 设置Reduce处理逻辑及输出类型
        job.setReducerClass(ProjectReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("output/project"));

        // 提交任务并等待完成
        if (!job.waitForCompletion(true)) {
            // 如果作业失败，退出
            return;
        }
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        String inputFilePath = "output/project/part-r-00000"; // MapReduce作业的输出路径
        String outputFilePath = "output/project/Output.txt"; // 排序后数据的存储路径

        DataSorter.sortAndWriteFile(inputFilePath, outputFilePath);
    }
}
