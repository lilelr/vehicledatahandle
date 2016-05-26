
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AlarmStatusManageMent {


    public static class Map
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable mkey = new IntWritable();

        /**
         * @param key     行号
         * @param value   每一行数据
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lss = line.split(",");
            if (lss.length >= 18) {
                // 报警位数据处理
//                int alarmField = Integer.valueOf(lss[17]);
//                int bitIndex =0;
//                while (alarmField!=0){
//                    if((alarmField&1) == 1){
//                        mkey.set(bitIndex);
//                        context.write(mkey,one);
//                    }
//                    alarmField >>= 1;
//                    bitIndex++;
//                }
                // 状态位数据处理
                int statusField = Integer.valueOf(lss[16]);
                int bitIndex = 0;
                while (statusField != 0) {
                    if ((statusField & 1) == 1) {
                        mkey.set(bitIndex);
                        context.write(mkey, one);
                    }
                    statusField >>= 1;
                    bitIndex++;
                }
            }

        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * @param key     报警位或状态位
         * @param values  报警位或状态位为1的数据集合
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    /**
     * 删除指定目录
     *
     * @param conf    hadoop configuration
     * @param dirPath 待删除的输出目录
     * @throws IOException
     */
    private static void deleteDir(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(dirPath);
        if (fs.exists(targetPath)) {
            boolean delResult = fs.delete(targetPath, true);
            if (delResult) {
                System.out.println(targetPath + " has been deleted sucessfullly. ok ");
            } else {
                System.out.println(targetPath + " deletion failed.");
            }
        }

    }

    /**
     * @param args 共4个参数  args[0] hdfsURI 如hdfs://10.2.9.42:9000/
     *             args[1] 输入文件目录 /lwlk_data/
     *             args[2] 要处理的数据文件名   320000.data
     *             args[3]  输出文件目录  /lwlk_data/20160526_statusTest/
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 读取hadoop配置
        if (args.length < 4) {
            System.err.println("args error.");
            return;
        }
        Configuration conf = new Configuration();
//        conf.set("mapred.textoutputformat.ignoreseparator", "true");
//        conf.set("mapred.textoutputformat.separator", ",");


        // 实例化一道作业

        //先删除输出目录
        deleteDir(conf, args[3]);

        Job job = Job.getInstance(conf, "vehicle AlarmOrStatus ");
        job.setJarByClass(AlarmStatusManageMent.class);
        job.setMapperClass(AlarmStatusManageMent.Map.class);
        job.setReducerClass(AlarmStatusManageMent.IntSumReducer.class);
        // map 输出Key的类型
        job.setMapOutputKeyClass(IntWritable.class);
        // map输出Value的类型
        job.setMapOutputValueClass(IntWritable.class);
        // rduce输出Key的类型
        job.setOutputKeyClass(IntWritable.class);
        // rduce输出Value的类型
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        Path[] paths = FilesList.getList(conf, args[0], args[1], args[2]);
//         输入hdfs路径
        for (Path path : paths) {
            FileInputFormat.addInputPath(job, path);
        }
//        Path path = new Path(args[1]);
//        FileInputFormat.addInputPath(job, path);

        // 输出hdfs路径
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        // 提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
