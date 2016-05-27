import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;


public class CountVehicleData {

    // 自定义map
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final Text vehicleKey = new Text();
        private final Text textValue = new Text();
        private HashMap<String, String> vehicleTypeMap = new HashMap<String, String>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSystem hdfs = FileSystem.get(context.getConfiguration());
//            File tmpVehicleData = new File("/user/root/vehicletype/fourCatalog.csv");
            InputStream inputStream = hdfs.open(new Path("hdfs://10.2.9.42:9000/user/root/vehicletype/fourCatalog.csv"));

//            FileReader reader = new FileReader(tmpVehicleData);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                String[] lineItems = line.split(",");
                if (lineItems.length == 3) {
                    String plateAndColor = lineItems[0] + "_" + lineItems[1];
                    String vehicleType = lineItems[2];

                    vehicleTypeMap.put(plateAndColor, vehicleType);
                }


            }
            br.close();
//            reader.close();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            vehicleTypeMap.clear();
        }


        private int getTimePeriod(int hour, int minute) {
            return (hour * 12) + (minute + 1) / 5;
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lss = line.split(",");
            if (lss.length >= 18) {
//                StringBuffer ID = new StringBuffer(lss[0]);
//                StringBuffer text = new StringBuffer(lss[6]);
//                for (int i = 1; i < 6; i++) ID.append("," + lss[i]);
//                for (int i = 7; i < lss.length; i++) text.append("," + lss[i]);
                String plate = lss[0];
                String plateColor = lss[1];
                String plateAndColor = plate + "_" + plateColor;
                String ts = lss[7];
                String tripId = "";
                //注意format的格式要与日期String的格式相匹配
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    Date backDate = sdf.parse(ts);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(backDate);
                    int timePeriod = getTimePeriod(backDate.getHours(), backDate.getMinutes());
                    String dateStr = ts.substring(0, ts.indexOf(' '));
                    String tmpType = vehicleTypeMap.get(plateAndColor);
                    tripId = dateStr + "_" + timePeriod + "_" + tmpType;

                } catch (ParseException e) {
                    e.printStackTrace();
                }

                vehicleKey.set(tripId);
                textValue.set(plate);
                context.write(vehicleKey, textValue);
            }

        }
    }


    // 自定义reduce
    //
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private final Text left = new Text();
        private final Text right = new Text();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            context.write(SEPARATOR, null);
            left.set(key);
            int count = 0;
            HashSet<String> plateSet = new HashSet<String>();
            for (Text val : values) {
                String tmpPlate = val.toString();
                if (!plateSet.contains(tmpPlate)) plateSet.add(tmpPlate);
                count++;
            }
            right.set(plateSet.size() + "," + count);
            context.write(left, right);


        }
    }


    /**
     * 删除指定目录
     *
     * @param conf
     * @param dirPath
     * @throws IOException
     */
    private static void deleteDir(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(dirPath);
        if (fs.exists(targetPath)) {
            boolean delResult = fs.delete(targetPath, true);
            if (delResult) {
                System.out.println(targetPath + " has been deleted sucessfullly. ok!");
            } else {
                System.out.println(targetPath + " deletion failed.");
            }
        }

    }

    /**
     * @param args
     * @throws URISyntaxException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // TODO Auto-generated method stub
        // 读取hadoop配置
        if (args.length < 4) {
            System.err.println("args error.");
            return;
        }
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("mapred.textoutputformat.separator", ",");

//        DistributedCache.createSymlink(conf);
//        String vehcleTypeDataPath = "/user/root/vehicletype/vehicletype.csv";
//        Path filePath = new Path(vehcleTypeDataPath);
//        String uriWithLink = filePath.toUri().toString() + "#" + "vehicletype.csv";
//        DistributedCache.addCacheFile(new URI(uriWithLink), conf);

        // 实例化一道作业
        Job job = Job.getInstance(conf, "vehiletype");
        conf.set("mapred.textoutputformat.separator", ",");
        //先删除输出目录
        deleteDir(conf, args[3]);
        job.setJarByClass(CountVehicleData.class);
        // Mapper类型
        job.setMapperClass(Map.class);
        // 不再需要Combiner类型，因为Combiner的输出类型<Text, IntWritable>对Reduce的输入类型<IntPair, IntWritable>不适用
        //job.setCombinerClass(Reduce.class);
        // Reducer类型
        job.setReducerClass(Reduce.class);
//        // 分区函数
//        job.setPartitionerClass(FirstPartitioner.class);
//        // 分组函数
//        job.setGroupingComparatorClass(GroupingComparator.class);
//
//        // map 输出Key的类型
        job.setMapOutputKeyClass(Text.class);
        // map输出Value的类型
        job.setMapOutputValueClass(Text.class);
        // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // rduce输出Value的类型
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
        job.setOutputFormatClass(TextOutputFormat.class);
        Path[] paths = FilesList.getList(conf, args[0], args[1], args[2]);
        // 输入hdfs路径
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
