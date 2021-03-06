import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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


public class Classfication {
    //自己定义的key类应该实现WritableComparable接口
    public static class VehiclePair implements WritableComparable<VehiclePair> {
        String tripID;
        String plate;

        /**
         * Set the left and right values.
         */
        public void set(String ID, String ts) {
            this.tripID = ID;
            this.plate = ts;
        }

        public String getTripID() {
            return tripID;
        }

        public void setTripID(String tripID) {
            this.tripID = tripID;
        }

        public String getPlate() {
            return plate;
        }

        public void setPlate(String plate) {
            this.plate = plate;
        }

        //反序列化，从流中的二进制转换成IntPair
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
            tripID = in.readUTF();
            plate = in.readUTF();
        }

        //序列化，将IntPair转化成使用流传送的二进制
        public void write(DataOutput out) throws IOException {
            // TODO Auto-generated method stub
            out.writeUTF(tripID);
            out.writeUTF(plate);
        }

        //key的比较
        public int compareTo(VehiclePair o) {
            // TODO Auto-generated method stub
            if (!tripID.equals(o.tripID)) {
                return tripID.compareTo(o.tripID) < 0 ? -1 : 1;
            } else if (!plate.equals(o.plate)) {
                return plate.compareTo(o.plate) < 0 ? -1 : 1;
            } else {
                return 0;
            }
        }

        //新定义类应该重写的两个方法
        @Override
        //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
        public int hashCode() {
            int ch = tripID.hashCode();
            return ch * ch + tripID.hashCode();
        }

        @Override
        public boolean equals(Object _pair) {
            if (_pair == null)
                return false;
            if (this == _pair)
                return true;
            if (_pair instanceof VehiclePair) {
                VehiclePair r = (VehiclePair) _pair;
                return r.tripID.equals(tripID) && r.plate.equals(plate);
            } else {
                return false;
            }
        }
    }

    /**
     * 分区函数类。根据first确定Partition。
     */
    public static class FirstPartitioner extends Partitioner<VehiclePair, Text> {
        @Override
        public int getPartition(VehiclePair key, Text value, int numPartitions) {
            return Math.abs(key.getTripID().hashCode() * 127) % numPartitions;
        }
    }

    /**
     * 分组函数类。只要first相同就属于同一个组。
     */
    /*//第一种方法，实现接口RawComparator
    public static class GroupingComparator implements RawComparator<IntPair> {
        @Override
        public int compare(IntPair o1, IntPair o2) {
            int l = o1.getFirst();
            int r = o2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
        @Override
        //一个字节一个字节的比，直到找到一个不相同的字节，然后比这个字节的大小作为两个字节流的大小比较结果。
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            // TODO Auto-generated method stub
             return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8,
                     b2, s2, Integer.SIZE/8);
        }
    }*/
    //第二种方法，继承WritableComparator
    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(VehiclePair.class, true);
        }

        @Override
        //Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2) {
            VehiclePair vp1 = (VehiclePair) w1;
            VehiclePair vp2 = (VehiclePair) w2;
            String l = vp1.getTripID();
            String r = vp2.getTripID();
            return l.equals(r) ? 0 : (l.compareTo(r) < 0 ? -1 : 1);
        }
    }



    // 自定义map
    public static class Map extends Mapper<LongWritable, Text, VehiclePair, Text> {
        private final VehiclePair vehicleKey = new VehiclePair();
        private final Text textValue = new Text();
        private  HashMap<String,String> vehicleTypeMap = new HashMap<String, String>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSystem hdfs =  FileSystem.get(context.getConfiguration());
            File tmpVehicleData = new File("/user/root/vehicletype/vehicletype.csv");
            InputStream inputStream = hdfs.open(new Path("hdfs://10.2.9.42:9000/user/root/vehicletype/vehicletype.csv"));

//            FileReader reader = new FileReader(tmpVehicleData);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                String[] lineItems = line.split(",");
                if(lineItems.length==4){
                    String plateAndColor = lineItems[0] + "_"+lineItems[1];
                    String vehicleType = "";
                    if(lineItems[2].equals("999")){
                        vehicleType = lineItems[3];
                    } else{
                        vehicleType = lineItems[2];
                    }

                    vehicleTypeMap.put(plateAndColor,vehicleType);
                }


            }
            br.close();
//            reader.close();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }


        private int getTimePeriod(int hour,int minute){
            return (hour*12)+(minute+1)/5;
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
                String plateAndColor = plate+"_"+plateColor;
                String ts = lss[7];
                String tripId = "";
                        //注意format的格式要与日期String的格式相匹配
                DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    Date backDate = sdf.parse(ts);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(backDate);
                    int timePeriod = getTimePeriod(backDate.getHours(),backDate.getMinutes());
                    String dateStr = ts.substring(0,ts.indexOf(' '));
                    String tmpType = vehicleTypeMap.get(plateAndColor);
                    tripId = dateStr+"_"+timePeriod+"_"+tmpType;


                } catch (ParseException e) {
                    e.printStackTrace();
                }

                vehicleKey.set(tripId, plate);
                textValue.set(line);
                context.write(vehicleKey, textValue);
            }
            // < (苏ES1965,2,320000,999,320500,0), 320000,2014-05-01...>
//            StringTokenizer tokenizer = new StringTokenizer(line);
//            int left = 0;
//            int right = 0;
//            if (tokenizer.hasMoreTokens())
//            {
//                left = Integer.parseInt(tokenizer.nextToken());
//                if (tokenizer.hasMoreTokens())
//                    right = Integer.parseInt(tokenizer.nextToken());
//                intkey.set(left, right);
//                intvalue.set(right);
//                context.write(intkey, intvalue);
//            }
        }
    }



    // 自定义reduce
    //
    public static class Reduce extends Reducer<VehiclePair, Text, Text, Text> {
        private final Text left = new Text();
        private final Text right = new Text();

        //        private static final Text SEPARATOR = new Text("------------------------------------------------");
        //   output :<苏ED5683 , values>
        public void reduce(VehiclePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            context.write(SEPARATOR, null);
            left.set(key.getTripID());
            int count =0;

            for (Text val : values) {
                count++;
            }
            right.set(count+"");
            context.write(left,right);


        }
    }

    private  static  boolean largerThanFiveMinute(Date d1,Date d2){
        if((d1.getTime() - d2.getTime())/1000 > 5*60){
            return true;
        }
        return false;
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

        DistributedCache.createSymlink(conf);
        String vehcleTypeDataPath = "/user/root/vehicletype/vehicletype.csv";
        Path filePath = new Path(vehcleTypeDataPath);
        String uriWithLink = filePath.toUri().toString() + "#" + "vehicletype.csv";
        DistributedCache.addCacheFile(new URI(uriWithLink), conf);

        // 实例化一道作业
        Job job = Job.getInstance(conf, "vehiletype");
        conf.set("mapred.textoutputformat.separator", ",");
        job.setJarByClass(CountVehicleData.class);
        // Mapper类型
        job.setMapperClass(Map.class);
        // 不再需要Combiner类型，因为Combiner的输出类型<Text, IntWritable>对Reduce的输入类型<IntPair, IntWritable>不适用
        //job.setCombinerClass(Reduce.class);
        // Reducer类型
        job.setReducerClass(Reduce.class);
        // 分区函数
        job.setPartitionerClass(FirstPartitioner.class);
        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        // map 输出Key的类型
        job.setMapOutputKeyClass(VehiclePair.class);
        // map输出Value的类型
        job.setMapOutputValueClass(Text.class);
        // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // rduce输出Value的类型
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(24);
        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
        job.setOutputFormatClass(TextOutputFormat.class);
//        Path[] paths = FilesList.getList(conf, args[0], args[1], args[2]);
//        // 输入hdfs路径
//        for (Path path : paths) {
//            FileInputFormat.addInputPath(job, path);
//        }
        Path path = new Path(args[1]);
        FileInputFormat.addInputPath(job, path);

        // 输出hdfs路径
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        // 提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
