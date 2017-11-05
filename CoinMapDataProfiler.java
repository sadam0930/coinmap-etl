import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class CoinMapDataProfiler {

    public static class CoinMapDataMapper extends Mapper<LongWritable, Text, Text, Text> {
        // Columns of interest:
        private final int CATEGORY = 0;
        private final int CITY = 1;
        private final int COUNTRY = 2;
        private final int CREATED_ON = 3;
        private final int NAME = 12;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(0 != key.get()) { //first line is column header
                String line = value.toString();
                String columns[] = line.split(",", -1);
                Text outputKey = new Text("created_on");
                Text outputValue = new Text(columns[CREATED_ON]);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class CoinMapDataReducer extends Reducer<Text, Text, Text, IntWritable> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;

            for(Text value : values) {
                String v_string = value.toString();
                int v = 0;
                if(v_string != ""){
                    try {
                        v = Integer.parseInt(v_string.split(".")[0]); //drop trailing .000000    
                    }
                    catch {
                        System.out.println("ERROR: " + v_string);
                    }
                }

                if(v < min) {
                    min = v;
                }
                if(v > max) {
                    max = v;
                }
            }

            context.write(new Text("min created_on"), new IntWritable(min));
            context.write(new Text("max created_on"), new IntWritable(max));
        }
    }

    public static void main (String[] args) throws Exception {
        if (args.length < 2){
            System.err.println("Usage: CoinMapDataProfiler <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(CoinMapDataProfiler.class);
        job.setJobName("CoinMap Data Profiler");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1); // 1 Reduce task 
        
        job.setMapperClass(CoinMapDataMapper.class);
        job.setReducerClass(CoinMapDataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}