import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CoinMapDataCleaner {

    public static void main (String[] args) throws Exception {
        if (args.length < 2){
            System.err.println("Usage: CoinMapDataCleaner <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(CoinMapDataCleaner.class);
        job.setJobName("CoinMap Data Cleaner");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1); // 1 Reduce task 
        
        job.setMapperClass(CoinMapDataMapper.class);
        job.setReducerClass(CoinMapDataReducer.class);

        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}