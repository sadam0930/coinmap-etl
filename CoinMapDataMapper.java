import org.apache.hadoop.io.Text;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CoinMapDataMapper extends Mapper<LongWritable, Text, Text, Text> {
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