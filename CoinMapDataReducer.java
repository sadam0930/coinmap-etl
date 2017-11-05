import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public static class CoinMapDataReducer extends Reducer<Text, Text, Text, IntWritable> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        for(Text value : values) {
            System.out.println("DEBUG: " + key.toString());
            System.out.println("DEBUG: " + value.toString());
            String v_string = value.toString();
            int v = 0;
            if(v_string != ""){
                try {
                    v = Integer.parseInt(v_string.split(".")[0]); //drop trailing .000000    
                }
                catch(Exception e) {
                    System.out.println("ERROR: " + v_string);
                    System.out.println("Exception: " + e);
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