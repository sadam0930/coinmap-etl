import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CoinMapDataReducer extends Reducer<Text, Text, Text, IntWritable> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String key_col_str[] = key.toString().split(":", -1);
        String column = key_col_str[0];

        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        if (column.equals("created_on")) {
            for(Text value : values) {
                String v_string = value.toString();
                int v = null;
                if(!v_string.equals("")) {
                    try {
                        v = Integer.parseInt(v_string.split("\\.")[0]); //drop trailing .000000    
                    }
                    catch(Exception e) {
                        System.out.println("ERROR: " + v_string);
                        System.out.println("Exception: " + e);
                        continue;
                    }
                }

                min = min(v, min);
                max = max(v, max);
            }

            context.write(new Text("min created_on"), new IntWritable(min));
            context.write(new Text("max created_on"), new IntWritable(max));   
        } else if (column.equals("category")) {
            String cat_name = key_col_str[1]; //if passed by category:cat_name
            if(cat_name.equals("")) { //cat name is in value
                for(value : values) {
                    cat_name = value.toString();
                    cat_name_length = cat_name.length();
                    min = min(cat_name_length, min);
                    max = max(cat_name_length, max);
                }
                context.write(new Text("min category name length"), new IntWritable(min));
                context.write(new Text("max category name length"), new IntWritable(max));
            } else {
                //if passed by category:cat_name
                int sum = 0;
                for(value : values) {
                    sum += Integer.parseInt(value.toString());
                }
                context.write(new Text("category count - " + cat_name), new IntWritable(sum));
            }
        }
        
    }

    public int min(int v, int min){
        if(v != null && v < min) {
            return v;
        }
        return min;
    }

    public int max(int v, int max){
        if(v != null && v > max) {
            return v;
        }
        return max;
    }
}