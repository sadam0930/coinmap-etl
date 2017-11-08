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
            int total_rows = 0;
            for(Text value : values) {
                String v_string = value.toString();
                Integer v = null;
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
                total_rows++;
            }

            context.write(new Text("min created_on"), new IntWritable(min));
            context.write(new Text("max created_on"), new IntWritable(max));   
            context.write(new Text("total rows"), new IntWritable(total_rows));
        } else if (column.equals("category")) {
            String cat_name = key_col_str[1]; //if passed by category:cat_name
            if(cat_name.equals("")) { //cat name is in value
                for(Text value : values) {
                    cat_name = value.toString();
                    int cat_name_length = cat_name.length();
                    min = min(cat_name_length, min);
                    max = max(cat_name_length, max);
                }
                context.write(new Text("min category name length"), new IntWritable(min));
                context.write(new Text("max category name length"), new IntWritable(max));
            } else {
                //if passed by category:cat_name
                int sum = 0;
                for(Text value : values) {
                    sum += Integer.parseInt(value.toString());
                }
                context.write(new Text("category count - " + cat_name), new IntWritable(sum));
            }
        } else if (column.equals("country")) {
            String country_code = key_col_str[1]; //if passed by country:country_code
            int sum = 0;
            for(Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            context.write(new Text("country count - " + country_code), new IntWritable(sum));
        }
        
    }

    public int min(Integer v, int min){
        if(v != null && v < min) {
            return v;
        }
        return min;
    }

    public int max(Integer v, int max){
        if(v != null && v > max) {
            return v;
        }
        return max;
    }
}