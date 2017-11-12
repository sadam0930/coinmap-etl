import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;

public class CoinMapDataReducer extends Reducer<Text, Text, Text, Text> {
    // Columns of interest:
    private final int CATEGORY = 0;
    private final int CITY = 1;
    private final int COUNTRY = 2;
    private final int CREATED_ON = 3;
    private final int NAME = 4;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value : values) {
            String line = value.toString();
            String columns[] = line.split(",", -1);
            if(columns[CATEGORY].equals("ATM")) {
                columns[CATEGORY] = "atm";
            }
            if(columns[COUNTRY].equals("China")) {
                columns[COUNTRY] = "CN";
            } else if(columns[COUNTRY].equals("Vietnam")) {
                columns[COUNTRY] = "VN";
            }

            try {
                //convert timestamp to date
                Timestamp timestamp = new Timestamp(Long.parseLong(columns[CREATED_ON]));
                Date date = new Date(timestamp.getTime() * 1000L);
                columns[CREATED_ON] = new SimpleDateFormat("yyyy-MM-dd").format(date);;
            } catch (Exception e) {
                System.out.println("ERROR: " + columns[CREATED_ON]);
                System.out.println("Exception: " + e);
                continue;
            }
            
            String output = toCSV(columns);
            context.write(new Text(output), new Text(""));
        }
    }

    public static String toCSV(String[] array) {
        String result = "";

        if (array.length > 0) {
            StringBuilder sb = new StringBuilder();

            for (String s : array) {
                sb.append(s).append(",");
            }

            result = sb.deleteCharAt(sb.length() - 1).toString();
        }
        return result;
    }

}