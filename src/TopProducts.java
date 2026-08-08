import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopProducts {

    public static class QuantityMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text product = new Text();
        private IntWritable qty = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("InvoiceNo")) return;

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (fields.length == 8) {
                try {
                    product.set(fields[2].replaceAll("\"", "").trim()); // Description
                    int quantity = Integer.parseInt(fields[3].trim());
                    qty.set(quantity);
                    if (product.getLength() > 0) {
                        context.write(product, qty);
                    }
                } catch (NumberFormatException e) {}
            }
        }
    }

    public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, String> topProducts = new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            topProducts.put(sum, key.toString());
            if (topProducts.size() > 10) {
                topProducts.remove(topProducts.firstKey());
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer sum : topProducts.descendingKeySet()) {
                context.write(new Text(topProducts.get(sum)), new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Products");
        job.setJarByClass(TopProducts.class);
        job.setMapperClass(QuantityMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1); // Force single reducer for global top 10
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
