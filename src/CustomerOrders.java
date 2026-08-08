import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerOrders {

    public static class OrderMapper extends Mapper<Object, Text, Text, Text> {
        private Text customer = new Text();
        private Text invoice = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("InvoiceNo")) return;

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (fields.length == 8) {
                String customerId = fields[6].replaceAll("\"", "").trim();
                String invoiceNo = fields[0].replaceAll("\"", "").trim();
                
                if (!customerId.isEmpty()) {
                    customer.set(customerId);
                    invoice.set(invoiceNo);
                    context.write(customer, invoice);
                }
            }
        }
    }

    public static class OrderReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> uniqueInvoices = new HashSet<>();
            for (Text val : values) {
                uniqueInvoices.add(val.toString());
            }
            context.write(key, new IntWritable(uniqueInvoices.size()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Orders");
        job.setJarByClass(CustomerOrders.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
