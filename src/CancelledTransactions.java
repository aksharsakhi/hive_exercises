import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CancelledTransactions {

    public static class CancelledMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text cancelledKey = new Text("Cancelled Transactions");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("InvoiceNo")) return;

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (fields.length > 0) {
                String invoiceNo = fields[0].replaceAll("\"", "").trim();
                if (invoiceNo.startsWith("C") || invoiceNo.startsWith("c")) {
                    // It's a cancelled transaction, however a single invoice can have multiple rows.
                    // To count unique cancelled invoices, we'd emit InvoiceNo and count distinct.
                    // We'll emit the invoice number to the reducer to get unique counts.
                    context.write(new Text(invoiceNo), one);
                }
            }
        }
    }

    public static class UniqueCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int totalCancelled = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            totalCancelled++; // Each unique key is a unique invoice
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Total Cancelled Orders:"), new IntWritable(totalCancelled));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cancelled Transactions");
        job.setJarByClass(CancelledTransactions.class);
        job.setMapperClass(CancelledMapper.class);
        job.setReducerClass(UniqueCountReducer.class);
        job.setNumReduceTasks(1); // Single reducer to sum up total unique
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
