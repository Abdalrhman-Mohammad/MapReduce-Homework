package sportRetailer;
import java.io.IOException;
// import java.util.StringTokenizer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class sportRetailerClass {
  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, DoubleWritable> {
    private DoubleWritable SalesAmount = new DoubleWritable(0);
    private Text word = new Text();
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      String op[] = line.split(",");
      String Retailer = op[0].trim();
      String City = op[2].trim();
      String Price_per_Unit = op[4].trim().substring(1);
      String Units_Sold = op[5].trim();
      // the last cloumn have commas "," instead of "." and the below code to
      // solve that
      for (int i = 6; i < op.length; i++) {
        Units_Sold += op[i].trim();
      }
      //
      double SalesAmountInner =
          Double.parseDouble(Price_per_Unit) * Double.parseDouble(Units_Sold);
      SalesAmount = new DoubleWritable(SalesAmountInner);
      word.set(Retailer + "," + City + "#");
      context.write(word, SalesAmount);
    }
  }
  public static class DoubleSumReducer
      extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context)
        throws IOException, InterruptedException {
      double sum = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  public static class SwapMapper
      extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable TotalSalesAmound = new DoubleWritable();
    private Text RetailerAndCity = new Text();
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      String op[] = line.split("#");
      System.out.println(op[1].trim());
      System.out.println(op[0].trim());
      TotalSalesAmound = new DoubleWritable(-Double.parseDouble(op[1].trim()));
      RetailerAndCity = new Text(op[0].trim());
      context.write(TotalSalesAmound, RetailerAndCity);
    }
  }
  public static class lastReducer
      extends Reducer<DoubleWritable, Text, Text, Text> {
    // private DoubleWritable result = new DoubleWritable();
    public void reduce(DoubleWritable key, Iterable<Text> values,
                       Context context)
        throws IOException, InterruptedException {
      Text RetailerAndCity = new Text();
      for (Text val : values) {
        key.set(key.get() * -1);
        RetailerAndCity = val;
        context.write(RetailerAndCity, new Text("$" + key));
      }
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Sport Retailer Job 1");
    job.setJarByClass(sportRetailerClass.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(DoubleSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "Sport Retailer Job 2");
    job1.setJarByClass(sportRetailerClass.class);
    job1.setMapperClass(SwapMapper.class);
    job1.setReducerClass(lastReducer.class);
    job1.setMapOutputKeyClass(DoubleWritable.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(args[1]));
    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
    System.exit(
        job.waitForCompletion(true) && job1.waitForCompletion(true) ? 0 : 1);
  }
}
