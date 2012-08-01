package HBaseIA.TwitBase.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TimeSpent {

  public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final String splitRE = "\\W+";
    private Text user = new Text();
    private LongWritable time = new LongWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] splits = line.split(splitRE);
      if(null == splits || splits.length < 8)
        return;

      user.set(splits[5]);
      time.set(new Long(splits[7].substring(0, splits[7].length()-1)));
      context.write(user, time);
    }
  }

  public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      long sum = 0;
      for(LongWritable time : values) {
        sum += time.get();
      }
      context.write(key, new LongWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      String usage =
        "TimeSpent is the log processing example app used in " +
        "Chapter 03 to demonstrate a MapReduce application.\n" +
        "Usage:\n" +
        "  TimeSpent path/to/input path/to/output\n";
      System.out.print(usage);
      System.exit(1);
    }

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    Configuration conf = new Configuration();
    Job job = new Job(conf, "TimeSpent");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    FileSystem fs = outputPath.getFileSystem(conf);
    if (fs.exists(outputPath)) {
      System.out.println("Deleting output path before proceeding.");
      fs.delete(outputPath, true);
    }

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
