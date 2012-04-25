package HBaseIA.Ch03;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TimeSpent {

    public static class Map extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, LongWritable> {

        private static final String splitRE = "\\W+";
        private Text user = new Text();
        private LongWritable time = new LongWritable();

        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, LongWritable> output,
                        Reporter reporter) throws IOException {
            String line = value.toString();
            String[] splits = line.split(splitRE);
            if(null == splits || splits.length < 8)
                return;

            user.set(splits[5]);
            time.set(new Long(splits[7].substring(0, splits[7].length()-1)));
            output.collect(user, time);
        }
    }

    public static class Reduce extends MapReduceBase
        implements Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterator<LongWritable> values,
                           OutputCollector<Text, LongWritable> output,
                           Reporter reporter) throws IOException {
            long sum = 0;
            while(values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(TimeSpent.class);
        conf.setJobName("timespent");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
