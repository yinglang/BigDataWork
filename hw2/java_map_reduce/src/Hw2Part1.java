import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jruby.RubyProcess;

import java.io.*;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

/**
 * Created by hui on 18-4-17.
 */
public class Hw2Part1 {

    /**
     * define KEY / OUT integration type
     * KEY type must be WritableComparable
     * OUT MUST be Writable
     */
    static class TextPair implements WritableComparable<TextPair> {

        Text source = new Text();
        Text destination = new Text();

        public Text getSource() {
            return source;
        }

        public void setSource(Text source) {
            this.source = source;
        }

        public Text getDestination() {
            return destination;
        }

        public void setDestination(Text destination) {
            this.destination = destination;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            source.write(out);
            destination.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            source.readFields(in);
            destination.readFields(in);
        }

        @Override
        public int compareTo(TextPair o) {
            int result = source.compareTo(o.source);
            if(result == 0) return destination.compareTo(o.destination);
            else return result;
        }
    }

    static class Result implements Writable{

        IntWritable count = new IntWritable();
        DoubleWritable time = new DoubleWritable();

        public IntWritable getCount() {
            return count;
        }

        public void setCount(IntWritable count) {
            this.count = count;
        }

        public DoubleWritable getTime() {
            return time;
        }

        public void setTime(DoubleWritable time) {
            this.time = time;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            count.write(out);
            time.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count.readFields(in);
            time.readFields(in);
        }
    }

    /**
     * def Map Reduce function
     * */
    public static class DealMapper
            extends Mapper<Object, Text, TextPair, Result>{
        private final static TextPair textPair = new TextPair();
        private final static Result result = new Result();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            if(itr.countTokens() != 3) {
                System.err.println("[Warning]: wrong format input: " + value.toString());
                return;
            }
            try {
                textPair.getSource().set(itr.nextToken());
                textPair.getDestination().set(itr.nextToken());
                result.getCount().set(1);
                result.getTime().set(Double.parseDouble(itr.nextToken()));
                context.write(textPair, result);
            }catch (Exception e){
                System.err.println("[Warning]: wrong format input: " + value.toString());
                System.err.println(e.toString());
            }

//            String[] res = value.toString().split(" ");
//            try{
//            if(res.length != 3) {
//                System.err.println("[Warning]: wrong format input: " + value.toString());
//                return;
//            }
//            textPair.getSource().set(res[0]);
//            textPair.getDestination().set(res[1]);
//            result.getCount().set(1);
//            result.getTime().set(Double.parseDouble(res[2]));
//            context.write(textPair, result);
//            }catch (Exception e) {
//                System.err.println("[Warning]: wrong format input: " + value.toString());
//                System.err.println(e.toString());
//            }
        }
    }

    public static class DealCombiner extends Reducer<TextPair, Result, TextPair, Result>{
        private final static Result sumResult = new Result();
        public void reduce(TextPair key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
            int sumCount = 0;
            double sumtime = 0;
            for(Result value: values){
                sumCount += value.getCount().get();
                sumtime += value.getTime().get();
            }
            sumResult.getCount().set(sumCount);
            sumResult.getTime().set(sumtime);
            context.write(key, sumResult);
        }
    }

    public static class DealReducer extends Reducer<TextPair, Result, Text, Text>{
        private final static Text result = new Text();
        private final static Text key = new Text();
        private final static DecimalFormat df = new DecimalFormat("#.000");
        public void reduce(TextPair key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
            Integer sumCount = 0;
            Double sumTime = 0.;
            for(Result value: values){
                sumCount += value.getCount().get();
                sumTime += value.getTime().get();
            }
            Double avgTime = sumTime / sumCount;
            // better to use Text append function to add String
            DealReducer.key.set(key.getSource().toString() + " " + key.getDestination().toString());
            result.set(sumCount.toString() + " " + df.format(avgTime));

            context.write(DealReducer.key, result);
        }
    }

    /**
     * def main for conf info to JobTasker
     * */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("Hw2Part1 <input file> <output directory>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "hw2");

        job.setJarByClass(Hw2Part1.class);  // main func

        job.setMapperClass(DealMapper.class);
        job.setCombinerClass(DealCombiner.class);
        job.setReducerClass(DealReducer.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Result.class);

        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(Text.class);


        // set input file
        for(int i = 0; i < otherArgs.length - 1; i++){
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // set output file, if exist then delete it first.
        FileSystem fs = FileSystem.get(URI.create(otherArgs[otherArgs.length-1]), conf);
        Path path = new Path(otherArgs[otherArgs.length-1]);
        if(fs.exists(path)){
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);

        int exit_id = job.waitForCompletion(true) ? 0 : 1;
        showOutput(fs, otherArgs[otherArgs.length-1] + "/part-r-00000");
        fs.close();

        System.exit(exit_id);
    }

    public static void showOutput(FileSystem fs, String file) throws IOException {
        Path path = new Path(file);
        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
        String s;
        while((s=in.readLine()) != null){
            System.out.println(s);
        }
        in.close();
    }
}
