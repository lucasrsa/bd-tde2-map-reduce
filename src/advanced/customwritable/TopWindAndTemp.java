package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TopWindAndTemp {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire-estudante");

        j.setJarByClass(TopWindAndTemp.class);
        j.setMapperClass(MapForMax.class);
        j.setCombinerClass(ReduceForMax.class);
        j.setReducerClass(ReduceForMax.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForMax extends Mapper<LongWritable, Text, Text, FloatWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            con.write(new Text("wind"),
                    new FloatWritable(Float.parseFloat(value.toString().split(",")[10]))
            );
            con.write(new Text("temp"),
                    new FloatWritable(Float.parseFloat(value.toString().split(",")[8]))
            );

        }

    }

    public static class ReduceForMax extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {
            Float max = 0.0f;

            for(FloatWritable val : values){
                if (val.get() > max) {
                    max = val.get();
                }
            }

            con.write(word, new FloatWritable(max));
        }
    }

    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            con.write(new Text("media"),
                    new FireAvgTempWritable(1, Float.parseFloat(value.toString().split(",")[8]))
            );
        }
    }

    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {
            int sumN = 0;
            Float sumVle = 0.0f;

            for(FireAvgTempWritable obj : values){
                sumN += obj.getN();
                sumVle += obj.getValue();
            }

            con.write(word, new FireAvgTempWritable(sumN, sumVle));
        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {
            Float sumN = 0.0f;
            Float sumVle = 0.0f;

            for(FireAvgTempWritable obj : values){
                sumN += obj.getN();
                sumVle += obj.getValue();
            }

            con.write(word, new FloatWritable(sumVle/sumN));
        }
    }

}
