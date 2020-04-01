package mapreduce;

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

import java.awt.*;
import java.io.IOException;

// Questão 3
public class HighestTradeBrazil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "highest-trade-brazil");

        // Registrando classes
        j.setJarByClass(HighestTradeBrazil.class);
        j.setMapperClass(MapForCommodityCount.class);
//        j.setCombinerClass(ReduceForCommodityCount.class);
        j.setReducerClass(ReduceForCommodityCount.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(CommodityQtdWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForCommodityCount extends Mapper<LongWritable, Text, Text, CommodityQtdWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            final String country = value.toString().split(";")[0];
            final String year = value.toString().split(";")[1];
            final String commodity = value.toString().split(";")[3];
            final String flow = value.toString().split(";")[4];
            final String quantity = value.toString().split(";")[8];

            if ( ( year.compareTo("2016") == 0 ) && ( country.compareTo("Brazil") == 0 ) && ( !quantity.isEmpty() ) ) {

                con.write(new Text(flow), new CommodityQtdWritable(commodity, Float.parseFloat(quantity)));

            }

        }

    }

    public static class ReduceForCommodityCount extends Reducer<Text, CommodityQtdWritable, Text, Text> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<CommodityQtdWritable> values, Context con)
                throws IOException, InterruptedException {

            String maxName = "";
            float maxN = 0;

            for (CommodityQtdWritable i : values){
                if (maxN < i.getQuantity()) {
                    maxName = i.getCommodity();
                    maxN = i.getQuantity();
                }
            }

            con.write(word, new Text(maxName));

        }
    }

}
