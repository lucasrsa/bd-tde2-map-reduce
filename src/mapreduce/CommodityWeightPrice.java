package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

// Questão 6
public class CommodityWeightPrice {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "commodity-weight-price");

        // Registrando classes
        j.setJarByClass(CommodityWeightPrice.class);
        j.setMapperClass(MapForCommodityCount.class);
//        j.setCombinerClass(ReduceForCommodityCount.class);
        j.setReducerClass(ReduceForCommodityCount.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(CommodityCostWeightWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForCommodityCount extends Mapper<LongWritable, Text, Text, CommodityCostWeightWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            final String commodity = value.toString().split(";")[3];
            final String cost = value.toString().split(";")[5];
            final String weight = value.toString().split(";")[6];

            // Checagem para ignorar cabeçalho
            if ( !(key.get() == 0 && value.toString().contains("country_or_area")) && (!cost.isEmpty()) && (!weight.isEmpty()) ) {

                con.write(new Text(commodity), new CommodityCostWeightWritable(Float.parseFloat(cost), Float.parseFloat(weight)));

            }

        }

    }

    public static class ReduceForCommodityCount extends Reducer<Text, CommodityCostWeightWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<CommodityCostWeightWritable> values, Context con)
                throws IOException, InterruptedException {

            float costSum = 0;
            float weightSum = 0;

            for (CommodityCostWeightWritable i : values){
                costSum = i.getCost();
                weightSum = i.getWeight();
            }

            con.write(word, new FloatWritable(costSum/weightSum));

        }
    }

}
