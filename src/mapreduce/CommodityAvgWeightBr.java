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

public class CommodityAvgWeightBr {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "commodity-avg-weight-br");

        // Registrando classes
        j.setJarByClass(CommodityAvgWeightBr.class);
        j.setMapperClass(MapForCommodityWeight.class);
//        j.setCombinerClass(ReduceForCommodityWeight.class);
        j.setReducerClass(ReduceForCommodityWeight.class);

        j.setOutputKeyClass(CommodityYearWritableComparable.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForCommodityWeight extends Mapper<LongWritable, Text, CommodityYearWritableComparable, FloatWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Checagem para ignorar cabeçalho
            if ( !(key.get() == 0 && value.toString().contains("country_or_area")) ) {

                final String country = value.toString().split(";")[0];
                final String year = value.toString().split(";")[1];
                final String commodity = value.toString().split(";")[3];
                final String weight = value.toString().split(";")[6];

                if (country.compareTo("Brazil") == 0 && !weight.isEmpty()) {

                    con.write(new CommodityYearWritableComparable(commodity, year), new FloatWritable(Float.parseFloat(weight)));

                }
            }
        }

    }

    public static class ReduceForCommodityWeight extends Reducer<CommodityYearWritableComparable, FloatWritable, CommodityYearWritableComparable, FloatWritable> {

        // Funcao de reduce
        public void reduce(CommodityYearWritableComparable word, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {

            int count = 0;
            float sum = 0;

            for (FloatWritable i : values){
                count += 1;
                sum += i.get();
            }

            con.write(word, new FloatWritable(sum / count));

        }
    }

}
