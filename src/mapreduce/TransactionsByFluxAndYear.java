package mapreduce;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
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

// Questão 7
public class TransactionsByFluxAndYear {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "transactions-by-flux-year");

        // Registrando classes
        j.setJarByClass(TransactionsByFluxAndYear.class);
        j.setMapperClass(MapForTradeCount.class);
        j.setReducerClass(ReduceForTradeCount.class);
        j.setCombinerClass(ReduceForTradeCount.class);

        j.setOutputKeyClass(FluxYearWritableComparable.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForTradeCount extends Mapper<LongWritable, Text, FluxYearWritableComparable, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            final String year = value.toString().split(";")[1];
            final String flux = value.toString().split(";")[4];

            con.write(new FluxYearWritableComparable(year, flux), new IntWritable(1));
        }

    }

    public static class ReduceForTradeCount extends Reducer<FluxYearWritableComparable, IntWritable, FluxYearWritableComparable, IntWritable> {

        // Funcao de reduce
        public void reduce(FluxYearWritableComparable word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable i : values){
                sum += i.get();
            }

            con.write(word, new IntWritable(sum));

        }
    }

}

