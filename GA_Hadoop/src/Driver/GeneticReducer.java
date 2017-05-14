package Driver;

import Chromosome.Chromosome;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GeneticReducer
extends MapReduceBase
implements Reducer<IntWritable, IntWritable, Text, Text> {
    Logger log = Logger.getLogger(GeneticReducer.class.getName());
    private int poolSize;
    private double crossoverRate;
    private double mutationRate;

    public void configure(JobConf job) {
        this.crossoverRate = Double.parseDouble(job.get("crossover_rate"));
        this.mutationRate = Double.parseDouble(job.get("mutation_rate"));
    	this.poolSize = Integer.parseInt(job.get("pool_size"));
   
    }

    public void reduce(IntWritable ip, Iterator<IntWritable> counts, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        boolean totalCount = false;
        int target = 0;
         while (counts.hasNext()) {
            target = Integer.parseInt(counts.next().toString());
        }
        Chromosome chromosome = new Chromosome(target, this.crossoverRate, this.mutationRate);
               
        output.collect(new Text("" + chromosome.score), new Text(String.valueOf(ip.toString()) + " " + chromosome.chromo.toString()));
    }
}