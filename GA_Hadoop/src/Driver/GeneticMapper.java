package Driver;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper that takes a line from an Apache access log and emits the IP with a
 * count of 1. This can be used to count the number of times that a host has hit
 * a website.
 */
public class GeneticMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {


	Logger loggger = Logger.getLogger("GeneticPDADriverMapper");
	private double crossoverRate;
	private double mutationRate;
	private int poolSize;
	private int target;
	
	@Override
	public void configure(JobConf job) {
		poolSize = Integer.parseInt(job.get("pool_size"));
		crossoverRate = Double.parseDouble(job.get("crossover_rate"));
		mutationRate = Double.parseDouble(job.get("mutation_rate"));
		target = Integer.parseInt(job.get("target"));
	}
	
	private static final IntWritable one = new IntWritable(1);
	@Override
	public void map(LongWritable fileOffset, Text lineContents, OutputCollector<IntWritable, IntWritable> output,
			Reporter reporter) throws IOException {

		
		StringTokenizer stringTokenizer = new StringTokenizer(lineContents.toString());
		
		int word =0;
		
		String tempkey = lineContents.toString();
		for(int i =0; i< poolSize;i++){
			output.collect( new IntWritable(i),new IntWritable(target));
		}
	}


}
