package MutationCrossover;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import Chromosome.Chromosome;

import Chromosome.Chromosome;


public class GeneticMutationCrossReducer extends MapReduceBase
implements Reducer<LongWritable, Text, DoubleWritable, Text> {

	private  int TARGET;
	private double generations;
	private double crossoverRate;
	private double mutationRate;
	static int chromoLen = 5;
	StringBuffer chromo = new StringBuffer(chromoLen * 4);
	
	
	@Override
	public void configure(JobConf job) {
		generations = Double.parseDouble(job.get("generation"));
		crossoverRate = Double.parseDouble(job.get("crossover_rate"));
		mutationRate = Double.parseDouble(job.get("mutation_rate"));
		TARGET = Integer.parseInt(job.get("target"));
	}

	@Override
	public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter arg3)
			throws IOException {
		
		if(values != null )
		{
			String chromosome_1 = null;
			String chromosome_2 = null;
	
			if(values.hasNext())
				chromosome_1 = values.next().toString();
			
			if(values.hasNext())
				chromosome_2 = values.next().toString();
			
			System.out.println("chromosome_1 "+chromosome_2.toString());
			System.out.println("chromosome_2 "+chromosome_2.toString());
		// get the chromosome from the op of mapper		
			StringTokenizer stringTokenizer = new StringTokenizer(chromosome_1.toString());
			
			Chromosome chromosome_new1 = new Chromosome(TARGET,crossoverRate,mutationRate);
			
			double score = Double.parseDouble(stringTokenizer.nextToken());
			
			long keyChromosome1 = Long.parseLong(stringTokenizer.nextToken());
			
			String chromo_1 = stringTokenizer.nextToken();
			
			chromosome_new1.score = score;
			
			chromosome_new1.chromo = new StringBuffer(chromo_1);
			
			
			stringTokenizer = new StringTokenizer(chromosome_2.toString());
			
			Chromosome chromosome_new2 = new Chromosome(TARGET,crossoverRate,mutationRate);
			
			score = Double.parseDouble(stringTokenizer.nextToken());
			
			long keyChromosome2 = Long.parseLong(stringTokenizer.nextToken());
			
			String chromo_2 = stringTokenizer.nextToken();
			
			chromosome_new2.score = score;
			
			chromosome_new2.chromo = new StringBuffer(chromo_2);
				
			// Crossover the chromosomes
			chromosome_new1.crossOver(chromosome_new2);
			
			// Mutate the chromosomes
			chromosome_new1.mutate();
			
			chromosome_new2.mutate();

			// Rescore the chromosomes
			
			chromosome_new2.scoreChromo(TARGET);
			
			chromosome_new2.scoreChromo(TARGET);

			// Check to see if either is the solution
			if (chromosome_new1.total == TARGET && chromosome_new1.isValid()) {
				output.collect(new DoubleWritable(chromosome_new1.score), new Text("generation : "+generations +
						" Solution: "+chromosome_new1.decodeChromo()));
				return;
			}
			if (chromosome_new2.total == TARGET && chromosome_new2.isValid()) {
				output.collect(new DoubleWritable(chromosome_new2.score), new Text("generation : "+generations +
						" Solution: "+chromosome_new2.decodeChromo()));
				return;
			}
			
			output.collect(new DoubleWritable(chromosome_new1.score), new Text(""+keyChromosome1+" "+chromosome_new1.chromo));
			output.collect(new DoubleWritable(chromosome_new2.score), new Text(""+keyChromosome2+" "+chromosome_new2.chromo));
		}
		
	}
	
	

}
