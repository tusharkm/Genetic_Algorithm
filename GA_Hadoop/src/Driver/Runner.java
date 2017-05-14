package Driver;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;




public class Runner {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Runner.class);
		conf.setJobName("Genetic_Algorithm_Driver");
		conf.set("target", args[2]);
		conf.set("pool_size", args[3]);
		conf.set("crossover_rate", args[4]);
		conf.set("mutation_rate", args[5]);
		
		//conf.setPartitionerClass(CaderPartitioner.class);
		
		conf.setMapperClass(GeneticMapper.class);
		conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setReducerClass(GeneticReducer.class);	
		Path outputPath = new Path(args[1]);

		// take the input and output from the command line
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, outputPath);

		try {
			FileSystem dfs = FileSystem.get(outputPath.toUri(), conf);
			if (dfs.exists(outputPath)) {
				dfs.delete(outputPath, true);
			}
			Job job = new Job(conf, "Genetic_Algorithm_Driver");
	       // job.setSortComparatorClass(DescendingKeyComparator.class);
	    	int code = job.waitForCompletion(true) ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
