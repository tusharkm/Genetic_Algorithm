package MutationCrossover;
import Chromosome.Chromosome;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

//import com.neu.genetic.nayyar.GAMapper;
//import com.neu.genetic.nayyar.GAReducer;

public class GeneticMutationCrossDriver {

	private static final String GENERATION = "generation";
	public static void main(String[] args) {
		
		int i =1;
		String input = args[0];
		while(true){
			JobClient client = new JobClient();
			JobConf conf = new JobConf(GeneticMutationCrossDriver.class);
			conf.setJobName("Genetic Iter_"+(i+1));
			conf.setOutputKeyClass(DoubleWritable.class);
		    conf.setOutputValueClass(Text.class);

		        conf.setMapperClass(GeneticMutationCrossMapper.class);
		        conf.setMapOutputKeyClass(LongWritable.class);
		        conf.setMapOutputValueClass(Text.class);
		        
		        conf.setReducerClass(GeneticMutationCrossReducer.class);
		        
		        conf.set("target", args[2]);    
			conf.set("crossover_rate",".7");
	        conf.set("mutation_rate",".001");
	        conf.set(GENERATION,""+i);
			
			//String previousoutput = (args[1]+(i-1));
			
			Path previousoutputPath = new Path(input+"/part-00000");
			try {
				FileSystem dfs = FileSystem.get(previousoutputPath.toUri(), conf);
				if (dfs.exists(previousoutputPath)) {
					BufferedReader br=new BufferedReader(new InputStreamReader(dfs.open(previousoutputPath)));
                    String line;
                    line=br.readLine();
                    while (line != null){
                            System.out.println(line);
                            line=br.readLine();
                            if(line != null && line.contains(GENERATION)){
                            	return ;
                            }
                    }
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			String output = (args[1]+(i+1));
			
			Path outputPath = new Path(output);
			FileInputFormat.setInputPaths(conf, new Path(input+"/part-00000"));
			FileOutputFormat.setOutputPath(conf, outputPath);

			try {
				FileSystem dfs = FileSystem.get(outputPath.toUri(), conf);
				if (dfs.exists(outputPath)) {
					dfs.delete(outputPath, true);
				}
				Job job = new Job(conf, "Genetic Iter_"+(i+1));
		        //job.setInputFormatClass(PdfInputFormat.class);
		    	int code = job.waitForCompletion(true) ? 0 : 1;
				//System.exit(code);
			} catch (Exception e) {
				e.printStackTrace();
			}
		
			input = output;
			i++;
		}
			

	}

}
