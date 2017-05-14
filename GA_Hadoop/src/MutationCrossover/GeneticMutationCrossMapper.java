package MutationCrossover;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class GeneticMutationCrossMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
	private int TARGET =0;
	   
	public void configure(JobConf job) {
	       
		 TARGET = Integer.parseInt(job.get("target"));
	    }
	
	@Override
	public void map(LongWritable lineNo, Text line, OutputCollector<LongWritable, Text> output, Reporter arg3)
			throws IOException {
		System.out.println("TARGET No "+TARGET);
		StringTokenizer stringTokenizer = new StringTokenizer(line.toString());
		stringTokenizer.nextToken();
		//long newLineNO = lineNo.get() + 1;
		long newLineNO = Long.parseLong(stringTokenizer.nextToken()) + 1;
		System.out.println("Line No "+newLineNO);
		System.out.println("Offset ::"+lineNo);
		long modValue = newLineNO % (40+1);
		long key = modValue;
		if(modValue % 2 == 0){
			key = key -1;
		}
		System.out.println("line "+line.toString());
		output.collect(new LongWritable(key), line);
	}


}
