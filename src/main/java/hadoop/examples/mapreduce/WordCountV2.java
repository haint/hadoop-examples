/*
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package hadoop.examples.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author <a href="mailto:haithanh0809@gmail.com">Nguyen Thanh Hai</a>
 * @version $Id$
 *
 */
public class WordCountV2 extends Configured implements Tool
{

	public static class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		static enum Counters { INPUT_WORDS }
		
		private final static IntWritable one = new IntWritable(1);
		
		private Text word = new Text();
		
		private boolean caseSensitive = true;
		
		private Set<String> patternsToSkip = new HashSet<String>();
		
		private long numberRecords = 0;

		private String inputFile;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);	
			inputFile = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			if(conf.getBoolean("wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(conf);
				} catch(IOException e) {
					System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(e));
				}
				
				for(Path patternsFile : patternsFiles) {
					parseSkipFile(patternsFile);
				}
			}
		}
		
		private void parseSkipFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				for(String line = fis.readLine(); line != null; line = fis.readLine()) {
					patternsToSkip.add(line);
				}
			} catch(IOException e) {
				System.err.println("Caught exception while parsing the cached file: " + StringUtils.stringifyException(e));
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = caseSensitive ? value.toString() : value.toString().toLowerCase();
			for(String pattern : patternsToSkip) {
				line = line.replaceAll(pattern, "");
			}
			
			StringTokenizer iterator = new StringTokenizer(line);
			while(iterator.hasMoreTokens()) {
				word.set(iterator.nextToken());
				context.write(word, one);
				context.getCounter(Counters.INPUT_WORDS).increment(1);
			}
			
			if(((++numberRecords) % 10) == 0) {
				context.setStatus("Finished processing " + numberRecords + " records from input file " + inputFile);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(Iterator<IntWritable> i = values.iterator(); i.hasNext();) {
				sum += i.next().get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
   public int run(String[] args) throws Exception
   {
   	Configuration conf = getConf();
   	List<String> other_args = new ArrayList<String>();
   	args = new GenericOptionsParser(conf, args).getRemainingArgs();
   	
   	for(int i =0; i < args.length; i++) {
   		if("-skip".equals(args[i])) {
   			DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
   			conf.setBoolean("wordcount.skip.patterns", true);
   		} else if("-D".equals(args[i])) {
   			String[] arr = args[++i].split("=");
   			conf.setBoolean(arr[0], Boolean.valueOf(arr[1]));
   		} else other_args.add(args[i]);
   	}
   	
   	Job job = new Job(conf);
   	job.setJarByClass(WordCountV2.class);
   	job.setJobName("word count version 2");
   	job.setOutputKeyClass(Text.class);
   	job.setOutputValueClass(IntWritable.class);
   	
   	job.setMapperClass(TokenizeMapper.class);
   	job.setCombinerClass(IntSumReducer.class);
   	job.setReducerClass(IntSumReducer.class);
   	
   	job.setInputFormatClass(TextInputFormat.class);
   	job.setOutputFormatClass(TextOutputFormat.class);
   	
   	FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
   	FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
	   return job.waitForCompletion(true) ? 0 : 1;
   }
   
   public static void main(String[] args) throws Exception
   {
	   int res = ToolRunner.run(new WordCountV2(), args);
	   System.exit(res);
   }
}
