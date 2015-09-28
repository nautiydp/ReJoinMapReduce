package com;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RJoin {

	
	//Mapper
	//Input key = 0 ,value = 0001,01-20-2013,400100,100,Exercise & Fitness,clarksville,credit
	//Output key = 400100  value = txn\t100
	public static class TxnMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text outkey = new Text();
		Text outvalue = new Text();
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String[] columns = value.toString().split(",");
			outkey.set(columns[2]);
			outvalue.set("txn\t"+columns[3]);
			context.write(outkey, outvalue);
		}
	}
	
	//Input key = 0 ,value = 400100,jaymin,jaymin@someplace.com
	//Output key = 400100  value = jaymin@someplace.com
	public static class UserMapper	extends Mapper<LongWritable, Text, Text, Text>{
		Text outkey = new Text();
		Text outvalue = new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] columns = value.toString().split(",");
			outkey.set(columns[0]);
			outvalue.set("cust\t"+columns[2]);
			context.write(outkey, outvalue);
		}
	}
	
	//Reducer
	//Input key = 400100 ,value = (txn\t100,txn\t2000,cust\tjaymin@someplace.com)
	//Output key = 400100  value = (email = jaymin@someplace.com & max amount = 2000)
	public static class RJoinReducer extends Reducer<Text,Text,Text,Text>{
		Text outvalue = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			String email = null;
			int max = 0;
			for(Text value : values){
				String[] parts = value.toString().split("\t");
				if("cust".equals(parts[0])){
					email = parts[1];
				}else if("txn".equals(parts[0])){
					if(max < Integer.parseInt(parts[1])){
						max = Integer.parseInt(parts[1]);
					}
				}
			}
			outvalue.set("email = "+email + " & max amount = "+max);
			context.write(key, outvalue);
		}
	}
	//Driver
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Reduce side join Example for Movie Review");
	    
	    job.setJarByClass( RJoin.class );
	    job.setReducerClass( RJoinReducer.class );
	    job.setOutputKeyClass( Text.class );	    
	    job.setOutputValueClass( Text.class );
	    
	    MultipleInputs.addInputPath(job, new Path( args[0] ), TextInputFormat.class, TxnMapper.class);
	    MultipleInputs.addInputPath(job, new Path( args[1] ), TextInputFormat.class, UserMapper.class);
	    
	    Path outputPath = new Path(args[2]);
	    FileOutputFormat.setOutputPath(job, outputPath);
	    System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	}

}

