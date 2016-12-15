package com.booksset;

import java.io.IOException;
import java.util.*;
import java.lang.Iterable;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


public class BooksSet {
	 public static class Map extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable,Text,Text,IntWritable> {
		    private final static IntWritable one = new IntWritable(1);
		//    private Text word = new Text();
		        
			@Override
			public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
					throws IOException {
				
				StringTokenizer stringTokenizer=new StringTokenizer(value.toString(),"\t");
            	for (int j = 0; j<4; j++)
            	{
            		stringTokenizer.nextToken();
				}
            	
				try 
				{
				JSONObject jsonObject=new JSONObject(stringTokenizer.nextToken().toString());
				JSONArray jsonArray=jsonObject.getJSONArray("subjects");
            	  for (int j = 0; j < jsonArray.length(); j++) 
            	  { 
//            		System.out.println("inside each subject "+jsonArray.get(j));  
            		  output.collect(new Text(jsonArray.get(j).toString()), one);
				  }
            	}
            	catch (Exception e)
            	{
					// TODO: handle exception
				}
            	
            }
  
			} 
		        
		 /**
		 * @author chaitu
		 *
		 */
		public static class Reduce extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, FloatWritable>
		 {
			 public static IntWritable avg;
			 public static IntWritable med,mini,maxi;
			 public static int count=0,totalSum=0, min=0,max=0,i=0;
			public static ArrayList<Integer> sumList=new ArrayList<Integer>();
			public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter)
					throws IOException {
				// TODO Auto-generated method stub
		        int sum = 0;
		        i++;
		        int median=0;
		        
		        while(values.hasNext())
		        {
		        	IntWritable i = values.next();
		        	sum+=i.get();
		        }
		        count++;
		        output.collect(key, new FloatWritable(sum));
				totalSum+=sum;
		        sumList.add(sum);
		        Collections.sort(sumList);
		        int size = sumList.size();
		        if(size%2==0)
		        {
		        	int half=size/2;
		        	median = sumList.get(half);
		        }
		        else 
		        {
		           int half = (size+1)/2;
		           median=sumList.get(half-1);
				}
		       
				if(i==1)
				{
					min=sum;
					max=sum;
				}
				if(sum<min)
				{
					min=sum;					
				}
				if(sum>max)
				{
					max=sum;					
				}
				output.collect(new Text("Average is :"),new FloatWritable(totalSum/count));
				output.collect(new Text("Median is  :"),new FloatWritable(median));
				output.collect(new Text("Minimum is :"), new FloatWritable(min));
				output.collect(new Text("Maximum is :"), new FloatWritable(max));	
			}
			
			protected void cleanup(OutputCollector<Text, IntWritable> outputCollector,Reporter reporter) throws IOException, InterruptedException 
   		    {
				

			}  


		 }


	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
	 //   Configuration conf = new Configuration();
        
	    JobClient client = new JobClient();
	    JobConf conf1 = new JobConf(com.booksset.BooksSet.class);
	   //     Job job = new Job(conf1, "BooksSet");
	    
	    conf1.setOutputKeyClass(Text.class);
	    conf1.setOutputValueClass(IntWritable.class);
	        
	    conf1.setMapperClass(Map.class);
	    conf1.setReducerClass(Reduce.class);
	    conf1.setInputFormat( TextInputFormat.class);
	    conf1.setOutputFormat( TextOutputFormat.class);        
	    FileInputFormat.setInputPaths(conf1,new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
	        
	    client.setConf(conf1);
	    try
	    {
	    	   JobClient.runJob(conf1);
	   	
	    }catch(Exception e)
	    {
	    	e.printStackTrace();
	    }
	  }

	}

