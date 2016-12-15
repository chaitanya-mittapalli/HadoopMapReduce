package com.booksset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

public class BookSet
{
	public static class SOTopTenMapper extends Mapper<Object, Text, Text, IntWritable> 
	{
		// Our output key and value Writables
		private final static IntWritable one = new IntWritable(1);
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
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
					context.write(new Text(jsonArray.get(j).toString()), one);
				}
			}
			catch (Exception e)
			{
				// TODO: handle exception
			}
		}
	}
	public static class SOTopTenReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public static IntWritable avg;
		public static IntWritable med,mini,maxi;
		public static int count=0,totalSum=0, min=0,max=0,i=0;
		public static ArrayList<Integer> sumList=new ArrayList<Integer>();
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			i++;
			int median=0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}		      
			count++;
			context.write(key, new IntWritable(sum));
			totalSum+=sum;
			sumList.add(sum);
			Collections.sort(sumList);
			int size = sumList.size();
			if(size%2==0)
			{
				int half=size/2;
				med = new IntWritable(sumList.get(half));
			}
			else 
			{
				int half = (size+1)/2;
				med = new IntWritable(sumList.get(half-1));
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
			avg=new IntWritable(totalSum/count);

		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{

			context.write(new Text("Average is :"),avg);
			context.write(new Text("Median is  :"),med);
			context.write(new Text("Minimum is :"),new IntWritable(min));
			context.write(new Text("Maximum is :"),new IntWritable(max));	
		}

	}



	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Top Ten Users by Reputation");
		job.setJarByClass(BookSet.class);
		job.setMapperClass(SOTopTenMapper.class);
		job.setReducerClass(SOTopTenReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
