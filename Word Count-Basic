# Word-Count
import java.io.IOException;

import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Wordcount_HW1_Sajid3138 {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> 
{
private final static IntWritable one = new IntWritable(1);
    
private Text word = new Text();   
    

public static void main(String[] args) throws Exception 
    
    
{ Configuration conf = new Configuration();  
    
conf.set("stop.words", "1,2,3,4,5,6,7,8,9,0,!,@,#,$,%,^,&,*,(,),-,_,=,+,[,{,],},:,<,>,/,?,`,~,.");
}
    
    

public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    
{StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());      
     
    
while (itr.hasMoreTokens())  
    
{word.set(itr.nextToken());
        
      
String wordx=word.toString();
      
if(wordx.length()>=2)
 {char wordy=wordx.charAt(1);
      
if(Character.isLetter(wordy)){wordx= String.valueOf(wordy);
      
word.set(wordx);
      
context.write(word, one);}}}
    
}
  }


 
public static class IntSumReducer
 extends Reducer<Text,IntWritable,Text,IntWritable> 
{
    private IntWritable result = new IntWritable();
    
public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException 
    
{int sum = 0; for (IntWritable val : values) {sum += val.get();}
     
result.set(sum);
 context.write(key, result);
    
}
  }
  
  

public static void main(String[] args) throws Exception 
{
Configuration conf = new Configuration();
    
Job job = Job.getInstance(conf, "Word Count HW1");
    
job.setJarByClass(Wordcount_HW1_Sajid3138.class);
    
job.setMapperClass(TokenizerMapper.class);
    
job.setCombinerClass(IntSumReducer.class);
    
job.setReducerClass(IntSumReducer.class);
    
job.setOutputKeyClass(Text.class);
    
job.setOutputValueClass(IntWritable.class);
    
FileInputFormat.addInputPath(job, new Path(args[0]));
    
FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
