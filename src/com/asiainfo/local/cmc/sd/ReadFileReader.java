 package com.asiainfo.local.cmc.sd;
 
 import java.io.IOException;
 import java.io.PrintStream;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.InputSplit;
 import org.apache.hadoop.mapreduce.RecordReader;
 import org.apache.hadoop.mapreduce.TaskAttemptContext;
 
 class ReadFileReader extends RecordReader<IntWritable, Text>
 {
   private IntWritable key = null;
   private Text value = null;
   private int pos = 0;
   private int length = 0;
   private String[] filesArray = null;
 
   public void initialize(InputSplit split, TaskAttemptContext context)
   {
     this.filesArray = ((ReadFileSplit)split).filesArray;
     this.length = this.filesArray.length;
     System.out.println("[ReadFileSizeInputFormat][initialize] get filearray from split, size=[" + this.filesArray.length + "]");
   }
 
   public boolean nextKeyValue()
     throws IOException
   {
     if (this.pos < this.length) {
       this.key = new IntWritable(this.pos + 1);
       this.value = new Text(this.filesArray[this.pos]);
       this.pos += 1;
       return true;
     }
     return false;
   }
 
   public IntWritable getCurrentKey()
   {
     return this.key;
   }
 
   public Text getCurrentValue()
   {
     return this.value;
   }
 
   public void close()
     throws IOException
   {
   }
 
   public float getProgress()
     throws IOException
   {
     return this.pos == this.length ? 100 : this.pos / this.length;
   }
 }