/*     */ package com.asiainfo.local.cmc.sd;
/*     */ 
/*     */ import java.io.BufferedReader;
/*     */ import java.io.IOException;
/*     */ import java.io.InputStreamReader;
/*     */ import java.io.PrintStream;
/*     */ import org.apache.hadoop.conf.Configuration;
/*     */ import org.apache.hadoop.fs.FSDataInputStream;
/*     */ import org.apache.hadoop.fs.FileStatus;
/*     */ import org.apache.hadoop.fs.FileSystem;
/*     */ import org.apache.hadoop.fs.Path;
/*     */ import org.apache.hadoop.io.IntWritable;
/*     */ import org.apache.hadoop.io.NullWritable;
/*     */ import org.apache.hadoop.io.Text;
/*     */ import org.apache.hadoop.mapreduce.Counter;
/*     */ import org.apache.hadoop.mapreduce.Mapper;
/*     */ import org.apache.hadoop.mapreduce.Mapper.Context;
/*     */ 
/*     */ class ReadFileSizeMapper extends Mapper<IntWritable, Text, IntWritable, NullWritable>
/*     */ {
/* 776 */   Configuration conf = null;
/*     */ 
/*     */   protected void setup(Mapper.Context context)
/*     */     throws IOException, InterruptedException
/*     */   {
/* 781 */     this.conf = context.getConfiguration();
/*     */   }
/*     */ 
/*     */   public void map(IntWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException
/*     */   {
/* 786 */     System.out.println("[ReadFileSizeMapper][map] start handle file[" + value.toString() + "]");
/* 787 */     FileSystem fs = FileSystem.newInstance(this.conf);
/* 788 */     Path path = new Path(value.toString());
/* 789 */     FileStatus fstatus = fs.getFileStatus(path);
/* 790 */     long fileSize = fstatus.getLen();
/* 791 */     long fileLine = 0L;
/*     */ 
/* 793 */     FSDataInputStream fsis = fs.open(path);
/* 794 */     BufferedReader br = new BufferedReader(new InputStreamReader(fsis));
/* 795 */     String line = null;
/* 796 */     while ((line = br.readLine()) != null) {
/* 797 */       fileLine += 1L;
/*     */     }
/*     */ 
/* 800 */     context.getCounter("file_LineNum_Group", path.getName() + "_size").setValue(fileSize);
/* 801 */     context.getCounter("file_LineNum_Group", path.getName() + "_line").setValue(fileLine);
/*     */   }
/*     */ }

/* Location:           C:\Users\chenlianguo\Desktop\云经分\ocetl_local_cmc_sd.jar
 * Qualified Name:     com.asiainfo.local.cmc.sd.ReadFileSizeMapper
 * JD-Core Version:    0.6.0
 */