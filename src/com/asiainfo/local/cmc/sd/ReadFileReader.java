/*     */ package com.asiainfo.local.cmc.sd;
/*     */ 
/*     */ import java.io.IOException;
/*     */ import java.io.PrintStream;
/*     */ import org.apache.hadoop.io.IntWritable;
/*     */ import org.apache.hadoop.io.Text;
/*     */ import org.apache.hadoop.mapreduce.InputSplit;
/*     */ import org.apache.hadoop.mapreduce.RecordReader;
/*     */ import org.apache.hadoop.mapreduce.TaskAttemptContext;
/*     */ 
/*     */ class ReadFileReader extends RecordReader<IntWritable, Text>
/*     */ {
/* 672 */   private IntWritable key = null;
/* 673 */   private Text value = null;
/* 674 */   private int pos = 0;
/* 675 */   private int length = 0;
/* 676 */   private String[] filesArray = null;
/*     */ 
/*     */   public void initialize(InputSplit split, TaskAttemptContext context)
/*     */   {
/* 681 */     this.filesArray = ((ReadFileSplit)split).filesArray;
/* 682 */     this.length = this.filesArray.length;
/* 683 */     System.out.println("[ReadFileSizeInputFormat][initialize] get filearray from split, size=[" + this.filesArray.length + "]");
/*     */   }
/*     */ 
/*     */   public boolean nextKeyValue()
/*     */     throws IOException
/*     */   {
/* 689 */     if (this.pos < this.length) {
/* 690 */       this.key = new IntWritable(this.pos + 1);
/* 691 */       this.value = new Text(this.filesArray[this.pos]);
/* 692 */       this.pos += 1;
/* 693 */       return true;
/*     */     }
/* 695 */     return false;
/*     */   }
/*     */ 
/*     */   public IntWritable getCurrentKey()
/*     */   {
/* 702 */     return this.key;
/*     */   }
/*     */ 
/*     */   public Text getCurrentValue()
/*     */   {
/* 708 */     return this.value;
/*     */   }
/*     */ 
/*     */   public void close()
/*     */     throws IOException
/*     */   {
/*     */   }
/*     */ 
/*     */   public float getProgress()
/*     */     throws IOException
/*     */   {
/* 719 */     return this.pos == this.length ? 100 : this.pos / this.length;
/*     */   }
/*     */ }

/* Location:           C:\Users\chenlianguo\Desktop\云经分\ocetl_local_cmc_sd.jar
 * Qualified Name:     com.asiainfo.local.cmc.sd.ReadFileReader
 * JD-Core Version:    0.6.0
 */