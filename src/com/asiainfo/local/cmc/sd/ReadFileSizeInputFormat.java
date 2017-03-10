/*     */ package com.asiainfo.local.cmc.sd;
/*     */ 
/*     */ import java.io.IOException;
/*     */ import java.io.PrintStream;
/*     */ import java.util.ArrayList;
/*     */ import java.util.List;
/*     */ import org.apache.hadoop.conf.Configuration;
/*     */ import org.apache.hadoop.io.IntWritable;
/*     */ import org.apache.hadoop.io.Text;
/*     */ import org.apache.hadoop.mapreduce.InputFormat;
/*     */ import org.apache.hadoop.mapreduce.InputSplit;
/*     */ import org.apache.hadoop.mapreduce.JobContext;
/*     */ import org.apache.hadoop.mapreduce.RecordReader;
/*     */ import org.apache.hadoop.mapreduce.TaskAttemptContext;
/*     */ 
/*     */ class ReadFileSizeInputFormat extends InputFormat<IntWritable, Text>
/*     */ {
/*     */   public List<InputSplit> getSplits(JobContext jobContext)
/*     */   {
/* 730 */     List ret = new ArrayList();
/* 731 */     String[] fileArray = jobContext.getConfiguration().getStrings("read_file_list");
/* 732 */     List groupedFile = makeGroup(fileArray);
/* 733 */     for (int i = 0; i < groupedFile.size(); i++) {
/* 734 */       ReadFileSplit rfs = new ReadFileSplit(((String)groupedFile.get(i)).split(","));
/* 735 */       System.out.println("[ReadFileSizeInputFormat][getSplits] get filearray from conf, filelist=[" + (String)groupedFile.get(i) + "]");
/* 736 */       ret.add(rfs);
/*     */     }
/* 738 */     return ret;
/*     */   }
/*     */ 
/*     */   private List<String> makeGroup(String[] fileList)
/*     */   {
/* 743 */     List ret = new ArrayList();
/* 744 */     int groupNum = fileList.length > 10 ? 10 : fileList.length;
/* 745 */     for (int i = 0; i < groupNum; i++) {
/* 746 */       ret.add("");
/*     */     }
/* 748 */     for (int i = 0; i < fileList.length; i++) {
/* 749 */       int group = i % groupNum;
/* 750 */       String tmp = (String)ret.get(group);
/* 751 */       tmp = tmp + fileList[i] + ",";
/* 752 */       ret.set(group, tmp);
/*     */     }
/* 754 */     for (int i = 0; i < groupNum; i++) {
/* 755 */       String tmp = (String)ret.get(i);
/* 756 */       tmp = tmp.substring(0, tmp.length() - 1);
/* 757 */       ret.set(i, tmp);
/*     */     }
/* 759 */     return ret;
/*     */   }
/*     */ 
/*     */   public RecordReader<IntWritable, Text> createRecordReader(InputSplit ignored, TaskAttemptContext taskContext)
/*     */     throws IOException
/*     */   {
/* 767 */     System.out.println("[ReadFileSizeInputFormat][getSplits] get filearray from conf, filelist=[" + ((ReadFileSplit)ignored).filesArray.toString() + "]");
/* 768 */     ReadFileReader rfr = new ReadFileReader();
/* 769 */     return rfr;
/*     */   }
/*     */ }

/* Location:           C:\Users\chenlianguo\Desktop\云经分\ocetl_local_cmc_sd.jar
 * Qualified Name:     com.asiainfo.local.cmc.sd.ReadFileSizeInputFormat
 * JD-Core Version:    0.6.0
 */