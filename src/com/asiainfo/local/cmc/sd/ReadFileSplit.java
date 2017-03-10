/*     */ package com.asiainfo.local.cmc.sd;
/*     */ 
/*     */ import java.io.DataInput;
/*     */ import java.io.DataOutput;
/*     */ import java.io.IOException;
/*     */ import org.apache.hadoop.io.Text;
/*     */ import org.apache.hadoop.io.Writable;
/*     */ import org.apache.hadoop.mapreduce.InputSplit;
/*     */ 
/*     */ class ReadFileSplit extends InputSplit
/*     */   implements Writable
/*     */ {
/*     */   public String[] filesArray;
/*     */   private int length;
/*     */ 
/*     */   public ReadFileSplit()
/*     */   {
/*     */   }
/*     */ 
/*     */   public ReadFileSplit(String[] fileArray)
/*     */   {
/* 630 */     this.filesArray = fileArray;
/* 631 */     this.length = fileArray.length;
/*     */   }
/*     */ 
/*     */   public void write(DataOutput out)
/*     */     throws IOException
/*     */   {
/* 637 */     out.writeInt(this.length);
/* 638 */     for (String file : this.filesArray)
/* 639 */       Text.writeString(out, file);
/*     */   }
/*     */ 
/*     */   public void readFields(DataInput in)
/*     */     throws IOException
/*     */   {
/* 646 */     this.length = in.readInt();
/* 647 */     this.filesArray = new String[this.length];
/* 648 */     for (int i = 0; i < this.length; i++)
/* 649 */       this.filesArray[i] = Text.readString(in);
/*     */   }
/*     */ 
/*     */   public long getLength()
/*     */   {
/* 656 */     return 0L;
/*     */   }
/*     */ 
/*     */   public String[] getLocations()
/*     */   {
/* 662 */     return new String[0];
/*     */   }
/*     */ }

/* Location:           C:\Users\chenlianguo\Desktop\云经分\ocetl_local_cmc_sd.jar
 * Qualified Name:     com.asiainfo.local.cmc.sd.ReadFileSplit
 * JD-Core Version:    0.6.0
 */