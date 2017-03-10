/*    */ package com.asiainfo.local.cmc.sd;
/*    */ 
/*    */ import com.ailk.cloudetl.action.Execution;
/*    */ import com.ailk.cloudetl.commons.utils.VelocityUtil;
/*    */ import com.ailk.cloudetl.dataflow.xmlmodel.ParseNames;
/*    */ import com.ailk.cloudetl.dbservice.DBUtils;
/*    */ import com.ailk.cloudetl.hive.internal.HiveUtil;
/*    */ import com.ailk.cloudetl.hive.xml.Hive4Trans;
/*    */ import com.ailk.cloudetl.script.ScriptEnv;
/*    */ import java.util.Iterator;
/*    */ import java.util.List;
/*    */ import java.util.Map;
/*    */ import java.util.Set;
/*    */ import org.apache.hadoop.mapred.JobClient;
/*    */ import org.apache.hadoop.mapred.JobConf;
/*    */ import org.apache.hadoop.mapred.JobStatus;
/*    */ import org.apache.hadoop.mapred.RunningJob;
/*    */ import org.apache.log4j.Logger;
/*    */ 
/*    */ public class Hive extends Execution
/*    */ {
/* 24 */   private String SQL = null;
/*    */ 
/*    */   protected void init() throws Exception {
/* 27 */     Hive4Trans thisNode = (Hive4Trans)this.node;
/*    */ 
/* 29 */     ScriptEnv scriptEnv = new ScriptEnv();
/* 30 */     Iterator keys = this.vars.keySet().iterator();
/* 31 */     while (keys.hasNext()) {
/* 32 */       String key = (String)keys.next();
/* 33 */       scriptEnv.setVar(key, this.vars.get(key));
/*    */     }
/*    */ 
/* 36 */     String sqlStr = thisNode.getFilter().getText();
/* 37 */     if (sqlStr == null) {
/* 38 */       return;
/*    */     }
/*    */ 
/* 41 */     scriptEnv.setVar("DFContext", new DFContext());
/*    */ 
/* 43 */     this.SQL = VelocityUtil.getInstance().evaluate(scriptEnv.getEnv(), sqlStr);
/*    */   }
/*    */ 
/*    */   protected void run(Map out) throws Exception
/*    */   {
/*    */     try {
/* 49 */       if ((this.SQL == null) || ("".equals(this.SQL))) {
/* 50 */         this.LOG.warn("SQL is Null");
/* 51 */         return;
/*    */       }
/*    */ 
/* 54 */       out.put("ACT.VAR.JOB_NAME", this.executionId + "_" + this.actName);
/* 55 */       String returnValue = HiveUtil.getInstance().executeSql(this.SQL, this.executionId + "_" + this.actName);
/*    */ 
/* 57 */       out.put("HIVE_VALUE", returnValue);
/*    */     } catch (Exception e) {
/* 59 */       out.put("ERROR", e.getMessage());
/* 60 */       throw e;
/*    */     }
/*    */   }
/*    */ 
/*    */   protected void clear()
/*    */   {
/*    */   }
/*    */ 
/*    */   protected void releaseResource(String executionId)
/*    */     throws Exception
/*    */   {
/* 71 */     List valueList = DBUtils.querySql("select VALUE_ from ocdc_execution_data where EXECUTION_ID_='" + executionId + "' and KEY_='" + "ACT.VAR.JOB_NAME" + "'");
/* 72 */     if ((valueList != null) && (valueList.size() > 0)) {
/* 73 */       JobConf conf = new JobConf();
/* 74 */       JobClient jc = new JobClient(conf);
/* 75 */       JobStatus[] jss = jc.getAllJobs();
/* 76 */       for (JobStatus js : jss)
/* 77 */         if ((js.getRunState() == 1) || (js.getRunState() == 4)) {
/* 78 */           RunningJob rj = jc.getJob(js.getJobID());
/* 79 */           if (valueList.get(0).toString().equals(rj.getJobName()))
/* 80 */             rj.killJob();
/*    */         }
/*    */     }
/*    */   }
/*    */ }

/* Location:           C:\Users\chenlianguo\Desktop\云经分\ocetl_local_cmc_sd.jar
 * Qualified Name:     com.asiainfo.local.cmc.sd.Hive
 * JD-Core Version:    0.6.0
 */