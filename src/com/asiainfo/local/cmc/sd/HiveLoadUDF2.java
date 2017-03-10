package com.asiainfo.local.cmc.sd;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.Session;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
import com.ailk.cloudetl.dbservice.commons.HiveJdbcClient;
import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
 
 public abstract class HiveLoadUDF2
   implements UDFActivity
 {
   private static final Log LOG = LogFactory.getLog(HiveLoadUDF2.class);
   private JobResult result = JobResult.RIGHT;
   private static final String LOAD_TMP_DST = "/asiainfo/ETL/load/temp/";
   private static final String LOAD_SRC_PATH = "/asiainfo/ETL/trans/out";
   SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
   private Map<String, Object> returnMap = new HashMap();
 
   private boolean singleLog = true;
   private Logger localLogger = null;
 
   private StringBuffer errorsb = new StringBuffer();
   private String database;
   private String connStr;
   private ThreadPoolExecutor copyThreadPool;
   private ThreadPoolExecutor loadThreadPool;
   List<String> loadedFiles = new ArrayList();
 
   private List<LoadJob> getLoadList(String tableName, String dataTime) {
     List result = new ArrayList();
     Session session = HibernateUtils.getSessionFactory().openSession();
     StringBuilder sb = new StringBuilder(
       "select distinct a.TABLE_NAME_, a.DATA_TIME_, a.FILE_NAME_, a.FILE_PATH_, b.LOAD_ACTION_, b.IS_PARTITION_, b.TIME_WINDOW_, b.DAY_FLAG_, a.VALIDATE_STATUS_,b.NEED_CHECK_RECORD_NUM_  from STATUS_LD_FILE a, (select distinct IF_NO_,\tTABLE_NAME_,LOAD_STYLE_,HIVE_STATUS_,NEED_CHECK_RECORD_NUM_,DAY_FLAG_,TIME_WINDOW_,IS_PARTITION_,LOAD_ACTION_, PRI_  from CFG_TABLE_INTERFACE) b where upper(a.TABLE_NAME_)=upper(b.TABLE_NAME_) and (a.HIVE_LOAD_STATUS_=0 or a.HIVE_LOAD_STATUS_ is null) and ((b.NEED_CHECK_RECORD_NUM_= 1 and a.VALIDATE_STATUS_ = 1) or (b.NEED_CHECK_RECORD_NUM_ is null)) and b.HIVE_STATUS_=1 and b.LOAD_STYLE_!=3 and exists (select 1 from STATUS_ET_FILE where FLOW_INST_ID_ is not null and a.FLOW_INST_ID_=FLOW_INST_ID_) and not exists (select 1 from STATUS_INTERFACE where b.LOAD_ACTION_=0 and upper(a.TABLE_NAME_)=upper(b.TABLE_NAME_) and b.IF_NO_=IF_NO_ and DATA_TIME_=a.DATA_TIME_ and (TRANS_STATUS_=0 or TRANS_STATUS_ is null))");
 
     if (!StringUtils.isEmpty(tableName)) {
       sb.append(" and a.TABLE_NAME_=?");
     }
     if (!StringUtils.isEmpty(dataTime)) {
       sb.append(" and a.DATA_TIME_=?");
     }
     sb.append(" order by b.PRI_ desc");
 
     SQLQuery querySql = session.createSQLQuery(sb.toString());
 
     boolean flag = false;
 
     if (!StringUtils.isEmpty(tableName)) {
       querySql.setString(0, tableName);
       flag = true;
     }
 
     if (!StringUtils.isEmpty(dataTime)) {
       if (flag)
         querySql.setInteger(1, Integer.valueOf(dataTime).intValue());
       else {
         querySql.setInteger(0, Integer.valueOf(dataTime).intValue());
       }
     }
     LOG.info("Hive加载查询待加载文件SQL:[" + querySql.getQueryString() + "]");
     writeLocalLog("Hive加载查询待加载文件SQL:[" + querySql.getQueryString() + "]");
     List temp = querySql.list();
     for (Iterator localIterator = temp.iterator(); localIterator.hasNext(); ) { Object obj = localIterator.next();
       result.add(new LoadJob((Object[])obj));
     }
     LOG.info("Hive加载查询待加载文件SQL结果是否为空:" + result.isEmpty() + ",待加载文件个数:" + result.size());
     writeLocalLog("Hive加载查询待加载文件SQL结果是否为空:" + result.isEmpty() + ",待加载文件个数:" + result.size());
     return result; } 
   // ERROR //

   private void initLocalLog(Environment env, Map<String, String> udfParams) { this.singleLog = TaskNodeLogger.isSingleLog();
     Object logObj = env.get("taskNodeLogger");
     if (logObj == null) {
       LOG.warn("the localLogger is not in Environment");
       this.singleLog = false;
     } else {
       this.localLogger = ((Logger)logObj);
     } }
 
   private void writeLocalLog(Exception e, String errMsg)
   {
     if (this.singleLog) {
       if (StringUtils.isBlank(errMsg)) {
         errMsg = "execute " + getClass().getName() + " activity error." + AICloudETLExceptionUtil.getErrMsg(e);
       }
       this.localLogger.error(errMsg, e);
     }
   }
 
   private void writeLocalLog(String message) {
     if (this.singleLog)
       this.localLogger.info(message);
   }
 
   private void copyMultiJobFile(List<LoadJob> partitionJobList, String tempdst, FileSystem hdfs) throws Exception
   {
     List copyThreads = new ArrayList();
 
     if (!hdfs.exists(new Path(tempdst)))
       hdfs.mkdirs(new Path(tempdst));
     HDFSCopyThread thread;
     for (LoadJob job : partitionJobList) {
       thread = new HDFSCopyThread(job.FILE_NAME_, job.FILE_PATH_, tempdst, hdfs);
       this.copyThreadPool.execute(thread);
       copyThreads.add(thread);
     }
     while (true) {
       boolean allDone = true;
       for (thread = (HDFSCopyThread) copyThreads.iterator(); ((Iterator) thread).hasNext(); ) { thread = (HDFSCopyThread)((Iterator) thread).next();
         if (!((HDFSCopyThread)thread).isDone) {
           allDone = false;
           break;
         }
       }
       if (allDone) {
         break;
       }
       Thread.sleep(1000L);
     }
 
//  for (Object thread = copyThreads.iterator(); ((Iterator)thread).hasNext(); ) { HDFSCopyThread thread = (HDFSCopyThread)((Iterator)thread).next();
     for (Object threads = copyThreads.iterator(); ((Iterator)threads).hasNext(); ) { 
				thread = (HDFSCopyThread)((Iterator)thread).next();
      if (!thread.isSuccess) {
        updateTable(thread.fileName, thread.filePath, "error", false);
        throw new RuntimeException("copy file fail." + thread.message);
      } }
  }

  private HashSet<String> getBasePartitionKeys(List<LoadJob> joblist)
  {
    HashSet result = new HashSet();
    for (LoadJob job : joblist) {
      String path = job.FILE_PATH_;
      if (File.separator.equals(String.valueOf(job.FILE_PATH_.charAt(job.FILE_PATH_.length() - 1)))) {
        path = job.FILE_PATH_.substring(0, job.FILE_PATH_.length() - 1);
      }
      String[] splits = path.split(File.separator);
      String partitionKey = new String();
      for (String split : splits) {
        if (split.indexOf("=") >= 0) {
          String[] partition = split.split("=");
          partitionKey = partition[0] + "='" + partition[1] + "'";

          break;
        }
      }
      result.add(partitionKey);
    }
    return result;
  }

  private Map<String, List<LoadJob>> getJobListByPartition(List<LoadJob> joblist) {
    Map partitionList = new HashMap();
    for (LoadJob job : joblist) {
      String path = job.FILE_PATH_;
      if (File.separator.equals(String.valueOf(job.FILE_PATH_.charAt(job.FILE_PATH_.length() - 1)))) {
        path = job.FILE_PATH_.substring(0, job.FILE_PATH_.length() - 1);
      }
      String[] splits = path.split(File.separator);
      StringBuilder partitionsb = new StringBuilder();
      for (String split : splits) {
        if (split.indexOf("=") >= 0) {
          String[] partition = split.split("=");
          partitionsb.append(partition[0]);
          partitionsb.append("='");
          partitionsb.append(partition[1]);
          partitionsb.append("',");
        }
      }
      String partitionStr = partitionsb.substring(0, partitionsb.length() - 1);
      if (!partitionList.containsKey(partitionStr)) {
        partitionList.put(partitionStr, new ArrayList());
      }
      ((List)partitionList.get(partitionStr)).add(job);
    }
    return partitionList;
  }

  private Map<Integer, List<LoadJob>> getJobListByDataTime(List<LoadJob> joblist) {
    Map partitionList = new HashMap();
    for (LoadJob job : joblist) {
      Integer dataTime = job.DATA_TIME_;
      if (!partitionList.containsKey(dataTime)) {
        partitionList.put(dataTime, new ArrayList());
        LOG.info("DataTime［" + dataTime + "］");
      }
      ((List)partitionList.get(dataTime)).add(job);
    }
    return partitionList;
  }

  private void doLoad(String tableName, List<LoadJob> joblist, Configuration conf, FileSystem hdfs)
  {
    boolean isPartition = ((LoadJob)joblist.get(0)).IS_PARTITION_.equals("1");
    boolean isOverWrite = ((LoadJob)joblist.get(0)).LOAD_ACTION_.equals("0");
    LOG.info("加载[" + tableName + "] 是否分区[" + isPartition + "] 是否追加［" + isOverWrite + "］");
    List partitionJobList;
    if (isPartition) {
      if (isOverWrite)
      {
        LOG.info("DROP对应的Partition");
        HashSet<String> basePartitionList = getBasePartitionKeys(joblist);
        try {
          for (String partitionKey : basePartitionList)
            excuteHiveSql("use " + this.database + ";ALTER TABLE " + tableName + " DROP PARTITION (" + partitionKey + ")");
        }
        catch (Exception e) {
          updateTableBatch(joblist, "error");
          throw new RuntimeException("DROP PARTITION FAILED. ", e);
        }
      }

      LOG.info("按分区字段分组加载");
      Map<String ,List<LoadJob>> partitionList = getJobListByPartition(joblist);

      for (String partitionStr : partitionList.keySet())
      {
        LOG.info("加载分区[" + partitionStr + "]");
        partitionJobList = (List)partitionList.get(partitionStr);
        updateTableBatch(partitionJobList, "do");
        String tempdst = "/asiainfo/ETL/load/temp/" + ((LoadJob)partitionJobList.get(0)).FILE_PATH_.substring("/asiainfo/ETL/trans/out".length());
        try {
          if (hdfs.exists(new Path(tempdst))) {
            hdfs.delete(new Path(tempdst), true);
          }
          copyMultiJobFile(partitionJobList, tempdst, hdfs);
          excuteHiveSql("use " + this.database + ";LOAD DATA INPATH '" + tempdst + "' INTO TABLE " + tableName + " PARTITION (" + partitionStr + ")");
          updateTableBatch(partitionJobList, "done");
        } catch (Exception e) {
          updateTableBatch(partitionJobList, "error");
          throw new RuntimeException("load files in [" + tempdst + "] into hive [" + tableName + "] fail. ", e);
        }
      }

    }
    else if (isOverWrite)
    {
      LOG.info("按dataTime字段加载最新数据");

      Map<Integer,List<LoadJob>> dataTimeList = getJobListByDataTime(joblist);

      Integer maxDataTime = Integer.valueOf(-2147483648);
      for (Integer dataTime : dataTimeList.keySet()) {
        if (dataTime.intValue() > maxDataTime.intValue()) {
          maxDataTime = dataTime;
        }
      }
      LOG.info("maxDataTime［" + maxDataTime + "］");

      for (Integer curDataTime : dataTimeList.keySet()) {
        LOG.info("加载［" + curDataTime + "］开始");
        List curJobList = (List)dataTimeList.get(curDataTime);
        if (curDataTime.equals(maxDataTime)) {
          String tempdst = "/asiainfo/ETL/load/temp/" + ((LoadJob)curJobList.get(0)).FILE_PATH_.substring("/asiainfo/ETL/trans/out".length());
          LOG.info("临时目标目录［" + tempdst + "］");
          try {
            if (hdfs.exists(new Path(tempdst))) {
              LOG.info("临时目标目录［" + tempdst + "］已存在，删除");
              hdfs.delete(new Path(tempdst), true);
            }
            copyMultiJobFile(curJobList, tempdst, hdfs);
            excuteHiveSql("use " + this.database + ";LOAD DATA INPATH '" + tempdst + "' OVERWRITE INTO TABLE " + tableName);
            updateTableBatch(curJobList, "done");
          } catch (Exception e) {
            updateTableBatch(curJobList, "error");
            throw new RuntimeException("load files in [" + tempdst + "] into hive [" + tableName + "] fail. ", e);
          }
        } else {
          updateTableBatch(curJobList, "done");
        }
        LOG.info("加载［" + curDataTime + "］完成");
      }
    } else {
      String tempdst = "/asiainfo/ETL/load/temp/" + ((LoadJob)joblist.get(0)).FILE_PATH_.substring("/asiainfo/ETL/trans/out".length());
      try {
        if (hdfs.exists(new Path(tempdst))) {
          hdfs.delete(new Path(tempdst), true);
        }
        copyMultiJobFile(joblist, tempdst, hdfs);
        excuteHiveSql("use " + this.database + ";LOAD DATA INPATH '" + tempdst + "' INTO TABLE " + tableName);
        updateTableBatch(joblist, "done");
      } catch (Exception e) {
        updateTableBatch(joblist, "error");
        throw new RuntimeException("load files in [" + tempdst + "] into hive [" + tableName + "] fail. ", e);
      }
    }
  }

  private void excuteHiveSql(String sql) throws Exception
  {
    LOG.info("执行HQL:[" + sql + "]");
    executeSql(sql);
  }

  public String executeSql(String sql) throws Exception {
    HiveJdbcClient hiveClient = new HiveJdbcClient(this.connStr);
    String returnValue = null;
    try {
      String[] sqls = StringUtils.splitByWholeSeparator(sql, ";");
      for (String s : sqls)
        hiveClient.execSql(s);
    }
    finally {
      if (hiveClient != null) {
        hiveClient.destroy();
      }
    }
    return returnValue;
  }
  private void updateTableBatch(List<LoadJob> jobList, String flag) {
    Session session = null;
    PreparedStatement ups = null;
    Connection con = null;
    try {
      session = HibernateUtils.getSessionFactory().openSession();
      con = session.connection();
      con.setAutoCommit(false);
      String sql = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=0,HIVE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      if ("do".equals(flag))
        sql = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=2,HIVE_LOAD_START_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      else if ("done".equals(flag)) {
        sql = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=1,HIVE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      }
      ups = con.prepareStatement(sql);
      for (LoadJob job : jobList) {
        String time = this.dateformatter.format(new Date());
        ups.setString(1, time);
        ups.setString(2, job.FILE_PATH_);
        ups.setString(3, job.FILE_NAME_);
        ups.execute();
        con.commit();

        ups.clearParameters();
      }
    }
    catch (Exception e)
    {
      LOG.error("updateTableBatch error", e);
      try
      {
        if (ups != null) {
          ups.close();
        }
        if (session != null)
          session.close();
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    finally
    {
      try
      {
        if (ups != null) {
          ups.close();
        }
        if (session != null)
          session.close();
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private void updateTable(String FILE_NAME_, String FILE_PATH_, String flag, boolean overwrite) {
    Session session = null;
    try
    {
      session = HibernateUtils.getSessionFactory().openSession();
      session.beginTransaction();

      String sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=0,HIVE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      if (overwrite) {
        sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=0,HIVE_LOAD_END_TIME_=? where FILE_PATH_=?";
        if ("do".equals(flag))
          sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=2,HIVE_LOAD_START_TIME_=? where FILE_PATH_=?";
        else if ("done".equals(flag)) {
          sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=1,HIVE_LOAD_END_TIME_=? where FILE_PATH_=?";
        }
      }
      else if ("do".equals(flag)) {
        sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=2,HIVE_LOAD_START_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      } else if ("done".equals(flag)) {
        sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=1,HIVE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      }

      SQLQuery STATUS_LD_FILE = session.createSQLQuery(sql4sqlSTATUS_LD_FILE);
      String time = this.dateformatter.format(new Date());
      STATUS_LD_FILE.setString(0, time);
      STATUS_LD_FILE.setString(1, FILE_PATH_);
      if (!overwrite) {
        STATUS_LD_FILE.setString(2, FILE_NAME_);
        LOG.info("excute sql [" + sql4sqlSTATUS_LD_FILE + "] [" + time + "] [" + FILE_PATH_ + "] [" + FILE_NAME_ + "]");
      } else {
        LOG.info("excute sql [" + sql4sqlSTATUS_LD_FILE + "] [" + time + "] [" + FILE_PATH_ + "]");
      }
      STATUS_LD_FILE.executeUpdate();

      session.getTransaction().commit();
    } catch (Exception e) {
      LOG.error("updateTable error", e);
    } finally {
      if (session != null)
        session.close();
    }
  }

  public void releaseResource(HisActivity hisAct)
  {
  }

  public JobResult getState()
  {
    return this.result;
  }

  public static void main(String[] args)
  {
    String FILE_PATH_ = "/asiainfo/ETL/trans/out/ods_userscore_accu_ds/PT_TIME_=20131012/";
    String path = FILE_PATH_;
    if ("/".equals(String.valueOf(FILE_PATH_.charAt(FILE_PATH_.length() - 1)))) {
      path = FILE_PATH_.substring(0, FILE_PATH_.length() - 1);
    }
    System.out.println(FILE_PATH_.charAt(FILE_PATH_.length() - 1));
    System.out.println(path);
    String partition = path.substring(path.lastIndexOf("/") + 1);
    System.out.println(partition);
    String partitionKey = partition.split("=")[0];
    String partitionValue = partition.split("=")[1];

    System.out.println(partitionKey + "|" + partitionValue);
    System.out.println(FILE_PATH_.substring("/asiainfo/ETL/trans/out".length()));

    StringBuffer s = new StringBuffer("123");
    System.out.println(s.substring(0, s.length()));
  }

  class HDFSCopyThread implements Runnable
  {
    private String fileName;
    private String filePath;
    private String destPath;
    private FileSystem hdfs;
    public String message;
    public boolean isSuccess;
    private boolean isDone = false;

    HDFSCopyThread(String fileName, String filePath, String destPath, FileSystem hdfs) {
      this.fileName = fileName;
      this.filePath = filePath;
      this.destPath = destPath;
      this.hdfs = hdfs;
    }

    public void run()
    {
      try {
        long start = System.currentTimeMillis();
        this.isSuccess = FileUtil.copy(this.hdfs, new Path(this.filePath + File.separator + this.fileName), this.hdfs, new Path(this.destPath), false, true, this.hdfs.getConf());
        long stop = System.currentTimeMillis();
        HiveLoadUDF2.LOG.info("copy file[" + this.filePath + File.separator + this.fileName + "] to [" + this.destPath + "] done , use time[" + (stop - start) + "]ms");
        HiveLoadUDF2.this.writeLocalLog("copy file[" + this.filePath + File.separator + this.fileName + "] to [" + this.destPath + "] done , use time[" + (stop - start) + "]ms");
      } catch (Exception ex) {
        this.isSuccess = false;
        this.message = ("copy file[" + this.filePath + File.separator + this.fileName + "] to [" + this.destPath + "] error: " + ex.getMessage());
      }
      this.isDone = true;
    }
  }

  class LoadJob
  {
    public String TABLE_NAME_;
    public Integer DATA_TIME_;
    public String FILE_NAME_;
    public String FILE_PATH_;
    public String LOAD_ACTION_;
    public String IS_PARTITION_;
    public String TIME_WINDOW_;
    public String DAY_FLAG_;
    public String VALIDATE_STATUS_;
    public String NEED_CHECK_RECORD_NUM_;

    public LoadJob(Object[] objs)
    {
      this.TABLE_NAME_ = (objs[0] == null ? null : objs[0].toString());
      this.DATA_TIME_ = (objs[1] == null ? null : Integer.valueOf(objs[1].toString()));
      this.FILE_NAME_ = (objs[2] == null ? null : objs[2].toString());
      this.FILE_PATH_ = (objs[3] == null ? null : objs[3].toString());
      this.LOAD_ACTION_ = (objs[4] == null ? null : objs[4].toString());
      this.IS_PARTITION_ = (objs[5] == null ? null : objs[5].toString());
      this.TIME_WINDOW_ = (objs[6] == null ? null : objs[6].toString());
      this.DAY_FLAG_ = (objs[7] == null ? null : objs[7].toString());
      this.VALIDATE_STATUS_ = (objs[8] == null ? null : objs[8].toString());
      this.NEED_CHECK_RECORD_NUM_ = (objs[9] == null ? null : objs[9].toString());
      HiveLoadUDF2.LOG.info("配置信息: 目标表[" + this.TABLE_NAME_ + "]" + 
        "数据日期[" + this.DATA_TIME_ + "]" + 
        "路径[" + this.FILE_PATH_ + "]" + 
        "文件名[" + this.FILE_NAME_ + "]" + 
        "是否追加[" + this.LOAD_ACTION_ + "]" + 
        "是否分区[" + this.IS_PARTITION_ + "]" + 
        "需要加载时间[" + this.TIME_WINDOW_ + "]" + 
        "月接口加载时间[" + this.DAY_FLAG_ + "]");
    }
  }

  class LoadTableThread
    implements Runnable
  {
    private String tableName;
    private List<HiveLoadUDF2.LoadJob> jobList;
    private FileSystem hdfs;
    private Configuration conf;
    private boolean isDone = false;

///*     */     LoadTableThread(List<HiveLoadUDF2.LoadJob> tableName, FileSystem jobList, Configuration hdfs) {
             LoadTableThread(String tableName, List<HiveLoadUDF2.LoadJob> jobList, FileSystem hdfs) {
               this.tableName = tableName;
               this.jobList = jobList;
               this.hdfs = hdfs;
               this.conf = conf;
             }         
             public void run()
             {
               try {
                 Date d = new Date();
                 Date compare = new Date();
                 String TIME_WINDOW_ = ((HiveLoadUDF2.LoadJob)this.jobList.get(0)).TIME_WINDOW_;
                 String DAY_FLAG_ = ((HiveLoadUDF2.LoadJob)this.jobList.get(0)).DAY_FLAG_;
                 if ((TIME_WINDOW_ != null) && (DAY_FLAG_ != null))
                 {
                   String[] ss = TIME_WINDOW_.toString().split(":");
                   compare.setHours(Integer.valueOf(ss[0]).intValue());
                   compare.setMinutes(Integer.valueOf(ss[1]).intValue());
                   compare.setSeconds(Integer.valueOf(ss[2]).intValue());
                   HiveLoadUDF2.LOG.info("入库时间和入库日期都有配置TIME_WINDOW_[" + TIME_WINDOW_ + "]DAY_FLAG_[" + DAY_FLAG_ + "]");
                   HiveLoadUDF2.this.writeLocalLog("入库时间和入库日期都有配置TIME_WINDOW_[" + TIME_WINDOW_ + "]DAY_FLAG_[" + DAY_FLAG_ + "]");
                   if ((d.after(compare)) && (Integer.valueOf(DAY_FLAG_.toString()).intValue() <= d.getDate())) {
                     HiveLoadUDF2.LOG.info("入库时间和日期符合要求,开始加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个");
                     HiveLoadUDF2.this.writeLocalLog("入库时间和日期符合要求,开始加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个");
                     long start = System.currentTimeMillis();
                     HiveLoadUDF2.this.doLoad(this.tableName, this.jobList, this.conf, this.hdfs);
                     long stop = System.currentTimeMillis();
                     HiveLoadUDF2.LOG.info("完成加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                     HiveLoadUDF2.this.writeLocalLog("完成加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                     for (HiveLoadUDF2.LoadJob job : this.jobList)
                       HiveLoadUDF2.this.loadedFiles.add(job.FILE_PATH_ + File.separator + job.FILE_NAME_);
                   }
                 }
                 else
                 {
                   HiveLoadUDF2.LoadJob job;
                   if (TIME_WINDOW_ != null)
                   {
                     String[] ss = TIME_WINDOW_.toString().split(":");
                     compare.setHours(Integer.valueOf(ss[0]).intValue());
                     compare.setMinutes(Integer.valueOf(ss[1]).intValue());
                     compare.setSeconds(Integer.valueOf(ss[2]).intValue());
                     HiveLoadUDF2.LOG.info("入库时间有配置TIME_WINDOW_[" + TIME_WINDOW_ + "]");
                     HiveLoadUDF2.this.writeLocalLog("入库时间有配置TIME_WINDOW_[" + TIME_WINDOW_ + "]");
                     if (d.after(compare)) {
                       HiveLoadUDF2.LOG.info("入库时间符合要求,开始加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个");
                       HiveLoadUDF2.this.writeLocalLog("入库时间符合要求,开始加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个");
                       long start = System.currentTimeMillis();
                       HiveLoadUDF2.this.doLoad(this.tableName, this.jobList, this.conf, this.hdfs);
                       long stop = System.currentTimeMillis();
                       HiveLoadUDF2.LOG.info("完成加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                       HiveLoadUDF2.this.writeLocalLog("完成加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
//                for (??? = this.jobList.iterator(); ???.hasNext(); ) { job = (HiveLoadUDF2.LoadJob)???.next();
                       for (LoadJob job2 : jobList) { 
                         HiveLoadUDF2.this.loadedFiles.add(job2.FILE_PATH_ + File.separator + job2.FILE_NAME_); }
                     }
                   }
                   else if (DAY_FLAG_ != null) {
                     HiveLoadUDF2.LOG.info("入库日期有配置DAY_FLAG_[" + DAY_FLAG_ + "]");
                     HiveLoadUDF2.this.writeLocalLog("入库日期有配置DAY_FLAG_[" + DAY_FLAG_ + "]");
          
                     if (Integer.valueOf(DAY_FLAG_.toString()).intValue() <= d.getDate()) {
                       HiveLoadUDF2.LOG.info("入库日期符合要求,开始加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个");
                       HiveLoadUDF2.this.writeLocalLog("入库日期符合要求,开始加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个");
                       long start = System.currentTimeMillis();
                       HiveLoadUDF2.this.doLoad(this.tableName, this.jobList, this.conf, this.hdfs);
                       long stop = System.currentTimeMillis();
                       HiveLoadUDF2.LOG.info("完成加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                       HiveLoadUDF2.this.writeLocalLog("完成加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                       for (HiveLoadUDF2.LoadJob jobb : this.jobList)
                         HiveLoadUDF2.this.loadedFiles.add(jobb.FILE_PATH_ + File.separator + jobb.FILE_NAME_);
                     }
                   }
                   else {
                     HiveLoadUDF2.LOG.info("入库日期和入库时间都没有配置");
                     HiveLoadUDF2.this.writeLocalLog("入库日期和入库时间都没有配置");
          
                     HiveLoadUDF2.LOG.info("开始加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个");
                     HiveLoadUDF2.this.writeLocalLog("开始加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个");
                     long start = System.currentTimeMillis();
                     HiveLoadUDF2.this.doLoad(this.tableName, this.jobList, this.conf, this.hdfs);
                     long stop = System.currentTimeMillis();
                     HiveLoadUDF2.LOG.info("完成加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                     HiveLoadUDF2.this.writeLocalLog("完成加载[" + this.tableName + "] 共计文件[" + this.jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                     for (HiveLoadUDF2.LoadJob joba : this.jobList)
                       HiveLoadUDF2.this.loadedFiles.add(joba.FILE_PATH_ + File.separator + joba.FILE_NAME_);
                   }
                 }
               } catch (Exception e) {
                 HiveLoadUDF2.this.errorsb.append(this.tableName + ",");
               }
               this.isDone = true;
             }
           }
 }
          
