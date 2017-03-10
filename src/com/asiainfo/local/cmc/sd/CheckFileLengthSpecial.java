package com.asiainfo.local.cmc.sd;

import com.ailk.cloudetl.action.UDFLog;
import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
import com.ailk.cloudetl.ndataflow.api.ActivityVariable;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.context.DataFlowContext;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import com.ailk.cloudetl.ndataflow.intarnel.history.model.HisActivityImpl;
import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
import com.ailk.cloudetl.ndataflow.intarnel.session.HisActivityDAO;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;

public class CheckFileLengthSpecial
  implements UDFActivity
{
  private static final Log LOG = LogFactory.getLog(CheckFileLengthSpecial.class);
  private JobResult result = JobResult.RIGHT;
  private FileSystem fs = null;
  private Map<String, String> targetMap = new HashMap();
  private Map<String, String> sourceMap = new HashMap();
  private String currentJobId = "";
  private static final String TRANS_OUT_DIR = "/asiainfo/SDDW/ODS/";
  private static final String PARTITION_KEY = "pt_time_";
  private static final String DATA_TIME = "DATA_TIME";
  private boolean singleLog = true;
  private Logger localLogger = null;

  private String needCalcFileArrayString = "";
  private ArrayList<Check> needCalcFileList = new ArrayList();
  Map<String, Object> returnMap = new HashMap();
  private String instanceId = "";

  private void excuteBatchSQL(List<String> sqlList)
    throws Exception
  {
    Session session = null;
    try {
      session = HibernateUtils.getSessionFactory().openSession();
      session.doWork(new BatchSQLWork(sqlList));
    } catch (Exception e) {
      LOG.error("excute SQL Failed", e);
      writeLocalLog(e, "excute SQL Failed");
      throw e;
    } finally {
      if (session != null)
        session.close();
    }
  }

  private void parseParams(Environment env, Map<String, String> udfParams) throws Exception
  {
    writeLocalLog("开始初始化配置参数...");

    String sourcePaths = (String)udfParams.get("sourcePaths");
    if (sourcePaths == null) {
      throw new Exception("can't find sourcePaths");
    }
    if ("".equals(sourcePaths)) {
      throw new Exception("empty sourcePaths");
    }
    String[] sourcePathArray = sourcePaths.split(",");
    for (String sourcePath : sourcePathArray) {
      if (!this.fs.exists(new Path(sourcePath))) {
        throw new Exception("sourcePath[" + sourcePath + "] not exist!");
      }
    }
    String sourceInterface = (String)udfParams.get("sourceInterface");
    if (sourceInterface == null) {
      throw new Exception("can't find sourceInterface");
    }
    if ("".equals(sourceInterface)) {
      throw new Exception("empty sourceInterface");
    }
    String[] sourceInterfaceArray = sourceInterface.split(",");

    if (sourcePathArray.length != sourceInterfaceArray.length) {
      throw new Exception("unmatch sourcePaths[" + sourcePathArray.length + "] and sourceInterfaces[" + sourceInterfaceArray.length + "]");
    }

    for (int i = 0; i < sourcePathArray.length; i++) {
      this.sourceMap.put(sourcePathArray[i], sourceInterfaceArray[i]);
    }

    String targetPaths = (String)udfParams.get("targetPaths");
    if (targetPaths == null) {
      throw new Exception("can't find targetPaths");
    }
    if ("".equals(targetPaths)) {
      throw new Exception("empty targetPaths");
    }
    String[] targetPathArray = targetPaths.split(",");
    for (String targetPath : targetPathArray) {
      if (!this.fs.exists(new Path(targetPath))) {
        throw new Exception("targetPath[" + targetPath + "] not exist!");
      }
    }
    String targetTables = (String)udfParams.get("targetTables");
    if (targetTables == null) {
      throw new Exception("can't find targetPaths");
    }
    if ("".equals(targetTables)) {
      throw new Exception("empty targetPaths");
    }
    String[] targetTableArray = targetTables.split(",");

    if (targetPathArray.length != targetTableArray.length) {
      throw new Exception("unmatch targetPaths[" + targetPathArray.length + "] and targetTables[" + targetTableArray.length + "]");
    }

    for (int i = 0; i < targetPathArray.length; i++) {
      this.targetMap.put(targetPathArray[i], targetTableArray[i]);
    }

    writeLocalLog("参数初始化完毕.");
  }

  private void getCheckFileList(Path targetPath, String targetPathString)
    throws Exception
  {
    try
    {
      FileStatus[] targetFiles = this.fs.listStatus(targetPath);
      for (FileStatus targetFile : targetFiles)
      {
        if (targetFile.isDirectory()) {
          getCheckFileList(targetFile.getPath(), targetPathString);
        } else {
          Path src = targetFile.getPath();

          Check check = new Check(src.getName());
          check.fullName = src.toString();
          check.tablename = ((String)this.targetMap.get(targetPathString));
          LOG.info("========src=file=====[" + src.toString() + "] ");
          writeLocalLog("========src=file=====[" + src.toString() + "] ");
          this.needCalcFileList.add(check);
          this.needCalcFileArrayString = (this.needCalcFileArrayString + src.toString() + ",");
        }
      }
    } catch (Exception ex) {
      String errMsg = "rename files in [" + targetPathString + "] on HDFS failed";
      LOG.error(errMsg, ex);
      this.result = JobResult.ERROR;
      writeLocalLog(ex, errMsg);
      this.returnMap.put("HDFS_ERROR", "rename files in [" + targetPathString + "] on HDFS failed : [" + ex.getMessage() + "]");
      throw ex;
    }
  }

  public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
    throws Exception
  {
    initLocalLog(env, udfParams);

    Configuration conf = new Configuration();
    try {
      this.fs = FileSystem.get(conf);
    } catch (IOException ex) {
      String errMsg = "init HDFS failed";
      LOG.error(errMsg, ex);
      writeLocalLog(ex, errMsg);
      this.result = JobResult.ERROR;
      this.returnMap.put("HDFS_ERROR", "init HDFS failed : [" + ex.getMessage() + "]");
      return this.returnMap;
    }
    try
    {
      parseParams(env, udfParams);
    } catch (Exception ex) {
      String errMsg = "init parameters failed";
      LOG.error(errMsg, ex);
      writeLocalLog(ex, errMsg);
      this.result = JobResult.ERROR;
      this.returnMap.put("PARAM_ERROR", "init parameters failed : [" + ex.getMessage() + "]");
      return this.returnMap;
    }

    String dataTime = (String)udfParams.get("DATA_TIME");
    DataFlowContext context = (DataFlowContext)env.get("DFContext");
    this.instanceId = ((String)env.get("jobLogId"));

    for (String targetPathString : this.targetMap.keySet()) {
      Path targetPath = new Path(targetPathString);
      try {
        log.info("=====targetPathString======" + targetPathString);
        getCheckFileList(targetPath, targetPathString);
      } catch (Exception ex) {
        this.result = JobResult.ERROR;
        return this.returnMap;
      }
    }
    this.needCalcFileArrayString = this.needCalcFileArrayString.substring(0, this.needCalcFileArrayString.length() - 1);
    try
    {
      writeLocalLog("创建一个job...");
      Job job = createJob(this.needCalcFileArrayString);
      writeLocalLog("job创建完毕。jobID=" + job.getJobID());
      writeLocalLog("提交job...");
      job.submit();
    } catch (Exception ex) {
      String errMsg = "submit mr-job failed";
      LOG.error(errMsg, ex);
      writeLocalLog(ex, errMsg);
      this.result = JobResult.ERROR;
      this.returnMap.put("MAPREDUCE_ERROR", "submit mr-job failed : [" + ex.getMessage() + "]");
      return this.returnMap;
    }
    Job job = null;
    this.currentJobId = job.getJobID().toString();
    this.returnMap.put("ACT.VAR.JOB_ID", job.getJobID().toString());
    writeLocalLog("保存job信息...");
    try
    {
      boolean isSucc = job.waitForCompletion(true);
      if (!isSucc) {
        String errMsg = "excute mr-job[" + job.getJobID() + "] failed";
        LOG.error(errMsg);
        writeLocalLog(null, errMsg);
        this.result = JobResult.ERROR;
        this.returnMap.put("MAPREDUCE_ERROR", "excute mr-job[" + job.getJobID() + "] failed");
        throw new RuntimeException("excute mr-job[" + job.getJobID() + "] failed");
      }
    } catch (Exception ex) {
      String errMsg = "excute mr-job[" + job.getJobID() + "] failed ";
      LOG.error(errMsg, ex);
      writeLocalLog(ex, errMsg);
      this.result = JobResult.ERROR;
      this.returnMap.put("MAPREDUCE_ERROR", "excute mr-job[" + job.getJobID() + "] failed : [" + ex.getMessage() + "]");
      return this.returnMap;
    }
    writeLocalLog("job保存完毕.");
    try {
      CounterGroup file_LineNum_Group = (CounterGroup)job.getCounters().getGroup("file_LineNum_Group");
      for (Check check : this.needCalcFileList) {
        Counter counter = file_LineNum_Group.findCounter(check.fileName + "_size");
        if (counter != null) {
          check.fileSize = counter.getValue();
        }
        counter = file_LineNum_Group.findCounter(check.fileName + "_line");
        if (counter != null)
          check.recordNum = counter.getValue();
      }
    }
    catch (IOException ex) {
      String errMsg = "excute mr-job[" + job.getJobID() + "] failed ";
      LOG.error(errMsg, ex);
      writeLocalLog(ex, errMsg);
      this.result = JobResult.ERROR;
      this.returnMap.put("MAPREDUCE_ERROR", "excute mr-job[" + job.getJobID() + "] failed : [" + ex.getMessage() + "]");
      return this.returnMap;
    }

    DataTimeUtil dtu = new DataTimeUtil();
    String newDataTime;
    Object dayOrMonth;
    Object tableName;
    String errMsg;
    for (Check check : this.needCalcFileList) {
      newDataTime = null;
      String sql = "select distinct DATE_OP_TYPE,DATE_OP_VALUE,DAY_OR_MONTH_,TABLE_NAME_ from CFG_TABLE_INTERFACE where 1=1 ";
      if (StringUtils.trimToNull(check.tablename) != null) {
        sql = sql + " and lower(TABLE_NAME_)=lower('" + check.tablename + "')";
      }
      List list = excuteQuerySQL(sql);
      if ((list != null) && (list.size() > 0)) {
        String opT = (String)((Object[])list.get(0))[0];
        String opV = (String)((Object[])list.get(0))[1];
        dayOrMonth = ((Object[])list.get(0))[2];
        tableName = ((Object[])list.get(0))[3];
        if ((StringUtils.trimToNull(opT) != null) && (StringUtils.trimToNull(opV) != null)) {
          newDataTime = dtu.dealDate(env, dataTime, opT, opV, "yyyyMMdd", "yyyy-MM-dd");
          if ("1".equals(String.valueOf(dayOrMonth))) {
            newDataTime = newDataTime.substring(0, newDataTime.length() - 2) + "01";
            LOG.info("ABCDEFGHIJKLMNOPQRSTUVWXYZ=====" + newDataTime + "=====ABCDEFGHIJKLMN");
          }
        }
      }
      try
      {
        check.newFullName = ("/asiainfo/SDDW/ODS/" + check.tablename + "/pt_time_=" + newDataTime + "/" + check.fileName);
        LOG.info("ABCDEFGHIJKLMNOPQRSTUVWXYZ===FULLFILENAMDE==" + newDataTime + "=====ABCDEFGHIJKLMN");
      }
      catch (Exception ex) 
      {
        errMsg = "mv files to [" + check.newFullName + "] failed ";
        LOG.error(errMsg, ex);
        writeLocalLog(ex, errMsg);
        this.result = JobResult.ERROR;
        this.returnMap.put("HDFS_ERROR", "mv files to [" + check.newFullName + "] failed [" + ex.getMessage() + "]");
        return this.returnMap;
      }
    }

    writeLocalLog("拼装更新表STATUS_ET_FILE的sql....");
    List sqlList = new ArrayList();
    Path sourcePath;
    for (String sourcePathString : this.sourceMap.keySet()) 
    {
      sourcePath = new Path(sourcePathString);
      try {
        FileStatus[] sourceFiles = this.fs.listStatus(sourcePath);
//        dayOrMonth = (tableName = sourceFiles).length; 
        tableName = sourceFiles;
        dayOrMonth = sourceFiles.length; 
        for (int i = 0; i < sourceFiles.length; i++) 
        { 
          FileStatus sourceFile =(FileStatus) ((Object[]) tableName)[i];
//          FileStatus sourceFile =sourceFiles[i];
          String sql = "UPDATE ";
          sql = sql + "STATUS_ET_FILE ";
          sql = sql + "SET TRANS_STATUS_=1, TRANS_END_TIME_=NOW(), FLOW_INST_ID_='" + this.instanceId + "' ";
          sql = sql + "WHERE FILE_NAME_='" + sourceFile.getPath().getName() + "'";
          sqlList.add(sql); 
        }
      }
      catch (Exception ex) {
        errMsg = "list files in [" + sourcePathString + "] on HDFS failed";
        LOG.error(errMsg, ex);
        writeLocalLog(ex, errMsg);
        this.result = JobResult.ERROR;
        this.returnMap.put("HDFS_ERROR", "list files in [" + sourcePathString + "] on HDFS failed : [" + ex.getMessage() + "]");

        return this.returnMap;
      }
    }
    writeLocalLog("sql拼装完成。");

    writeLocalLog("开始批量执行update STATUS_ET_FILE 的sql");
    try {
      excuteBatchSQL(sqlList);
    } catch (Exception ex) {
      errMsg = "update files status on table[STATUS_ET_FILE] failed";
      LOG.error(errMsg, ex);
      writeLocalLog(ex, errMsg);
      this.result = JobResult.ERROR;
      this.returnMap.put("DB_ERROR", "update files status on table[STATUS_ET_FILE] failed : [" + ex.getMessage() + "]");
      return this.returnMap;
    }
    writeLocalLog("sql批量执行完毕");

    writeLocalLog("拼装sql，INSERT INTO STATUS_LD_FILE...");
    List sqlList2 = new ArrayList();
    for (Check check : this.needCalcFileList)
    {
      String sql = "INSERT INTO STATUS_LD_FILE ";
      sql = sql + "SET DATA_TIME_='" + dataTime + "' , " + 
        "FILE_NAME_='" + check.fileName + "' , " + 
        "TABLE_NAME_='" + check.tablename + "', " + 
        "FILE_PATH_='" + check.newFullName.substring(0, check.newFullName.length() - check.fileName.length()) + "' , " + 
        "FILE_SIZE_='" + check.fileSize + "' , " + 
        "FILE_RECORD_NUM_='" + check.recordNum + "' , " + 
        "FLOW_INST_ID_='" + this.instanceId + "' ";
      sqlList2.add(sql);
    }

    writeLocalLog("执行sql：INSERT INTO STATUS_LD_FILE...");
    try {
      excuteBatchSQL(sqlList2);
    } catch (Exception ex) {
      errMsg = "insert files status on table[STATUS_LD_FILE] failed";
      LOG.error(errMsg, ex);
      writeLocalLog(ex, errMsg);
      this.result = JobResult.ERROR;
      this.returnMap.put("DB_ERROR", "insert files status on table[STATUS_LD_FILE] failed : [" + ex.getMessage() + "]");
      return this.returnMap;
    }
    writeLocalLog("sql执行完毕.");
    return this.returnMap;
  }

  private List<Object[]> excuteQuerySQL(String sql) throws Exception
  {
    Session session = null;
    try {
      session = HibernateUtils.getSessionFactory().openSession();
      SQLQuery q = session.createSQLQuery(sql);
      List list = q.list();
      List localList1 = list;
      return localList1;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("excute SQL[" + sql + "] Failed", e);
      throw e;
    } finally {
      if (session != null)
        session.close();
    }
  }

  private void saveJobId(String activityName, String dataFlowId, String jobId)
  {
    Session session = null;
    try {
      session = HibernateUtils.getSessionFactory().openSession();
      session.beginTransaction();
      HisActivityDAO hisActDao = new HisActivityDAO(session);
      HisActivityImpl hisAct = hisActDao.find(activityName, dataFlowId);
      Map vars = hisAct.getOwnVariables();

      String skey = "ACT.VAR.JOB_IDS";
      if (vars.containsKey("ACT.VAR.JOB_ID")) {
        Object jobIds = vars.get(skey);
        if (jobIds == null)
          hisAct.putOwnVariable(skey, vars.get("ACT.VAR.JOB_ID") + "," + jobId);
        else {
          hisAct.putOwnVariable(skey, jobIds + "," + jobId);
        }
      }

      hisAct.putOwnVariable("ACT.VAR.JOB_ID", jobId);
      hisActDao.update(hisAct);
      session.getTransaction().commit();
    } catch (Exception e) {
      e.printStackTrace();
      session.getTransaction().rollback();
      throw new RuntimeException(e);
    } finally {
      if (session != null)
        session.close();
    }
  }

  private void initLocalLog(Environment env, Map<String, String> udfParams)
  {
    this.singleLog = TaskNodeLogger.isSingleLog();
    Object logObj = env.get("taskNodeLogger");
    if (logObj == null) {
      LOG.warn("the localLogger is not in Environment");
      this.singleLog = false;
    } else {
      this.localLogger = ((Logger)logObj);
    }
  }

  private void writeLocalLog(String info) {
    if (this.singleLog)
      this.localLogger.info(info);
  }

  private void writeLocalLog(Exception e, String errMsg) {
    if (this.singleLog) {
      if (StringUtils.isBlank(errMsg)) {
        errMsg = "execute " + getClass().getName() + " activity error." + AICloudETLExceptionUtil.getErrMsg(e);
      }
      this.localLogger.error(errMsg, e);
    }
  }

  public JobResult getState()
  {
    return this.result;
  }

  public static void main(String[] args) throws Exception {
    CheckFileLengthSpecial c = new CheckFileLengthSpecial();
    Map udfParams = new HashMap();
    udfParams.put("checkFilePath", "file:/d:/tmp/check/in/A_000NBA_F-NBS1.0-1001-000000-201302271700-0000.CHK");
    udfParams.put("dataDir", "file:/d:/tmp/check/in");
    udfParams.put("line_split", "\n");
    udfParams.put("codeFormat", "gbk");
    c.execute(null, udfParams);
    System.out.println(c.getState());
  }

  public void releaseResource(HisActivity arg0)
  {
    ActivityVariable var = (ActivityVariable)arg0.getActVariables().get("ACT.VAR.JOB_ID");
    if ((var != null) && (var.getValue() != null)) {
      Configuration conf = new Configuration();
      try {
        JobClient jc = new JobClient(conf);
        RunningJob job = jc.getJob(var.getValue().toString());
        if (job != null)
          job.killJob();
      }
      catch (IOException ex) {
        String errMsg = "shut down mr-job[" + var.getValue().toString() + "] failed ";
        LOG.error(errMsg, ex);
        writeLocalLog(ex, errMsg);
        this.result = JobResult.ERROR;
      }
    }
  }

  public Job createJob(String input) throws IOException
  {
    Configuration conf = new Configuration();
    conf.setStrings("read_file_list", input.split(","));
    Job job = Job.getInstance(conf, "ReadFileSize");
    job.setJarByClass(getClass());
    job.setNumReduceTasks(0);
    job.setMapperClass(ReadFileSizeMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setInputFormatClass(ReadFileSizeInputFormat.class);
    job.setSpeculativeExecution(false);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setJobName("ReadFileSize job");
    FileInputFormat.addInputPath(job, new Path("ignored"));
    return job;
  }

  private class BatchSQLWork
    implements Work
  {
    private List<String> sqlList = new ArrayList();

    public BatchSQLWork(List<String> sqlList)
    {
      this.sqlList = sqlList;
    }

    public void execute(Connection connection)
      throws SQLException
    {
      Statement sta = null;
      try {
        connection.setAutoCommit(false);
        sta = connection.createStatement();
        for (String sql : this.sqlList) {
          CheckFileLengthSpecial.LOG.info("execute sql[" + sql + "]");
          sta.execute(sql);
          connection.commit();
        }
      }
      catch (SQLException e) {
        throw e;
      } finally {
        if (sta != null) {
          sta.close();
        }
        connection.rollback();
      }
    }
  }

  private class Check
  {
    public String fullName;
    public String fileName;
    public long fileSize;
    public long recordNum;
    public String ifname;
    public String tablename;
    public String newFullName;

    private Check(String name)
    {
      this.fileName = name;
    }
  }
}
