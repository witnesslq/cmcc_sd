 package com.asiainfo.local.cmc.sd;
 
 import com.ailk.cloudetl.commons.api.Environment;
 import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
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
 import com.ailk.udf.node.ctc.ods.SysConfig;
 import com.ailk.udf.node.shandong.ftp.FTPClientAdapter;
 import com.ailk.udf.node.shandong.ftp.FtpUDFUtil;
 import com.ailk.udf.node.shandong.ftp.exception.FTPConnectException;
 import com.ailk.udf.node.shandong.ftp.exception.FTPLoginException;
 import com.ailk.udf.node.shandong.ftp.mr.FTPInputFormat;
 import com.ailk.udf.node.shandong.ftp.mr.FtpMapper;
 import java.io.IOException;
 import java.net.URI;
 import java.sql.Connection;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.SQLException;
 import java.util.ArrayList;
 import java.util.Arrays;
 import java.util.Comparator;
 import java.util.HashMap;
 import java.util.List;
 import java.util.Map;
 import org.apache.commons.lang.ArrayUtils;
 import org.apache.commons.lang.StringUtils;
 import org.apache.commons.logging.Log;
 import org.apache.commons.logging.LogFactory;
 import org.apache.commons.net.ftp.FTPFile;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.filecache.DistributedCache;
 import org.apache.hadoop.fs.FileStatus;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.mapred.JobClient;
 import org.apache.hadoop.mapred.RunningJob;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.JobID;
 import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
 import org.apache.log4j.Logger;
 import org.hibernate.Session;
 import org.hibernate.SessionFactory;
 import org.hibernate.Transaction;
 import org.hibernate.jdbc.Work;
 
 public class FtpExtractUDF
   implements UDFActivity
 {
   private static final Log log = LogFactory.getLog(FtpExtractUDF.class);
   private String fromPath;
   private String toPath;
   private FTPClientAdapter ftp = new FTPClientAdapter();
 
   private String sdate = null;
 
   private JobResult result = JobResult.RIGHT;
 
   Map<String, Object> returnMap = new HashMap();
   private static final String ERROR = "ERROR_MSG";
   private static final String KEY_HOST = "hostname";
   private static final String KEY_PWD = "pwd";
   private static final String KEY_USER = "user";
   private boolean singleLog = true;
   private Logger localLogger = null;
 
   public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
   {
     String platform = (String)udfParams.get("platform");
 
     this.fromPath = ((String)udfParams.get("ftppath"));
 
     this.toPath = ((String)udfParams.get("hdfspath"));
 
     this.sdate = ((String)udfParams.get("date"));
 
     String reg = (String)udfParams.get("reg");
 
     String fcode = (String)udfParams.get("fcode");
 
     String tcode = (String)udfParams.get("tcode");
 
     String maptasknum = (String)udfParams.get("maptasknum");
 
     String important = (String)udfParams.get("important");
 
     initLocalLog(env, udfParams);
     StringBuilder msg = new StringBuilder().append("\n基本信息：\nftp路径：").append(this.fromPath)
       .append("\n本地路径").append(this.toPath).append("\n文件日期：").append(this.sdate)
       .append("\n正则表达式:").append(reg).append("\nFrom编码:").append(fcode)
       .append("\nTo编码:").append(tcode).append("\nMap数量:").append(maptasknum).append("\n优先级:").append(important);
     log.info(msg.toString());
     writeLocalLog(msg.toString());
     try
     {
       Map platMap = FtpUDFUtil.getPlateInfo(platform);
       String host = String.valueOf(platMap.get("hostname"));
       String user = String.valueOf(platMap.get("user"));
       String pwd = String.valueOf(platMap.get("pwd"));
       msg = new StringBuilder().append("host:").append(host).append(";user:").append(user)
         .append(";password:").append(pwd);
       log.info(msg);
       writeLocalLog(msg.toString());
 
       this.ftp.setUser(user);
       this.ftp.setPwd(pwd);
       this.ftp.setHost(host);
 
       FTPFile[] files = (FTPFile[])null;
       try {
         files = this.ftp.listRemoteNames(this.fromPath, reg, true);
       }
       catch (FTPLoginException e1) {
         writeLocalLog(e1, e1.getMessage());
       } catch (FTPConnectException e1) {
         writeLocalLog(e1, e1.getMessage());
       }
 
       files = delExitedFile(Arrays.asList(files), this.sdate, important);
       log.info("需要抽取" + files.length + "个文件");
       writeLocalLog("需要抽取" + files.length + "个文件");
 
       if (files.length == 0) {
         this.result = JobResult.RIGHT;
         this.returnMap.put("ERROR_MSG", "没有要抽取的文件。");
         return this.returnMap;
       }
 
       sortFiles(files);
 
       log.info("添加ETL类路径 classpath");
       writeLocalLog("添加ETL类路径 classpath");
 
       Configuration conf = new Configuration();
       FileSystem fs = FileSystem.get(conf);
       FileStatus[] jarFiles = fs.listStatus(new Path("/classpath232"));
       if (!ArrayUtils.isEmpty(jarFiles)) {
         for (FileStatus file : jarFiles) {
           DistributedCache.addFileToClassPath(new Path(file.getPath().toUri().getPath()), conf);
         }
       }
       log.info("添加ETL配置路径 conf");
       writeLocalLog("添加ETL配置路径 conf");
 
       FileStatus[] confFiles = fs.listStatus(new Path("/conf232"));
       if (!ArrayUtils.isEmpty(confFiles)) {
         for (FileStatus file : confFiles) {
           DistributedCache.addArchiveToClassPath(new Path(file.getPath().toUri().getPath()), conf);
         }
       }
       log.info("传入基本参数");
       writeLocalLog("传入基本参数:fromPath=" + this.fromPath + ",toPath=" + this.toPath + ",sdate=" + this.sdate);
       conf.set("FROM_PATH", this.fromPath);
       conf.set("TO_PATH", this.toPath);
       conf.set("S_DATE", this.sdate);
 
       log.info("序列化 抽取的文件数组为json");
       conf.set("FILE_JSON", new FtpUDFUtil().objToJson(files));
       conf.set("platform", new FtpUDFUtil().objToJson(platMap));
 
       String url = SysConfig.getValue("db.url");
       String username = SysConfig.getValue("db.user");
       String password = SysConfig.getValue("db.pwd");
       String dbclass = SysConfig.getValue("db.class");
       conf.set("DB_USER", username);
       conf.set("DB_PWD", password);
       conf.set("DB_URI", url);
       conf.set("DB_CLASS", dbclass);
 
       conf.set("F_CODE", fcode);
       conf.set("T_CODE", tcode);
 
       log.info("mapred.map.tasks.speculative.execution = " + conf.get("mapred.map.tasks.speculative.execution"));
       writeLocalLog("mapred.map.tasks.speculative.execution = " + conf.get("mapred.map.tasks.speculative.execution"));
       conf.set("mapred.map.tasks.speculative.execution", "false");
 
       log.info("序列化成功");
       writeLocalLog("序列化成功");
 
       log.info("启动一个job");
       writeLocalLog("启动一个job");
       conf.set("ocdc.maptask.num", maptasknum);
       Job job = Job.getInstance(conf, getClass().getName());
       log.info("初始化job成功");
       writeLocalLog("初始化job成功");
       job.setJobName("FTP_UDF");
       job.setJarByClass(getClass());
       job.setMapperClass(FtpMapper.class);
       job.setNumReduceTasks(0);
       job.setInputFormatClass(FTPInputFormat.class);
       job.setOutputFormatClass(NullOutputFormat.class);
 
       log.info("提交job");
       writeLocalLog("提交job,jobId=" + job.getJobID());
       job.submit();
       this.returnMap.put("ACT.VAR.JOB_ID", job.getJobID().toString());
       DataFlowContext context = (DataFlowContext)EnvironmentImpl.getCurrent().get("DFContext");
 
       if (!job.waitForCompletion(true)) {
         String msgs = "--job成功启动，map中的代码产生了问题";
         log.info(msgs);
         writeLocalLog(msgs);
         this.returnMap.put("ERROR_MSG", msgs);
         this.result = JobResult.ERROR;
       } else {
         writeLocalLog("job执行成功");
       }
 
     }
     catch (Exception e)
     {
       this.result = JobResult.ERROR;
       this.returnMap.put("ERROR_MSG", e.getMessage());
       log.info("异常信息");
       writeLocalLog(e, e.getMessage());
       return this.returnMap;
     }
     return this.returnMap;
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
       log.warn("the localLogger is not in Environment");
       this.singleLog = false;
     } else {
       this.localLogger = ((Logger)logObj);
     }
   }
 
   private void writeLocalLog(String info) {
     if (this.singleLog)
       this.localLogger.info(info);
   }
 
   private void writeLocalLog(Exception e, String errMsg)
   {
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
         log.error(errMsg, ex);
         writeLocalLog(ex, errMsg);
         this.result = JobResult.ERROR;
       }
     }
   }
 
   private FTPFile[] delExitedFile(List<FTPFile> allList, String sdate, String important)
   {
     final String querySql = "select FILE_NAME_ from STATUS_ET_FILE a,( select if_no_,ftp_important_ from cfg_table_interface where ftp_important_ is not null ) b  where a.if_no_ = b.if_no_ and a.STATUS_ = 1 and a.DATA_TIME_ = " + sdate + " and b.ftp_important_ = " + important;
     log.info("查询已经下载的文件：" + querySql + "===== select FILE_NAME_ from STATUS_ET_FILE a,( select if_no_,ftp_important_ from cfg_table_interface where ftp_important_ is not null ) b  where a.if_no_ = b.if_no_ and a.STATUS_ = 1 and a.DATA_TIME_ =" + sdate + " and b.ftp_important_ = " + important);
 
     final String querySql2 = "select  if_no_  from cfg_table_interface b where b.ftp_important_ = " + important;
     log.info("查询需要下载的接口：" + querySql2 + "=====select  if_no_  from cfg_table_interface b where b.ftp_important_ = " + important);
 
     log.info("important=====" + important + "==sdate" + sdate);
 
     List<String> exitedList = dbOperation(querySql, 0);
     List<String> wantedIfNoStr = dbOperation(querySql2, 0);
     List<FTPFile> allListFilterByIfNo = new ArrayList<FTPFile>();
     List<FTPFile> resultList = new ArrayList<FTPFile>();
 
     StringBuffer sb1 = new StringBuffer();
     StringBuffer sb2 = new StringBuffer();
     StringBuffer sb3 = new StringBuffer();
     StringBuffer sb4 = new StringBuffer();
 
     for (int i = 0; i < wantedIfNoStr.size(); i++) {
       sb3.append((String)wantedIfNoStr.get(i) + "\n");
     }
 
     log.info("======100000========  wanted  ifno:" + sb3);
 
     for (int i = 0; i < exitedList.size(); i++) {
       sb4.append((String)exitedList.get(i) + "\n");
     }
 
     log.info("======200000========  existed record from status_et_file:" + sb4);
 
     for (FTPFile f : allList) {
       for (int i = 0; i < wantedIfNoStr.size(); i++) {
         if (f.getName().toUpperCase().indexOf(((String)wantedIfNoStr.get(i)).toString().toUpperCase()) == 0) {
           sb1.append(f.getName() + "\n");
           allListFilterByIfNo.add(f);
           break;
         }
       }
     }
 
     log.info("======300000========  ftp file from server filter by ifno:" + sb1);
 
     if ((allListFilterByIfNo != null) && (allListFilterByIfNo.size() > 0)) {
       for (FTPFile f : allListFilterByIfNo) {
         if (exitedList.contains(f.getName()))
           continue;
         if ((f.getName().length() == 24) || (f.getName().length() == 26)) {
           if (((f.getName().charAt(14) >= '0') && (f.getName().charAt(14) <= '9')) || ((f.getName().charAt(14) >= 'a') && (f.getName().charAt(14) <= 'z')))
             resultList.add(f);
         }
         else {
           resultList.add(f);
         }
         sb2.append(f.getName() + "\n");
       }
 
     }
 
     log.info("======400000========  reallay ftp file from server filter by ifno and exist record from status_et_file:" + sb2);
 
     FTPFile[] result = (FTPFile[])resultList.toArray(new FTPFile[resultList.size()]);
     return result;
   }
 
   private List<String> dbOperation(final String sql, final int type)
   {
     final List exitedFileList = new ArrayList();
     Session session = null;
     log.info("申请hibernate数据库会话");
     writeLocalLog("申请hibernate数据库会话");
     try {
       session = HibernateUtils.getSessionFactory().openSession();
       log.info("---------db连接正常，成功申请会话");
       writeLocalLog("---------db连接正常，成功申请会话");
       session.doWork(new Work()
       {
         public void execute(Connection connection) throws SQLException {
           FtpExtractUDF.log.info("---------进入执行SQL代码块");
           FtpExtractUDF.this.writeLocalLog("---------进入执行SQL代码块");
           PreparedStatement sta = null;
           try {
             sta = connection.prepareStatement(sql);
             FtpExtractUDF.log.info("---------开始执行SQL查询:[" + sql + "]");
             FtpExtractUDF.this.writeLocalLog("---------开始执行SQL查询:[" + sql + "]");
             ResultSet rs = sta.executeQuery();
             FtpExtractUDF.log.info("---------SQL执行成功");
             FtpExtractUDF.this.writeLocalLog("---------SQL执行成功");
             if (type == 0) {
               while (rs.next())
               {
                 exitedFileList.add(rs.getString(1));
               }
             } else {
               int code = sta.executeUpdate(sql);
               FtpExtractUDF.log.info("execute sql[" + sql + "] return code=" + code);
               FtpExtractUDF.this.writeLocalLog("execute sql[" + sql + "] return code=" + code);
             }
             FtpExtractUDF.log.info("---------事务开始提交");
             FtpExtractUDF.this.writeLocalLog("---------事务开始提交");
             connection.commit();
             FtpExtractUDF.log.info("---------事务完成提交");
             FtpExtractUDF.this.writeLocalLog("---------事务完成提交");
           } finally {
             sta.close();
           }
         } } );
     } catch (Exception e) {
       e.printStackTrace();
       writeLocalLog(e, null);
     } finally {
       if (session != null) {
         session.close();
       }
     }
     return exitedFileList;
   }
 
   private static void sortFiles(FTPFile[] fs)
   {
     Arrays.sort(fs, new Comparator<FTPFile>()
     {
       public int compare(FTPFile o1, FTPFile o2) {
         long o1size = o1.getSize();
         return o1size < o2.getSize() ? -1 : o1size == o2.getSize() ? 0 : 1;
       }
     });
   }
 
   public List<String> listFileName(List<FTPFile> wantedFiles) {
     List retList = null;
     if (wantedFiles == null) {
       return null;
     }
     retList = new ArrayList();
     for (FTPFile ftpFile : wantedFiles) {
       retList.add(ftpFile.getName());
     }
     return retList;
   }
 }
