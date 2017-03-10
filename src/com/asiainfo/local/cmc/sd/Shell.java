 package com.asiainfo.local.cmc.sd;
 
 import ch.ethz.ssh2.Connection;
 import ch.ethz.ssh2.Session;
 import ch.ethz.ssh2.StreamGobbler;
 import com.ailk.cloudetl.action.Execution;
 import com.ailk.cloudetl.commons.api.Environment;
 import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
 import com.ailk.cloudetl.dataflow.xmlmodel.Property;
 import com.ailk.cloudetl.dbservice.DBUtils;
 import com.ailk.cloudetl.dbservice.platform.PlatformProperty;
 import com.ailk.cloudetl.dbservice.platform.PlatformTypeProperty;
 import com.ailk.cloudetl.exception.AICloudETLRuntimeException;
 import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
 import com.ailk.cloudetl.ndataflow.api.constant.ActivityType;
 import com.ailk.cloudetl.ndataflow.api.constant.ActivityVariableScope;
 import com.ailk.cloudetl.ndataflow.intarnel.history.var.ActivityVariableImpl;
 import com.ailk.cloudetl.ndataflow.intarnel.history.var.ActivityVariableParser;
 import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
 import com.ailk.cloudetl.script.ScriptEnv;
 import com.ailk.cloudetl.script.ScriptUtil;
 import java.io.BufferedReader;
 import java.io.InputStream;
 import java.io.InputStreamReader;
 import java.io.PrintStream;
 import java.io.PrintWriter;
 import java.nio.charset.Charset;
 import java.util.ArrayList;
 import java.util.Date;
 import java.util.HashMap;
 import java.util.Iterator;
 import java.util.List;
 import java.util.Map;
 import java.util.Random;
 import java.util.Set;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 import net.sf.json.JSONObject;
 import org.apache.commons.lang.StringUtils;
 import org.apache.commons.logging.Log;
 import org.apache.commons.logging.LogFactory;
 import org.apache.hadoop.mapred.JobClient;
 import org.apache.hadoop.mapred.JobConf;
 import org.apache.hadoop.mapred.JobStatus;
 import org.apache.hadoop.mapred.RunningJob;
 import org.apache.log4j.Logger;
 import org.hsqldb.lib.StringUtil;
 
 public class Shell extends Execution
 {
   private static final Log LOGGER = LogFactory.getLog(Shell.class);
   private String ip = null;
   private String usr = null;
   private String psword = null;
   private long time_out = 0L;
   private String cmd = null;
   private String outStr = null;
   private static Map paramMap = new HashMap();
 
   private boolean singleLog = true;
   private Logger localLogger = null;
 
   private void initLocalLog(Environment env, Map<String, String> udfParams) { this.singleLog = TaskNodeLogger.isSingleLog();
     Object logObj = env.get("taskNodeLogger");
     if (logObj == null) {
       LOGGER.info("the localLogger is not in Environment");
       try {
         this.localLogger = new TaskNodeLogger().getNodeLogger(ActivityType.SHELL.toString(), this.executionId);
         this.singleLog = true;
       } catch (Exception e) {
         this.singleLog = false;
       }
     } else {
       this.localLogger = ((Logger)logObj);
     } }
 
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
 
   protected void init() throws Exception {
     initLocalLog(EnvironmentImpl.getCurrent(), null);
     writeLocalLog("----------start shell init------------");
     com.ailk.cloudetl.shell.xml.Shell thisNode = (com.ailk.cloudetl.shell.xml.Shell)this.node;
     Map params = new HashMap();
 
     ScriptEnv scriptEnv = new ScriptEnv();
     scriptEnv.setVar("DFContext", new DFContext());
     Iterator keys = this.vars.keySet().iterator();
     while (keys.hasNext()) {
       String key = (String)keys.next();
       scriptEnv.setVar(key, this.vars.get(key));
     }
 
     ScriptUtil stuil = new ScriptUtil();
 
     String hql = "from " + com.ailk.cloudetl.dbservice.platform.Platform.class.getName() + 
       " p where p.platformName='" + 
       thisNode.getPlatform().getName() + "' and p.isInUse=1";
     List data = DBUtils.query(hql);
     if (data.size() == 0)
       throw new Exception("平台信息为空");
     com.ailk.cloudetl.dbservice.platform.Platform pf = (com.ailk.cloudetl.dbservice.platform.Platform)data.get(0);
 
     boolean isGroup = pf.getIsGroup() == null ? false : pf.getIsGroup().booleanValue();
     int randIndex = 0;
     Map pfMap = new HashMap();
     PlatformProperty pp;
     if (isGroup) {
       int maxIndex = 0;
       for (Iterator localIterator1 = pf.getPfProperties().iterator(); localIterator1.hasNext(); ) { pp = (PlatformProperty)localIterator1.next();
         if (pp.getPropIndex() >= maxIndex)
           maxIndex = pp.getPropIndex();
       }
       randIndex = new Random().nextInt(maxIndex + 1);
     }
 
     for (PlatformProperty pp1 : pf.getPfProperties()) {
       if ((!isGroup) || ((isGroup) && (pp1.getPropIndex() == randIndex))) {
         if ((pp1.getPfTypeProperty().getName().trim().equals("pwd")) && 
           (EnvironmentImpl.getCurrent().get(
           "com.ailk.cloudetl.password.encrypt") != null)) {
           pp1.decryptPwd();
         }
         ActivityVariableImpl var = ActivityVariableParser.parse(pp1.getPropValue());
         var.setScope(ActivityVariableScope.FLOW_IN);
         pfMap.put(pp1.getPfTypeProperty().getName(), var);
         params.put(pp1.getPfTypeProperty().getName(), pp1.getPropValue());
//         pfMap.put(pp.getPfTypeProperty().getName(), var);
//         params.put(pp.getPfTypeProperty().getName(), pp.getPropValue());
       }
 
     }
 
//     for (Property pp : thisNode.getPlatform().getProperties()) {
     for (Property prp : thisNode.getPlatform().getProperties()) {
       params.put(prp.getName(), prp.getValue());
     }
 
     this.ip = ((String)params.get("host"));
     this.usr = ((String)params.get("user"));
     this.psword = ((String)params.get("pwd"));
     this.cmd = ((String)stuil.getScriptResult(scriptEnv, 
       (String)params.get("cmd")));
 
     if ((params.get("timeout") != null) && (!"".equals(params.get("timeout"))) && 
       (!"null".equals(params.get("timeout"))))
       try {
         this.time_out = Long.parseLong((String)params.get("timeout"));
       }
       catch (Exception localException)
       {
       }
     if (StringUtils.isEmpty(this.ip))
       throw new AICloudETLRuntimeException("参数ip不能为空");
     if (StringUtils.isEmpty(this.usr))
       throw new AICloudETLRuntimeException("参数usr不能为空");
     if (StringUtils.isEmpty(this.psword))
       throw new AICloudETLRuntimeException("参数psword不能为空");
     if (StringUtils.isEmpty(this.cmd))
       throw new AICloudETLRuntimeException("参数cmd不能为空");
     Map map = new HashMap();
     map.put("ip", this.ip);
     map.put("usr", this.usr);
     map.put("psword", this.psword);
     map.put("cmd", this.cmd);
     paramMap.put(this.executionId, map);
   }
 
   protected void run(Map out) throws Exception {
     out.put("SHELL_HOST", this.ip);
     out.put("SHELL_USER", this.usr);
     out.put("SHELL_COMMAND", this.cmd);
     StringBuffer outInfo = new StringBuffer("<pre>");
 
     Connection conn = null;
     Session session = null;
     InputStream stdOut = null;
     InputStream stdErr = null;
     try {
       conn = new Connection(this.ip);
       conn.connect();
       if (!conn.authenticateWithPassword(this.usr, this.psword)) {
         throw new Exception("login remote machine failure");
       }
       session = conn.openSession();
 
       String defaultShellType = "bash";
       String getShellTypeCmd = "echo $0";
       String bash = "bash";
       String ksh = "ksh";
       String csh = "csh";
       defaultShellType = getShellType(conn, getShellTypeCmd);
       if (bash.equals(defaultShellType)) {
         defaultShellType = bash;
       }
       else if (ksh.equals(defaultShellType)) {
         defaultShellType = ksh;
       }
       else if (csh.equals(defaultShellType))
         defaultShellType = csh;
       else {
         defaultShellType = bash; } 
 this.LOG.info("shellType:" + defaultShellType);
       StringBuffer cmdInfo = new StringBuffer();
       Date start = new Date();
       cmdInfo.append("执行命令:" + this.cmd + "\n");
       cmdInfo.append("创建日期:" + start.toString() + "\n");
 
       session.requestPTY(defaultShellType);
       session.startShell();
       stdOut = new StreamGobbler(session.getStdout());
       stdErr = new StreamGobbler(session.getStderr());
       BufferedReader stdoutReader = new BufferedReader(
         new InputStreamReader(stdOut));
       BufferedReader stderrReader = new BufferedReader(
         new InputStreamReader(stdErr));
       PrintWriter outer = new PrintWriter(session.getStdin());
       outer.println(this.cmd);
       outer.println("exit");
       outer.close();
 
       StringBuilder sb_out = new StringBuilder();
       StringBuilder sb_err = new StringBuilder();
       String line;
       do { line = stdoutReader.readLine();
         String key = "Starting Job = ";
         if ((line != null) && (line.indexOf(key) > -1)) {
           String jobId = line.substring(
             line.indexOf(key) + key.length(), 
             line.indexOf(","));
 
           String skey = "ACT.VAR.JOB_IDS";
           if (this.vars.containsKey("ACT.VAR.JOB_ID")) {
             Object jobIds = this.vars.get(skey);
             if (jobIds == null)
               out.put(skey, 
                 this.vars.get("ACT.VAR.JOB_ID") + 
                 "," + jobId);
             else {
               out.put(skey, jobIds + "," + jobId);
             }
           }
           LOGGER.info("page jobId=" + jobId);
           out.put("ACT.VAR.JOB_ID", jobId);
         }
         sb_out.append(line + "\n\r"); }
       while (line != null);
 
       String outStr = sb_out.toString();
 
       String jobId = "";
       Pattern pattern = Pattern.compile("job_\\d+_\\d+");
       Matcher matcher = pattern.matcher(outStr);
       if (matcher.find()) {
         jobId = matcher.group();
       }
 
       Map map = (Map)paramMap.get(this.executionId);
       if (!StringUtil.isEmpty(jobId)) {
         LOGGER.info("--------get jobId -----:" + jobId);
         map.put("jobId", jobId);
         paramMap.put(this.executionId, map);
       }
//       String line;
       do { line = stderrReader.readLine();
         sb_err.append(line + "\n\r"); }
       while (line != null);
 
       String outErr = sb_err.toString();
 
       int returnCode = 0;
       if (this.time_out > 0L)
         returnCode = session.waitForCondition(34, 
           this.time_out);
       else {
         returnCode = session.waitForCondition(34, 
           0L);
       }
 
       Integer ret = session.getExitStatus();
 
       Date end = new Date();
       long use_time = end.getTime() - start.getTime();
       cmdInfo.append("消耗时间:").append(use_time).append("\n");
 
       boolean result = false;
 
       if ((outStr == null) || (outStr.indexOf("HIVE_SQL_EXEC_ERROR") <= -1))
       {
         if ((outErr == null) || (outErr.equals("")) || 
           (outErr.trim().equals("null")))
         {
           if (ret == null) {
             if (returnCode == 18)
               outStr = "服务终端[" + conn.getHostname() + ":" + 
                 conn.getPort() + "]连接断开\n" + outStr;
             else if (returnCode == 16)
               outStr = "服务终端[" + conn.getHostname() + ":" + 
                 conn.getPort() + "]SSH CHANNEL STATE IS EOF\n" + 
                 outStr;
           }
           else if (ret.intValue() == 0)
           {
             result = true;
           }
         }
       }
       if (result)
         cmdInfo.append("执行状态:").append("SUCCESS").append("\n");
       else {
         cmdInfo.append("执行状态:").append("FAILURE").append("\n");
       }
       cmdInfo.append("返回结果:\n")
         .append(outStr.replace("\r", "").replace("\n\n", "\n"))
         .append("\n");
 
       if (!result) {
         out.put("shellReturnMessage", "<pre>" + cmdInfo.toString() + 
           "</pre>");
         throw new Exception("远程调用脚本出错:\n" + 
           outStr.replace("\r", "").replace("\n\n", "\n"));
       }
 
       outInfo.append(cmdInfo);
 
       Thread.sleep(1000L);
     } catch (Exception e) {
       throw e;
     } finally {
       try {
         if (stdErr != null)
           stdErr.close();
       } catch (Exception localException1) {
       }
       try {
         if (stdOut != null)
           stdOut.close();
       } catch (Exception localException2) {
       }
       try {
         if (session != null)
           session.close();
       } catch (Exception localException3) {
       }
       try {
         if (conn != null)
           conn.close();
       } catch (Exception e4) {
         this.LOG.error(e4);
       }
     }
   }
 
   protected void clear()
   {
   }
 
   public String getShellType(Connection conn, String type)
   {
     String returnType = null;
     InputStream stdOut = null;
     InputStream stdErr = null;
     Session session = null;
     try {
       String charset = Charset.defaultCharset().toString();
 
       session = conn.openSession();
       session.execCommand(type);
       stdOut = new StreamGobbler(session.getStdout());
       stdErr = new StreamGobbler(session.getStderr());
       if (this.time_out != 0L)
         session.waitForCondition(50, 
           this.time_out);
       else {
         session.waitForCondition(50, 
           0L);
       }
       Integer ret = session.getExitStatus();
       String outStr = processStream(stdOut, charset);
       String outErr = processStream(stdErr, charset);
       if ((ret == null) || (ret.intValue() != 0)) {
         returnType = null;
       }
       else if (ret.intValue() == 0)
         returnType = outStr.trim();
       else
         returnType = null;
     }
     catch (Throwable e)
     {
       e.printStackTrace();
       try
       {
         if (stdOut != null)
           stdOut.close();
       } catch (Exception localException) {
       }
       try {
         if (stdErr != null)
           stdErr.close();
       } catch (Exception localException1) {
       }
       try {
         if (session != null)
           session.close();
       }
       catch (Exception localException2)
       {
       }
     }
     finally
     {
       try
       {
         if (stdOut != null)
           stdOut.close();
       } catch (Exception localException3) {
       }
       try {
         if (stdErr != null)
           stdErr.close();
       } catch (Exception localException4) {
       }
       try {
         if (session != null)
           session.close();
       } catch (Exception localException5) {
       }
     }
     return returnType;
   }
 
   private String processStream(InputStream in, String charset) throws Exception
   {
     byte[] buf = new byte[1024];
     StringBuilder sb = new StringBuilder();
     while (in.read(buf) != -1) {
       sb.append(new String(buf, charset) + "\n\r");
     }
     return sb.toString();
   }
 
   protected void releaseResource(String executionId) throws Exception
   {
     LOGGER.info("start release executionId=" + executionId);
     String jobId = "";
     Map map = (Map)paramMap.get(executionId);
     if (map == null) {
       return;
     }
     if (map.containsKey("jobId")) {
       LOGGER.info("jobId release:" + jobId);
       jobId = (String)map.get("jobId");
       JobConf conf = new JobConf();
       JobClient jc = new JobClient(conf);
       JobStatus[] js = jc.getAllJobs();
       for (int i = 0; i < js.length; i++) {
         if (((js[i].getRunState() != 1) && (js[i].getRunState() != 4)) || 
           (!jobId.equals(js[i].getJobID()))) continue;
         RunningJob rj = jc.getJob(jobId);
         rj.killJob();
         LOGGER.info("jobId=" + jobId + " was killed:" + jobId);
       }
 
     }
 
     String ip = "";
     String usr = "";
     String psword = "";
     String cmd = "";
     if (map != null) {
       ip = (String)map.get("ip");
       usr = (String)map.get("usr");
       psword = (String)map.get("psword");
       cmd = (String)map.get("cmd");
     }
 
     Connection conn = conn = new Connection(ip);
     conn.connect();
     if (!conn.authenticateWithPassword(usr, psword)) throw new Exception("login remote machine failure");
     LOGGER.info("cmd process killed start");
     killProcess(conn, cmd, usr);
     paramMap.remove(executionId);
   }
 
   public void killProcess(Connection conn, String cmd, String usr)
   {
     LOGGER.info("----------start kill shell ------------");
     InputStream stdOut = null;
     InputStream stdErr = null;
     String shellName = getShellNameFromCmd(cmd);
     String psCmd = "ps -ef | grep '" + cmd + "'";
     String parentPID = null;
     try { Session session = conn.openSession();
       session.execCommand(psCmd);
 
       stdOut = new StreamGobbler(session.getStdout());
       stdErr = new StreamGobbler(session.getStderr());
       BufferedReader stdoutReader = new BufferedReader(
         new InputStreamReader(stdOut));
       BufferedReader stderrReader = new BufferedReader(
         new InputStreamReader(stdErr));
 
       StringBuilder sb_out = new StringBuilder();
       StringBuilder sb_err = new StringBuilder();
       String line;
       do { line = stdoutReader.readLine();
         sb_out.append(line + "\n\r"); } while (line != null);
 
       String outStr = sb_out.toString();
       LOGGER.info("psCmd:" + psCmd + " execute outStr=" + outStr);
//       String line;
       do { line = stderrReader.readLine();
         sb_err.append(line + "\n\r"); }
       while (line != null);
 
       String outErr = sb_err.toString();
       LOGGER.info("outErr=" + outErr);
 
       String shellPath = getShellPathFromCmd(cmd);
       LOGGER.info("shellPath:" + shellPath);
       parentPID = getParentPIDFromOutStr(outStr, shellPath, usr);
       LOGGER.info("parentPID:" + parentPID);
 
       if (parentPID != null) { int pid = Integer.parseInt(parentPID);
         int childPid = pid;
         LOGGER.info("pid=" + pid + ",childPid=" + childPid);
         Integer result;
         do { result = null;
           try
           {
             childPid++;
             Session newSession = conn.openSession();
             result = killProcess(newSession, childPid);
             LOGGER.info("kill childPid=" + childPid);
             if (newSession != null) {
               newSession.close();
             }
           }
           catch (Throwable localThrowable)
           {
           }
         }
         while ((result != null) && (result.intValue() == 0));
 
         Session newSession = conn.openSession();
         result = killProcess(newSession, pid);
//         Integer result = killProcess(newSession, pid);
         if (newSession != null) {
           newSession.close();
         }
       }
 
       if (session != null)
         session.close();
     }
     catch (Throwable localThrowable1)
     {
     }
   }
 
   public String getParentPIDFromOutStr(String outStr, String shellPath, String user)
   {
     List list = new ArrayList();
     String jobId = "";
     String parentPID = null;
     String[] array = outStr.split("\n\r");
     for (int i = 0; i < array.length; i++)
     {
       String proc_str = array[i];
       String newStr = proc_str.replaceAll(" ", "#");
       String[] newArray = newStr.split("#");
       for (int k = 0; k < newArray.length; k++) {
         if (newArray[k].trim().contains(shellPath.trim())) {
           for (int j = 0; j < newArray.length; j++) {
             if ((newArray[j] != null) && (!"".equals(newArray[j]))) {
               list.add(newArray[j].trim());
             }
           }
         }
       }
     }
 
     LOGGER.info("list.size():" + list.size());
     LOGGER.info("usr:" + user);
 
     if (list.size() > 0) {
       for (int i = 0; i < list.size(); i++) {
         if ((!user.trim().equals(list.get(0))) || 
           (list.get(1) == null)) continue;
         int tempPID = 0;
         try {
           tempPID = Integer.parseInt((String)list.get(1));
         } catch (Exception localException) {
         }
         if (tempPID != 0) {
           parentPID = tempPID+"";
         }
 
       }
 
     }
 
     return parentPID;
   }
 
   public Integer killProcess(Session session, int pid)
     throws Exception
   {
     Integer result = null;
     String killChildProc = "kill -9 " + pid;
     session.execCommand(killChildProc);
     LOGGER.info("killChildProc cmd:" + killChildProc);
     result = session.getExitStatus();
     LOGGER.info("session status:" + result);
 
     return result;
   }
 
   public String getShellNameFromCmd(String cmd) {
     String[] array = cmd.split(" ");
     boolean startAppend = false;
     StringBuilder sb = new StringBuilder();
     for (int i = 0; i < array.length; i++) {
       if (startAppend) {
         sb.append(" ");
         sb.append(array[i]);
       }
       if (array[i].trim().contains(".")) {
         String[] newarray = array[i].trim().split("/");
 
         for (int j = 0; j < newarray.length; j++) {
           if (newarray[j].trim().contains(".")) {
             sb.append(newarray[j].trim());
             startAppend = true;
           }
         }
       }
 
     }
 
     return sb.toString();
   }
 
   public String getShellPathFromCmd(String cmd) {
     boolean startAppend = false;
     StringBuilder sb = new StringBuilder();
     String[] array = cmd.split(" ");
     for (int i = 0; i < array.length; i++) {
       if (startAppend) {
         sb.append(" ");
         sb.append(array[i]);
       }
       if (array[i].trim().contains(".")) {
         sb.append(array[i].trim());
         startAppend = true;
       }
     }
 
     return sb.toString();
   }
 
   public String getParentPath(String cmd)
   {
     String path = getShellPathFromCmd(cmd);
     String shellName = getShellNameFromCmd(cmd);
     if (path.length() == shellName.length()) {
       return "./.";
     }
     return path.substring(0, path.length() - shellName.length());
   }
 
   public static void main(String[] args) {
     String str = "{\"test3.txt\":{\"FILE_SIZE\":\"22726918\",\"FIlE_NAME\":\"test3.txt\"}}";
     JSONObject j = JSONObject.fromObject(str);
     System.out.println(j.get("test3.txt"));
   }
 }