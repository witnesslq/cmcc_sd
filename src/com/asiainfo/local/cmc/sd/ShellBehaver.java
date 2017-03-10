 package com.asiainfo.local.cmc.sd;
 
 import ch.ethz.ssh2.Connection;
 import ch.ethz.ssh2.InteractiveCallback;
 import ch.ethz.ssh2.StreamGobbler;
 import com.ailk.cloudetl.commons.api.Environment;
 import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
 import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
 import com.ailk.cloudetl.exception.AICloudETLRuntimeException;
 import com.ailk.cloudetl.ndataflow.api.ActivityVariable;
 import com.ailk.cloudetl.ndataflow.api.DataFlowInstance;
 import com.ailk.cloudetl.ndataflow.api.HisActivity;
 import com.ailk.cloudetl.ndataflow.api.constant.ActivityVariableScope;
 import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
 import com.ailk.cloudetl.ndataflow.api.context.DataFlowContext;
 import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
 import com.ailk.cloudetl.ndataflow.intarnel.activity.ActivityBehaver;
 import com.ailk.cloudetl.ndataflow.intarnel.activity.ActivityBehaver.ExeResult;
 import com.ailk.cloudetl.ndataflow.intarnel.activityclass.ActivityInClassManager;
 import com.ailk.cloudetl.ndataflow.intarnel.history.model.HisActivityImpl;
 import com.ailk.cloudetl.ndataflow.intarnel.history.var.ActivityVariableImpl;
 import com.ailk.cloudetl.ndataflow.intarnel.history.var.ActivityVariableParser;
 import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
 import com.ailk.cloudetl.ndataflow.intarnel.model.ActivityClassImpl;
 import com.ailk.cloudetl.ndataflow.intarnel.model.ActivityImpl;
 import com.ailk.cloudetl.ndataflow.intarnel.model.ExecutionImpl;
 import com.ailk.cloudetl.ndataflow.intarnel.session.ActivityClassDAO;
 import com.ailk.cloudetl.ndataflow.intarnel.session.HisActivityDAO;
 import java.io.BufferedReader;
 import java.io.IOException;
 import java.io.InputStream;
 import java.io.InputStreamReader;
 import java.io.PrintWriter;
 import java.io.Serializable;
 import java.nio.charset.Charset;
 import java.util.ArrayList;
 import java.util.Collections;
 import java.util.Date;
 import java.util.HashMap;
 import java.util.List;
 import java.util.Map;
 import org.apache.commons.lang.StringUtils;
 import org.apache.commons.logging.Log;
 import org.apache.commons.logging.LogFactory;
 import org.apache.log4j.Logger;
 import org.hibernate.SessionFactory;
 import org.hibernate.Transaction;
 import org.hsqldb.lib.StringUtil;
 
 public class ShellBehaver
   implements ActivityBehaver
 {
   private static final Log LOG = LogFactory.getLog(ShellBehaver.class);
   private Map<String, String> paramMap = new HashMap();
 
   public Map<String, String> getParamMap() { return this.paramMap;
   }
 
   public void setParamMap(Map<String, String> paramMap)
   {
     this.paramMap = paramMap;
   }
 
   public ActivityBehaver.ExeResult execute(ExecutionImpl execution)
   {
     Environment env = EnvironmentImpl.getCurrent();
     DataFlowContext context = (DataFlowContext)env.get("DFContext");
 
     org.hibernate.Session session = (org.hibernate.Session)EnvironmentImpl.getCurrent().get(org.hibernate.Session.class);
     JobResult jobResult = JobResult.RIGHT;
     Logger localLogger = new TaskNodeLogger().getNodeLogger(execution.getDataFlowIns().getId(), execution.getActivityName());
 
     ActivityImpl activity = null;
     ActivityClassImpl actClass = null;
     ActivityInClassManager activityInClassManager = new ActivityInClassManager();
 
     String flowInsID = (String)env.get("jobLogId");
     try {
       activity = (ActivityImpl)execution.getActivity();
       ActivityVariableImpl var = ActivityVariableParser.parse(RmtShell.class.getName());
       var.setScope(ActivityVariableScope.ACT);
       activity.getVariables().put("ACT_EXEC_CLASS", var);
       activity.startRunning(execution);
 
       if (!StringUtils.isEmpty(activity.getActivityClass())) {
         ActivityClassDAO dao = new ActivityClassDAO((org.hibernate.Session)EnvironmentImpl.getCurrent().get(org.hibernate.Session.class));
         actClass = dao.findInUseByName(activity.getActivityClass());
       }
 
       session.getTransaction().commit();
 
       session.close();
       EnvironmentImpl.getCurrent().remove(org.hibernate.Session.class);
 
       if (actClass != null) {
         activityInClassManager.waitInQueue(execution, actClass, activity);
       }
 
       long actStartTime = System.currentTimeMillis() / 1000L;
 
       RmtShell rmtShell = new RmtShell();
 
       saveHostInfo(execution, activity, this.paramMap);
       Map actVars = rmtShell.execute(EnvironmentImpl.getCurrent(), this.paramMap);
 
       long actEndTime = System.currentTimeMillis() / 1000L;
 
       if (actVars == null) actVars = new HashMap();
 
       actVars.put("ACT_DEAL_TIME", Long.valueOf(actEndTime - actStartTime));
 
       if (actVars != null) {
         activity.setOwnVariables(actVars);
       }
       jobResult = rmtShell.getState();
     } catch (Throwable e) {
       if (LOG.isErrorEnabled())
         LOG.error("execute UDF method error.", e);
       localLogger.error("execute UDF method error.", e);
       throw new AICloudETLRuntimeException(e);
     }
     finally
     {
       if (actClass != null) {
         activityInClassManager.removeFromQueue(actClass.getName(), execution.getId());
       }
     }
     session = HibernateUtils.getSessionFactory().openSession();
     session.beginTransaction();
     EnvironmentImpl.getCurrent().set(org.hibernate.Session.class, session);
     if (jobResult == JobResult.ERROR)
       return ActivityBehaver.ExeResult.FAIL;
     return ActivityBehaver.ExeResult.SUCCESS;
   }
 
   private void saveHostInfo(ExecutionImpl execution, ActivityImpl activity, Map<String, String> paramMap) {
     org.hibernate.Session session = null;
     try {
       session = HibernateUtils.getSessionFactory().openSession();
       session.beginTransaction();
       HisActivityDAO hisActDao = new HisActivityDAO(session);
       HisActivityImpl hisAct = hisActDao.find(activity.getName(), execution.getDataFlowIns().getId());
       Map vars = hisAct.getFlowInVariables();
       if (vars == null)
         vars = new HashMap();
       vars = putActVariable(vars, "SHELL_HOST", (String)paramMap.get("host"));
       vars = putActVariable(vars, "SHELL_USER", (String)paramMap.get("user"));
       vars = putActVariable(vars, "SHELL_TIME_OUT", (String)paramMap.get("timeout"));
       HashMap parameterMap = new HashMap();
       parameterMap.put("SHELL_HOST", paramMap.get("host"));
       parameterMap.put("SHELL_USER", paramMap.get("user"));
       parameterMap.put("SHELL_TIME_OUT", paramMap.get("timeout"));
       parameterMap.put("SHELL_COMMAND", paramMap.get("cmd"));
       String psword = (String)paramMap.get("pwd");
       if (!StringUtil.isEmpty(psword)) {
         StringBuffer buffer = new StringBuffer();
         for (int index = 0; index < psword.length(); index++)
           buffer = buffer.append("*");
         vars = putActVariable(vars, "SHELL_PASSWORD", buffer.toString());
         parameterMap.put("SHELL_PASSWORD", buffer.toString());
       }
       hisAct.setFlowInVariables(vars);
       hisActDao.update(hisAct);
       activity.setOwnVariables(parameterMap);
       session.getTransaction().commit();
     } catch (Exception e) {
       LOG.error("Save HostInfo error.", e);
       session.getTransaction().rollback();
       throw new RuntimeException(e);
     } finally {
       if (session != null)
         session.close();
     }
   }
 
   private Map<String, ActivityVariable<?>> putActVariable(Map<String, ActivityVariable<?>> map, String key, String value)
   {
     if ((StringUtil.isEmpty(key)) || (StringUtil.isEmpty(value)))
       return map;
     ActivityVariableImpl var = ActivityVariableParser.parse(value);
     var.setScope(ActivityVariableScope.FLOW_IN);
     LOG.info("Put activity variable ==> key:" + key + ", value:" + value);
     map.put(key, var);
     return map;
   }
 
   private static class ExecInfo
     implements Serializable
   {
     private static final long serialVersionUID = 3167442330533841549L;
     private String cmd = null;
     private String state = null;
     private String returnstr = null;
     private Long use_time = Long.valueOf(0L);
     private Date createDate;
 
     public String getCmd()
     {
       return this.cmd;
     }
     public void setCmd(String cmd) {
       this.cmd = cmd;
     }
     public String getState() {
       return this.state;
     }
     public void setState(String state) {
       this.state = state;
     }
     public String getReturnstr() {
       return this.returnstr;
     }
     public void setReturnstr(String returnstr) {
       this.returnstr = returnstr;
     }
     public Long getUse_time() {
       return this.use_time;
     }
     public void setUse_time(Long use_time) {
       this.use_time = use_time;
     }
     public Date getCreateDate() {
       return this.createDate;
     }
     public void setCreateDate(Date createDate) {
       this.createDate = createDate;
     }
   }
 
   public static class RmtShell implements UDFActivity
   {
     private static final Log log = LogFactory.getLog(RmtShell.class);
 
     private JobResult jobResult = JobResult.RIGHT;
 
     public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
     {
       Map retMap = new HashMap();
       String ip = (String)udfParams.get("host");
       String usr = (String)udfParams.get("user");
       String psword = (String)udfParams.get("pwd");
       String time_out = (String)udfParams.get("timeout");
       String cmd = (String)udfParams.get("cmd");
       Long time = Long.valueOf(0L);
 
       log.info("udfParams:\n" + udfParams);
 
       if (StringUtils.isEmpty(ip)) {
         throw new AICloudETLRuntimeException("参数ip不能为空");
       }
       retMap.put("SHELL_HOST", ip);
       if (StringUtils.isEmpty(usr)) {
         throw new AICloudETLRuntimeException("参数usr不能为空");
       }
       retMap.put("SHELL_USER", usr);
       if (StringUtils.isEmpty(psword)) {
         throw new AICloudETLRuntimeException("参数psword不能为空");
       }
       retMap.put("SHELL_PASSWORD", psword);
       if (StringUtils.isEmpty(cmd)) {
         throw new AICloudETLRuntimeException("参数cmd不能为空");
       }
       retMap.put("SHELL_COMMAND", cmd);
       if ((time_out != null) && (!"".equals(time_out)) && (!"null".equals(time_out)))
         try {
           time = Long.valueOf(Long.parseLong("time_out"));
           retMap.put("SHELL_TIME_OUT", time);
         } catch (Exception localException1) {
         }
       else if ((time_out == null) || ("".equals(time_out))) {
         time = null;
       }
 
       ShellBehaver.RmtShellExecutor se = new ShellBehaver.RmtShellExecutor(ip, usr, psword, time);
       try
       {
         Map result = se.exec(cmd);
         retMap.put("shellReturnMessage", se.getOut((List)result.get("list_out")));
         if (!se.isResult()) {
           this.jobResult = JobResult.ERROR;
           retMap.put("state", "FAILURE");
           log.info("call Remote Scripte Err:\n" + se.getErr((List)result.get("list_err")));
           throw new AICloudETLRuntimeException("远程调用脚本出错:\n" + se.getErr((List)result.get("list_err")));
         }
         retMap.put("state", "SUCCESS");
 
         return retMap; } catch (Exception e) {
        	 throw new AICloudETLRuntimeException(e);
       }
       
     }
 
     public JobResult getState()
     {
       return this.jobResult;
     }
 
     public void releaseResource(HisActivity his)
     {
       String flowInsId = his.getDataFlowInsId();
       log.info("进入释放资源.flowInsId:" + flowInsId);
       log.info("flowInsId:" + flowInsId + " 连接数量:" + ((RmtShellExecutor.conns == null) || (ShellBehaver.RmtShellExecutor.conns.get(flowInsId) == null) ? 0 : ((List)ShellBehaver.RmtShellExecutor.conns.get(flowInsId)).size()));
       if ((RmtShellExecutor.conns != null) && (RmtShellExecutor.conns.get(flowInsId) != null)) {
         for (Connection conn : RmtShellExecutor.conns.get(flowInsId)) {
           if (conn != null) {
             conn.close();
             log.info("flowInsId:" + flowInsId + " 释放连接:" + conn.getHostname() + ":" + conn.getPort() + " hashCode:" + conn.hashCode());
           }
         }
         ShellBehaver.RmtShellExecutor.conns.remove(flowInsId);
       }
       log.info("释放资源结束.flowInsId:" + flowInsId);
     }
   }
 
   private static class RmtShellExecutor {
     private static final Log log = LogFactory.getLog(RmtShellExecutor.class);
     private Connection conn;
     private String ip;
     private String usr;
     private String psword;
     private String charset = Charset.defaultCharset().toString();
     private String outStr = null;
     private String outErr = null;
     private boolean result = true;
 
     private Long time_out = null;
     public static Map<String, List<Connection>> conns = Collections.synchronizedMap(new HashMap());
 
     public RmtShellExecutor()
     {
     }
 
     public RmtShellExecutor(String ip, String usr, String ps, Long time_out) {
       this.ip = ip;
       this.usr = usr;
       this.psword = ps;
       this.time_out = time_out;
     }
 
     private boolean login() throws IOException
     {
       this.conn = new Connection(this.ip);
       this.conn.connect();
       Environment env = EnvironmentImpl.getCurrent();
       DataFlowContext context = (DataFlowContext)env.get("DFContext");
       String key = (String)env.get("jobLogId");
 
       if (conns.get(key) == null) {
         conns.put(key, Collections.synchronizedList(new ArrayList()));
       }
       ((List)conns.get(key)).add(this.conn);
       boolean isAuthenticated = false;
       if (this.conn.isAuthMethodAvailable(this.usr, "keyboard-interactive"))
         isAuthenticated = this.conn.authenticateWithKeyboardInteractive(this.usr, new InteractiveCallback()
         {
           public String[] replyToChallenge(String name, String instruction, int numPrompts, String[] prompt, boolean[] echo) throws Exception
           {
             String[] result = new String[numPrompts];
             for (int i = 0; i < numPrompts; i++) {
               result[i] = psword;
             }
             return result;
           }
         });
       else isAuthenticated = this.conn.authenticateWithPassword(this.usr, this.psword);
       return isAuthenticated; } 
     public Map<String, List<ShellBehaver.ExecInfo>> exec(String cmds) throws Exception { 
       Map map = new HashMap();
       List list_err = new ArrayList();
       List list_out = new ArrayList();
       map.put("list_out", list_out);
       map.put("list_err", list_err);
 
       Integer ret = -1;
       try { if (login())
         {
           String[] cmdarrays = cmds.split(";");
 
           for (int i = 0; i < cmdarrays.length; i++)
           {
             ch.ethz.ssh2.Session session = this.conn.openSession();
 
             String defaultShellType = "bash";
             String getShellTypeCmd = "echo $0";
             String bash = "bash";
             String ksh = "ksh";
             String csh = "csh";
             defaultShellType = getShellType(this.conn, getShellTypeCmd);
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
             log.info("shellType==" + defaultShellType);
             ExecInfo exec = new ExecInfo();
             exec.setCreateDate(new Date());
             exec.setCmd(cmdarrays[i]);
             Date start = new Date();
             log.info("cmdarrays[" + i + "]==" + cmdarrays[i]);
 
             session.requestPTY(defaultShellType);
             session.startShell();
             InputStream stdOut = new StreamGobbler(session.getStdout());
             InputStream stdErr = new StreamGobbler(session.getStderr());
             BufferedReader stdoutReader = new BufferedReader(
               new InputStreamReader(stdOut));
             BufferedReader stderrReader = new BufferedReader(
               new InputStreamReader(stdErr));
             PrintWriter outer = new PrintWriter(session.getStdin());
             outer.println(cmdarrays[i]);
             outer.println("exit");
             outer.close();
 
             StringBuilder sb_out = new StringBuilder();
             StringBuilder sb_err = new StringBuilder();
             String line;
             do 
             { 
               line = stdoutReader.readLine();
               String key = "Starting Job = ";
               if ((line != null) && (line.indexOf(key) > -1)) 
               {
                 String jobId = line.substring(line.indexOf(key) + key.length(), line.indexOf(","));
                 DataFlowContext localDataFlowContext1 = (DataFlowContext)EnvironmentImpl.getCurrent().get("DFContext");
               } 
               sb_out.append(line + "\n\r"); 
             }
             while (line != null);
 
             this.outStr = sb_out.toString();
             do { line = stderrReader.readLine();
               sb_err.append(line + "\n\r"); }
             while (line != null);
 
             this.outErr = sb_err.toString();
 
             int returnCode = 0;
             if (this.time_out != null)
               returnCode = session.waitForCondition(34, 
                 this.time_out.longValue());
             else {
               returnCode = session.waitForCondition(34, 
                 0L);
             }
 
             ret = session.getExitStatus();
 
             Date end = new Date();
             long use_time = end.getTime() - start.getTime();
             exec.setUse_time(Long.valueOf(use_time));
 
             if (ret == null) {
               this.result = false;
               exec.setState("FAILURE");
               if (returnCode == 18)
                 this.outStr = ("服务终端[" + this.conn.getHostname() + ":" + this.conn.getPort() + "]连接断开\n" + this.outStr);
               else if (returnCode == 16) {
                 this.outStr = ("服务终端[" + this.conn.getHostname() + ":" + this.conn.getPort() + "]SSH CHANNEL STATE IS EOF\n" + this.outStr);
               }
               exec.setReturnstr(this.outStr);
               list_err.add(exec);
             }
             else if (ret.intValue() == 0) {
               exec.setState("SUCCESS");
             } else {
               this.result = false;
               exec.setState("FAILURE");
             }
 
             if ((this.outStr != null) && (!this.outStr.equals(""))) {
               if (this.outStr.indexOf("HIVE_SQL_EXEC_ERROR") > -1) {
                 this.result = false;
                 list_err.add(exec);
                 exec.setState("FAILURE");
                 exec.setReturnstr(this.outStr);
                 break;
               }
               exec.setReturnstr(this.outStr);
             }
             else if ((this.outErr != null) && (!this.outErr.equals(""))) {
               log.info("outErr==" + this.outErr);
               this.result = false;
               exec.setReturnstr(this.outErr);
               list_err.add(exec);
               break;
             }
 
             log.info("ret==" + ret);
 
             if ((ret != null) && (ret.intValue() == 0)) {
               list_out.add(exec);
             }
             else if ((ret != null) && (ret.intValue() != 0)) {
               list_err.add(exec);
             }
 
             if ((this.time_out != null) && (use_time > 10000L) && 
               (use_time >= this.time_out.longValue())) {
               log.info("cmdarrays[" + i + "] 超时 use_time==" + 
                 use_time);
 
               if (this.conn != null) {
                 Environment env = EnvironmentImpl.getCurrent();
                 DataFlowContext context = (DataFlowContext)env.get("DFContext");
                 String key = (String)env.get("jobLogId");
 
                 if (conns != null) {
                   List list = (List)conns.get(key);
                   if (list != null) {
                     list.remove(this.conn);
                   }
                 }
                 this.conn.close();
               }
 
             }
 
             Thread.sleep(1000L);
             if (stdoutReader != null) {
               stdoutReader.close();
             }
             if (stderrReader != null) {
               stderrReader.close();
             }
             if (stdOut != null) {
               stdOut.close();
             }
             if (stdErr != null) {
               stdErr.close();
             }
             if (session != null)
               session.close();
           }
         }
         else
         {
           this.result = false;
           ShellBehaver.ExecInfo exec = new ShellBehaver.ExecInfo();
           exec.setState("1");
           exec.setReturnstr("login remote machine failure,ip:" + this.ip + 
             " usr:" + this.usr + " psword:" + this.psword);
           list_err.add(exec);
         }
       } catch (Throwable e) {
         this.result = false;
         throw new AICloudETLRuntimeException(e);
       } finally {
         if (this.conn != null) {
           Environment env = EnvironmentImpl.getCurrent();
           DataFlowContext context = (DataFlowContext)env.get("DFContext");
           String key = (String)env.get("jobLogId");
 
           if (conns != null) {
             List list = (List)conns.get(key);
             if (list != null) {
               list.remove(this.conn);
             }
           }
           this.conn.close();
         }
       }
       return map;
     }
 
     private void saveJobId(String activityName, String dataFlowId, String jobId)
     {
       org.hibernate.Session session = null;
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
 
     public String getShellType(Connection conn, String type)
     {
       try
       {
         String returnType = null;
         ch.ethz.ssh2.Session session = conn.openSession();
         session.execCommand(type);
         InputStream stdOut = new StreamGobbler(session.getStdout());
         InputStream stdErr = new StreamGobbler(session.getStderr());
         if (this.time_out != null)
           session.waitForCondition(50, 
             this.time_out.longValue());
         else {
           session.waitForCondition(50, 
             0L);
         }
         Integer ret = session.getExitStatus();
 
         String outStr = processStream(stdOut, this.charset);
 
         String outErr = processStream(stdErr, this.charset);
 
         if ((ret == null) || (ret.intValue() != 0)) {
           returnType = null;
         }
         else if (ret.intValue() == 0)
           returnType = outStr.trim();
         else {
           returnType = null;
         }
 
         if (stdOut != null) {
           stdOut.close();
         }
         if (stdErr != null) {
           stdErr.close();
         }
         if (session != null) {
           session.close();
         }
 
         return returnType;
       }
       catch (Throwable e)
       {
         e.printStackTrace();
       }
 
       return null;
     }
 
     public void killProcess(Connection conn, String cmd)
     {
       String shellName = getShellNameFromCmd(cmd);
       String psCmd = "ps -ef | grep '" + shellName + "'";
       String parentPID = null;
       try
       {
         ch.ethz.ssh2.Session session = conn.openSession();
 
         session.execCommand(psCmd);
 
         String shellPath = getShellPathFromCmd(cmd);
         parentPID = getParentPIDFromOutStr(this.outStr, shellPath);
         log.info("parentPID==" + parentPID);
 
         if (parentPID != null) { int pid = Integer.parseInt(parentPID);
           int childPid = pid;
           Integer result;
           do {
        	  result = null;
             try
             {
               childPid++;
               log.info("childPid==" + childPid);
               ch.ethz.ssh2.Session newSession = conn.openSession();
               result = killProcess(newSession, childPid);
               if (newSession != null) {
                 newSession.close();
               }
             }
             catch (Throwable localThrowable)
             {
             }
           }
           while ((result != null) && (result.intValue() == 0));
 
           ch.ethz.ssh2.Session newSession = conn.openSession();
           result = killProcess(newSession, pid);
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
 
     public Integer killProcess(ch.ethz.ssh2.Session session, int pid)
       throws Exception
     {
       Integer result = null;
       String killChildProc = "kill -9 " + pid;
 
       session.execCommand(killChildProc);
 
       result = session.getExitStatus();
 
       log.info("kill 掉进程" + pid);
 
       return result;
     }
 
     public String getParentPIDFromOutStr(String outStr, String shellPath)
     {
       List list = new ArrayList();
       String parentPID = null;
       String[] array = outStr.split("\n\r");
       for (int i = 0; i < array.length; i++)
       {
         String proc_str = array[i];
         String newStr = proc_str.replaceAll(" ", "#");
         String[] newArray = newStr.split("#");
         for (int k = 0; k < newArray.length; k++)
         {
           if (newArray[k].trim().contains(shellPath.trim())) {
             for (int j = 0; j < newArray.length; j++) {
               if ((newArray[j] != null) && (!"".equals(newArray[j]))) {
                 list.add(newArray[j].trim());
               }
             }
           }
         }
 
       }
 
       if (list.size() > 0) {
         for (int i = 0; i < list.size(); i++) {
           if ((!this.usr.trim().equals(list.get(0))) || 
             (list.get(1) == null))
             continue;
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
 
     public String getShellNameFromCmd(String cmd)
     {
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
 
     private String processStream(InputStream in, String charset)
       throws Exception
     {
       byte[] buf = new byte[1024];
       StringBuilder sb = new StringBuilder();
       while (in.read(buf) != -1) {
         sb.append(new String(buf, charset) + "\n\r");
       }
 
       return sb.toString();
     }
 
     public String getErr(List<ShellBehaver.ExecInfo> result) {
       StringBuilder sb = new StringBuilder("<pre>");
       for (ShellBehaver.ExecInfo info : result) {
         if ("FAILURE".equals(info.getState())) {
           sb.append("执行命令:" + info.getCmd() + "\n");
           sb.append("执行状态:" + info.getState() + "\n");
           sb.append("消耗时间:" + info.getUse_time() + "\n");
           sb.append("创建日期:" + info.getCreateDate().toString() + "\n");
           sb.append("返回结果:\n" + info.getReturnstr().replace("\r", "").replace("\n\n", "\n") + "\n");
         }
       }
       sb.append("</pre>");
       return sb.toString();
     }
 
     public String getOut(List<ShellBehaver.ExecInfo> result) {
       StringBuilder sb = new StringBuilder("<pre>");
       for (ShellBehaver.ExecInfo info : result) {
         sb.append("执行命令:" + info.getCmd() + "\n");
         sb.append("执行状态:" + info.getState() + "\n");
         sb.append("消耗时间:" + info.getUse_time() + "\n");
         sb.append("创建日期:" + info.getCreateDate().toString() + "\n");
         sb.append("返回结果:\n" + info.getReturnstr().replace("\r", "").replace("\n\n", "\n") + "\n");
       }
       sb.append("</pre>");
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
 
     public String getOutStr()
     {
       return this.outStr;
     }
 
     public void setOutStr(String outStr) {
       this.outStr = outStr;
     }
 
     public String getOutErr() {
       return this.outErr;
     }
 
     public void setOutErr(String outErr) {
       this.outErr = outErr;
     }
 
     public boolean isResult() {
       return this.result;
     }
 
     public void setResult(boolean result) {
       this.result = result;
     }
   }
 }
