package com.asiainfo.local.cmc.sd;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
import com.ailk.cloudetl.commons.internal.tools.ReadableMsgUtils;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.context.DataFlowContext;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
 
 public class ExeExternalNode   implements UDFActivity
 {
  private static final Log log = LogFactory.getLog(ExeExternalNode.class);
  private static final String COMMAND_NAME = "command";
  private static final String WORK_DIR = "workDir";
  private JobResult result = JobResult.RIGHT;

  public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
  {
       String msg = null;
       ShellCommandExecutor shexec = null;
       String workDir="";
       try 
       {
         String shell = (String)udfParams.get("command");
         workDir = (String)udfParams.get("workDir");
          
         boolean isWindows = System.getProperty("os.name").toLowerCase().indexOf("win") > -1;          
         String[] commandArg = null;          
         if (isWindows) 
         {
            commandArg = StringUtils.split(shell);
         } 
         else 
         {
            commandArg = new String[3];
            commandArg[0] = "sh";
            commandArg[1] = "-c";
            commandArg[2] = shell;
         }
         if (StringUtils.isEmpty(workDir))
           shexec = new ShellCommandExecutor(commandArg);
         else
           shexec = new ShellCommandExecutor(commandArg, new File(workDir));
         shexec.execute();
         int exitcode = shexec.getExitCode();
         if (exitcode != 0) 
         {
            msg = "执行命令失败， 返回码：" + exitcode;
            if ((shexec != null) && (!StringUtils.isEmpty(shexec.getOutput()))) 
            {
              msg = msg + "\n" + shexec.getOutput();
            }
            log.error(msg);
            this.result = JobResult.ERROR;
         }
         else if ((shexec != null) && (!StringUtils.isEmpty(shexec.getOutput()))) 
         {
            msg = shexec.getOutput();
         }
      }
      catch (Throwable e) 
      {
          log.error("Exception raised from Shell command," + e.getMessage(), e);
          log.error("Command failed with exit output = " + (shexec != null ? shexec.getOutput() : ""));
          msg = "执行命令失败：" + e.getMessage();
          this.result = JobResult.ERROR;
      }
 
     if (StringUtils.isEmpty(msg)) 
     {
       if (this.result == JobResult.RIGHT)
         msg = "命令执行成功";
       else 
       {
         msg = "命令执行失败";
       }
     }
     Map paramMap = new HashMap();
     if (this.result == JobResult.ERROR)
         paramMap.put(ReadableMsgUtils.readableMsg, msg);
     else 
     {
         paramMap.put("ExecuteInfo", msg);
     }
     if (StringUtils.isNotEmpty(msg)) 
     {
        msg = msg.replace("\n", "");
        msg = msg.replace("\r", "");
        if (msg.matches("[0-9]+")) 
        {
                workDir = (String) EnvironmentImpl.getCurrent().get("DFContext");
                //DataFlowContext   workDir = (DataFlowContext)EnvironmentImpl.getCurrent().get("DFContext");
        }
      } 
     return paramMap;
   }
 
   public JobResult getState()
   {
     return this.result;
   }
 
   public void releaseResource(HisActivity arg0)
   {
   }
 }