package com.asiainfo.local.cmc.sd;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hsqldb.lib.StringUtil;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
import com.ailk.udf.node.shandong.trans.FileTransUtil;

public class Date2MonthTransUDF
  implements UDFActivity
{
  private static final Log LOGGER = LogFactory.getLog(Date2MonthTransUDF.class);

  private JobResult udfResult = JobResult.RIGHT;
  private final String ERROR_MESSAGE = "ERROR_MESSAGE";

  private boolean singleLog = true;
  private Logger localLogger = null;

  public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
  {
    initLocalLog(env, udfParams);
    writeLocalLog("开始执行日转月...");
    Map executeResult = null;
    Session session = null;
    Connection conn = null;
    PreparedStatement queryStmt = null;
    ResultSet queryRs = null;
    String time = (String)udfParams.get("DATA_TIME_");
    String targetTime = null;
    try {
      session = HibernateUtils.getSessionFactory().openSession();
      conn = session.connection();
      conn.setAutoCommit(true);
      if (StringUtils.isBlank(time)) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(5, 1);
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd");
        targetTime = dateformat.format(calendar.getTime());
      } else {
        targetTime = time;
      }

      String selectSql = "select cfg.IF_NO_, et.FILE_NAME_, et.FILE_PATH_, et.DATA_TIME_, et.FILE_SIZE_, et.FILE_RECORD_NUM_ from (select IF_NO_, FILE_NAME_, FILE_PATH_, DATA_TIME_, FILE_SIZE_, FILE_RECORD_NUM_ from STATUS_ET_FILE where STATUS_=1 and DATA_TIME_=" + 
        targetTime + ") et inner join (select IF_NO_ from CFG_TABLE_INTERFACE where NEED_COPY_TO_MONTH_=1 " + 
        "and DAY_OR_MONTH_=1) cfg on et.IF_NO_=cfg.IF_NO_ and et.file_path_ is not null  where NOT EXISTS(select * from STATUS_ET_FILE tt " + 
        "where tt.FILE_NAME_=CONCAT('M',SUBSTRING(et.FILE_NAME_, 2)) and tt.file_path_ is not null);";
      queryStmt = conn.prepareStatement(selectSql);
      LOGGER.info("Query interface file sql ==> " + selectSql);
      writeLocalLog("Query interface file sql ==> " + selectSql);
      queryRs = queryStmt.executeQuery();
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      while (queryRs.next()) {
        String ifNumber = queryRs.getString(1);
        String fileName = queryRs.getString(2);
        String filePath = queryRs.getString(3);
        int dataTime = queryRs.getInt(4);
        String fileSize = queryRs.getString(5);
        String recordNum = queryRs.getString(6);
        if ((!fileName.trim().startsWith("M")) && (!ifNumber.trim().startsWith("M"))) {
          fileName = fileName.trim();
          filePath = filePath.trim();
          String destName = changeHeaderChar(fileName, Character.valueOf('M'));
          String copyResult = null;
          if (fileName.endsWith(".AVL")) {
            copyResult = FileTransUtil.hdfsFileCopy(conf, fs, FileTransUtil.getAbsolutePath(filePath, fileName), 
              FileTransUtil.getAbsolutePath(filePath, destName));
          } else if (fileName.endsWith(".CHK")) {
            copyResult = chkFileCopy(conf, fs, FileTransUtil.getAbsolutePath(filePath, fileName), 
              FileTransUtil.getAbsolutePath(filePath, destName));
          } else {
            String errMsg = "No need to copy day to month data file. fileName ==> " + FileTransUtil.getAbsolutePath(filePath, fileName);
            LOGGER.info(errMsg);
            writeLocalLog(errMsg);
            continue;
          }
          if (copyResult == null)
            if (!updateStatus(changeHeaderChar(ifNumber, Character.valueOf('M')), destName, filePath, dataTime, fileSize, recordNum))
              executeResult = checkResult("Update database error. dataTime:" + dataTime + ", filePath:" + filePath + ", fileName" + destName, executeResult);
            else
              executeResult = checkResult(copyResult, executeResult);
        }
      }
    } catch (SQLException e) {
      this.udfResult = JobResult.ERROR;
      String errMsg = "Date2MonthTransUDF SQLException";
      LOGGER.error(errMsg, e);
      writeLocalLog(e, errMsg);
      Map localMap1 = executeResult;
      //String errMsg;
      return localMap1;
    } catch (IOException e) {
      this.udfResult = JobResult.ERROR;
      String errMsg = "Date2MonthTransUDF IOException";
      LOGGER.error(errMsg, e);
      writeLocalLog(e, errMsg);
      Map localMap1 = executeResult;
      //String errMsg;
      return localMap1;
    } finally {
      try {
        if (queryRs != null)
          queryRs.close();
        if (queryStmt != null)
          queryStmt.close();
        if (session != null)
          session.close();
      } catch (SQLException e) {
        String errMsg = "Close db connection[execute] error.";
        LOGGER.error(errMsg, e);
        writeLocalLog(e, errMsg);
      }
    }
    try
    {
      if (queryRs != null)
        queryRs.close();
      if (queryStmt != null)
        queryStmt.close();
      if (session != null)
        session.close();
    } catch (SQLException e) {
      String errMsg = "Close db connection[execute] error.";
      LOGGER.error(errMsg, e);
      writeLocalLog(e, errMsg);
    }

    writeLocalLog("日转月完毕。");
    return executeResult;
  }

  private void initLocalLog(Environment env, Map<String, String> udfParams) {
    this.singleLog = TaskNodeLogger.isSingleLog();
    Object logObj = env.get("taskNodeLogger");
    if (logObj == null) {
      LOGGER.warn("the localLogger is not in Environment");
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

  public String changeHeaderChar(String name, Character newChar) {
    if (StringUtil.isEmpty(name))
      return name;
    StringBuffer buffer = new StringBuffer(name.trim());

    buffer.setCharAt(0, newChar.charValue());
    return buffer.toString();
  }

  public String chkFileCopy(Configuration conf, FileSystem fs, String srcPath, String destPath) {
    String copyFile = null;
    FSDataOutputStream os = null;
    FSDataInputStream is = null;
    try {
      LOGGER.info("Copy CHK file from [" + srcPath + "] to [" + destPath + "]");
      writeLocalLog("Copy CHK file from [" + srcPath + "] to [" + destPath + "]");
      os = FileSystem.get(conf).create(new Path(destPath), true);
      is = FileSystem.get(conf).open(new Path(srcPath));
      String temp = is.readLine();
      while (temp != null) {
        String changeContent = changeHeaderChar(temp, Character.valueOf('M'));
        os.writeBytes(changeContent + "\n");
        temp = is.readLine();
      }
      os.hflush();
      String str1 = copyFile;
      String errMsg;
      return str1;
    } catch (Exception e) {
      String errMsg = "Copy file from [" + srcPath + "] to [" + destPath + "] has exception.";
      LOGGER.error(errMsg, e);
      writeLocalLog(e, errMsg);
    }
    finally
    {
      try
      {
        String errMsg;
        if (os != null)
          os.close();
        if (is != null)
          is.close();
      } catch (IOException e) {
        String errMsg = "Close HDFS file stream[hdfsFileCopy] error.";
        LOGGER.error(errMsg, e);
        writeLocalLog(e, errMsg);
      }
    }
    return copyFile; } 
  public boolean updateStatus(String iterface, String fileName, String filePath, int dataTime, String fileSize, String recordNum) { boolean update = true;
    Session session = null;
    Connection conn = null;
    Statement updateStmt = null;
    String errMsg;
    try { session = HibernateUtils.getSessionFactory().openSession();
      conn = session.connection();
      String insertSql = "insert into STATUS_ET_FILE(IF_NO_, DATA_TIME_, FILE_NAME_, FILE_PATH_, FILE_SIZE_, FILE_RECORD_NUM_, STATUS_, EXTRACT_START_TIME_, EXTRACT_END_TIME_ ) values ('" + 
        iterface + "', " + dataTime + ", '" + fileName + "', '" + filePath + "', '" + fileSize + 
        "', '" + recordNum + "', 1, now(), now())";
      LOGGER.info("Update interface status sql ==> " + insertSql);
      writeLocalLog("Update interface status sql ==> " + insertSql);
      conn.setAutoCommit(true);
      updateStmt = conn.createStatement();
      int insert = updateStmt.executeUpdate(insertSql);
      if (insert == 0)
        update = false;
      writeLocalLog("更新STATUS_ET_FILE 成功. ");
    } catch (Exception e) {
      if (conn != null)
        try {
          conn.rollback();
        } catch (SQLException e1) {
          errMsg = "Database connection rollback Exception";
          LOGGER.error(errMsg, e1);
          writeLocalLog(e1, errMsg);
        }
      this.udfResult = JobResult.ERROR;
      errMsg = "Date2MonthTransUDF update status Exception";
      LOGGER.error(errMsg, e);
      writeLocalLog(e, errMsg);
      //String errMsg;
      return update;
    } finally {
      try {
        if (updateStmt != null)
          updateStmt.close();
        if (session != null)
          session.close();
      } catch (SQLException e) {
        errMsg = "Close db connection[updateStatus] error.";
        LOGGER.error(errMsg, e);
        writeLocalLog(e, errMsg);
      }
    }
    return update; }

  public Map<String, Object> checkResult(String copyResult, Map<String, Object> resultMap)
  {
    if (copyResult != null) {
      this.udfResult = JobResult.ERROR;
      if (resultMap == null) {
        resultMap = new HashMap();
        resultMap.put("ERROR_MESSAGE", copyResult);
      } else {
        StringBuffer buffer = new StringBuffer((String)resultMap.get("ERROR_MESSAGE"));
        resultMap.put("ERROR_MESSAGE", buffer.append(", \n" + copyResult));
      }
    }
    return resultMap;
  }

  public void releaseResource(HisActivity hisAct)
  {
  }

  public JobResult getState()
  {
    return this.udfResult;
  }

  public static void main(String[] args) {
    Date2MonthTransUDF udf = new Date2MonthTransUDF();
    System.out.println(udf.changeHeaderChar("I0909001", Character.valueOf('M')));
    Calendar calendar = Calendar.getInstance();
    calendar.add(2, 1);
    calendar.set(5, 1);
    SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd");
    System.out.println(Integer.parseInt(dateformat.format(calendar.getTime())));
    udf.execute(null, null);
  }
}
