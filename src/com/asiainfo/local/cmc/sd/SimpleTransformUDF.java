package com.asiainfo.local.cmc.sd;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
import com.ailk.udf.node.shandong.DataTimeUtil;
import com.ailk.udf.node.shandong.trans.FileTransUtil;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

public class SimpleTransformUDF
  implements UDFActivity
{
  private static final Log LOGGER = LogFactory.getLog(SimpleTransformUDF.class);

  private final String DAY_OR_MONTH = "DAY_OR_MONTH";
  private final String TRANS_LOAD_DIR_OUT = "/asiainfo/ETL/trans/out/$TABLENAME/";
  private final String ERROR_MESSAGE = "ERROR_MESSAGE";

  private JobResult result = JobResult.RIGHT;

  private boolean singleLog = true;
  private Logger localLogger = null;

  public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
  {
    initLocalLog(env, udfParams);
    writeLocalLog("开始执行节点" + getClass().getName());
    Map executeResult = new HashMap();
    String dayOrMonth = (String)udfParams.get("DAY_OR_MONTH");
    int dayMonth = 0;
    if ((dayOrMonth != null) && (dayOrMonth.trim().length() > 0) && (dayOrMonth.trim().toLowerCase().startsWith("d"))) {
      dayMonth = 1;
    } else if ((dayOrMonth != null) && (dayOrMonth.trim().length() > 0) && (dayOrMonth.trim().toLowerCase().startsWith("m"))) {
      dayMonth = 2;
    } else {
      this.result = JobResult.ERROR;
      String errMsg = "Parameter [DAY_OR_MONTH]'s value must be one of [day, month], please set the right value.";
      LOGGER.error(errMsg);
      writeLocalLog(errMsg);
      executeResult.put("ERROR", errMsg);
      return null;
    }
    Session session = null;
    Connection conn = null;
    PreparedStatement queryStmt = null;
    ResultSet queryRs = null;
    try {
      session = HibernateUtils.getSessionFactory().openSession();
      conn = session.connection();
      conn.setAutoCommit(true);
      String selectSql = "select cfg.IF_NO_, cfg.TABLE_NAME_, cfg.IS_PARTITION_, ett.FILE_NAME_, ett.FILE_PATH_, ett.DATA_TIME_ from (select et.IF_NO_, et.FILE_NAME_, et.FILE_PATH_, et.DATA_TIME_,si.CHK_STATUS_ from STATUS_ET_FILE et left join STATUS_INTERFACE si on et.IF_NO_=si.IF_NO_ and et.DATA_TIME_=si.DATA_TIME_ where et.STATUS_=1 and et.FILE_NAME_ not like '%CHK' and et.FILE_NAME_ not like '%VERF' and (et.TRANS_STATUS_=0 or et.TRANS_STATUS_ is null)) ett inner join (select IF_NO_, TABLE_NAME_, IS_PARTITION_,LOAD_ACTION_,IF_NAME_ from CFG_TABLE_INTERFACE where LOAD_STYLE_=1 and DAY_OR_MONTH_=" + 
        dayMonth + ") cfg on ett.IF_NO_=cfg.IF_NO_ " + 
        "where (cfg.LOAD_ACTION_=0 and ett.CHK_STATUS_=1) or cfg.LOAD_ACTION_=1 or (cfg.LOAD_ACTION_=0 and cfg.IF_NAME_='DIM')";
      queryStmt = conn.prepareStatement(selectSql);
      LOGGER.info("Query config interface sql ==> " + selectSql);
      writeLocalLog("Query config interface sql ==> " + selectSql);
      queryRs = queryStmt.executeQuery();
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      String dfInstanceId = (String)env.get("jobLogId");
      while (queryRs.next()) {
        String ifNumber = queryRs.getString(1).trim();
        String tableName = queryRs.getString(2).trim();
        int isPartition = queryRs.getInt(3);
        String fileName = queryRs.getString(4).trim();
        String filePath = queryRs.getString(5).trim();
        int dataTime = queryRs.getInt(6);
        conn.createStatement().executeUpdate("update STATUS_ET_FILE set TRANS_START_TIME_=now() where IF_NO_='" + ifNumber + "' " + 
          "and FILE_NAME_='" + fileName + "' and DATA_TIME_=" + dataTime);
        String loadOutPath = "/asiainfo/ETL/trans/out/$TABLENAME/".replace("$TABLENAME", tableName);
        String dateString = "";
        DataTimeUtil dtu = new DataTimeUtil();
        if (isPartition == 1)
        {
          dateString = "PT_TIME_=" + dtu.dealDate(env, "", tableName, fileName.substring(6, 14), "yyyy-MM-dd") + "/";
        } else if (isPartition != 0)
        {
          this.result = JobResult.ERROR;
          LOGGER.error("Unsupported isPartition value ::" + isPartition);
          writeLocalLog(null, "Unsupported isPartition value ::" + isPartition);
          continue;
        }
        loadOutPath = loadOutPath + dateString;
        String srcPath = FileTransUtil.getAbsolutePath(filePath, fileName);
        String resLoadOut = copyFile(conf, fs, srcPath, loadOutPath + fileName);
        executeResult = checkResult(resLoadOut, executeResult);
        if (resLoadOut == null) {
          boolean update = updateStatus(dfInstanceId, ifNumber, tableName, fileName, loadOutPath, dataTime);
          if (!update) {
            String errorMsg = "Update file status ERROR ==> IF_NO_:" + ifNumber + ", FILE_NAME_:" + fileName + ", DATA_TIME_:" + dataTime + 
              ", DataFlowInstanceId:" + dfInstanceId;
            LOGGER.info(errorMsg);
            writeLocalLog(null, errorMsg);
            executeResult = checkResult(errorMsg, executeResult);
          }
        }
      }
    } catch (SQLException e) {
      this.result = JobResult.ERROR;
      String errMsg = "SimpleTransformUDF SQLException";
      LOGGER.error(errMsg, e);
      writeLocalLog(e, errMsg);
    //  String errMsg;
      return null;
    } catch (IOException e) {
      this.result = JobResult.ERROR;
      String errMsg = "SimpleTransformUDF IOException";
      LOGGER.error(errMsg, e);
      writeLocalLog(e, errMsg);
   //  String errMsg;
     return null;
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
 /*  try
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
*/
   writeLocalLog("节点执行完毕.");
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

 public Map<String, Object> checkResult(String copyResult, Map<String, Object> resultMap) {
   if (copyResult != null) {
     this.result = JobResult.ERROR;
     if (resultMap == null) {
       resultMap = new HashMap();
       resultMap.put("ERROR_MESSAGE", copyResult);
     } else {
       StringBuffer buffer = new StringBuffer(resultMap.get("ERROR_MESSAGE")+"");
       resultMap.put("ERROR_MESSAGE", buffer.append(", \n" + copyResult));
     }
   }
   return resultMap; } 
 public boolean updateStatus(String insId, String iterfaceNO, String tableName, String fileName, String filePath, int dataTime) { boolean update = false;
   Session session = null;
   Connection conn = null;
   PreparedStatement queryStmt = null;
   Statement stmt = null;
   ResultSet queryRs = null;
   String errMsg;
   try { session = HibernateUtils.getSessionFactory().openSession();
     conn = session.connection();
     conn.setAutoCommit(false);
     String querySql = "select FILE_SIZE_, FILE_RECORD_NUM_ from STATUS_ET_FILE where IF_NO_='" + 
       iterfaceNO + "' and FILE_NAME_='" + fileName + "' and DATA_TIME_=" + dataTime;
     queryStmt = conn.prepareStatement(querySql);
     queryRs = queryStmt.executeQuery();

     String udpateSql = "update STATUS_ET_FILE set TRANS_STATUS_=1, FLOW_INST_ID_='" + insId + "', TRANS_END_TIME_=now() " + 
       "where IF_NO_='" + iterfaceNO + "' and FILE_NAME_='" + fileName + "' and DATA_TIME_=" + dataTime;
     LOGGER.info("Update  ET file status sql ==> " + udpateSql);
     writeLocalLog("Update  ET file status sql ==> " + udpateSql);
     stmt = conn.createStatement();
     stmt.execute(udpateSql);
     while (queryRs.next()) {
       String fileSize = queryRs.getString(1);
       String recordNum = queryRs.getString(2);
       String insertSql = "insert into STATUS_LD_FILE(DATA_TIME_, FLOW_INST_ID_, TABLE_NAME_, FILE_NAME_, FILE_PATH_, FILE_SIZE_, FILE_RECORD_NUM_) values (" + 
         dataTime + ", '" + insId + "', '" + tableName + "', '" + fileName + "', '" + filePath + "', '" + fileSize + "', '" + 
         recordNum + "')";
       stmt.execute(insertSql);
       LOGGER.info("Insert  LD file status sql ==> " + insertSql);
       writeLocalLog("Insert  LD file status sql ==> " + insertSql);
     }
     conn.commit();
     update = true;
   } catch (Exception e) {
     if (conn != null)
       try {
         conn.rollback();
       } catch (SQLException e1) {
         String errMsge = "Database connection rollback Exception";
         LOGGER.error(errMsge, e1);
         writeLocalLog(e, errMsge);
       }
     this.result = JobResult.ERROR;
     String errMsge = "SimpleTransformUDF update status Exception";
     LOGGER.error(errMsge, e);
     writeLocalLog(e, errMsge);
    // String errMsg;
     return update;
   } finally {
     try {
       if (queryRs != null)
         queryRs.close();
       if (queryStmt != null)
         queryStmt.close();
       if (stmt != null)
         stmt.close();
       if (session != null)
         session.close();
     } catch (SQLException e) {
       String errMsge = "Close db connection[updateStatus] error.";
       writeLocalLog(e, errMsge);
       LOGGER.error(errMsge, e);
     }
   }
   return update; }

 public String copyFile(Configuration conf, FileSystem fs, String srcPath, String destPath)
 {
   String copyFile = null;
   try {
     LOGGER.info("Copy file from [" + srcPath + "] to [" + destPath + "]");
     writeLocalLog("Copy file from [" + srcPath + "] to [" + destPath + "]");
     boolean copyResult = FileUtil.copy(fs, new Path(srcPath), fs, new Path(destPath), false, true, conf);
     if (!copyResult) {
       this.result = JobResult.ERROR;
       copyFile = "Copy file from [" + srcPath + "] to [" + destPath + "] error.";
       LOGGER.error(copyFile);
       writeLocalLog(copyFile);
     }
     return copyFile;
   } catch (Exception e) {
     this.result = JobResult.ERROR;
     String errMsg = "Copy file from [" + srcPath + "] to [" + destPath + "] has exception.";
     writeLocalLog(e, errMsg);
     LOGGER.error(errMsg, e);
   }
   return copyFile;
 }

 public void releaseResource(HisActivity hisAct)
 {
 }

 public JobResult getState()
 {
   return this.result;
 }

 public static void main(String[] args) {
   SimpleTransformUDF udf = new SimpleTransformUDF();
   Map udfParams = new HashMap();
   udf.getClass(); udfParams.put("DAY_OR_MONTH", "day");
   udf.execute(null, udfParams);
 }
}
