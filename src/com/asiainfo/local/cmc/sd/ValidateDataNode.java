package com.asiainfo.local.cmc.sd;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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
import org.apache.log4j.Logger;
import org.hibernate.classic.Session;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
import com.ailk.cloudetl.dbservice.platform.api.DBPool;
import com.ailk.cloudetl.exception.AICloudETLRuntimeException;
import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;

public class ValidateDataNode
  implements UDFActivity
{
  private static final Log log = LogFactory.getLog(ValidateDataNode.class);

  public Map<String, Object> retMap = new HashMap<String, Object>();

  private JobResult result = JobResult.RIGHT;
  private static final String DB_CONFIGURE_NAME = "DB_CONFIGURE_NAME";
  private static final String CHK_FILE_PATH = "CHK_FILE_PATH";
  private static final String VERF_FILE_PATH = "VERF_FILE_PATH";
  private static final String INTERFACE_NAME = "INTERFACE_NAME";
  private static final String V_DATE = "V_DATE";
  private static final String FILE_NAME = "FILE_NAME";
  private static final String CHK_VALIDATE_SUCCESS_LIST = "CHK_VALIDATE_SUCCESS_LIST";
  private static final String CHK_VALIDATE_FAILURE_LIST = "CHK_VALIDATE_FAILURE_LIST";
  private static final String VERF_VALIDATE_SUCCESS_LIST = "VERF_VALIDATE_SUCCESS_LIST";
  private static final String VERF_VALIDATE_FAILURE_LIST = "VERF_VALIDATE_FAILURE_LIST";
  private Connection con = null;
  private Session session = null;

  private Boolean isDBPool = Boolean.valueOf(false);

  private boolean singleLog = true;
  private Logger localLogger = null;

  public static void main(String[] args) throws Exception {
    while (true) {
      ValidateDataNode n = new ValidateDataNode();
      n.getConnection(null);
      n.validateChkFile(null, "D:\\TEST", null);
      n.validateVerfFile(null, "D:\\TEST");
      n.updateTransStatus(null, null);
      n.updateRecordValidateStatus(null);
      System.out.println(n.retMap);
      n.closeConnection();
      Thread.sleep(5000L);
    }
  }

  private void getConnection(String dbConfigureName) throws Exception {
    if (con == null)
      if ((dbConfigureName != null) && (!"".equals(dbConfigureName.toString()))) {
        con = DBPool.INSTANCE.getDBConnection(dbConfigureName.toString());
        con.setAutoCommit(false);
        isDBPool = Boolean.valueOf(true);
      } else {
        session = HibernateUtils.getSessionFactory().openSession();
        con = session.connection();
        con.setAutoCommit(false);
      }
  }

  private void closeConnection() throws Exception
  {
    if (this.isDBPool.booleanValue()) {
      if ((this.con != null) && (!this.con.isClosed())) {
        this.con.close();
      }
    }
    else if (this.session != null)
      this.session.close();
  }

  public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
  {
    try
    {
      initLocalLog(env, udfParams);

      String dataTime = (String)udfParams.get("DATETIME");
      String chkFilePath = (String)udfParams.get("CHK_FILE_PATH");
      String verfFilePath = (String)udfParams.get("VERF_FILE_PATH");
      String dbConfigureName = (String)udfParams.get("DB_CONFIGURE_NAME");
      writeLocalLog("获取数据库连接");
      getConnection(dbConfigureName);

      validateChkFile(dbConfigureName, chkFilePath, dataTime);

      validateVerfFile(dbConfigureName, verfFilePath);

      writeLocalLog("更新status_interface表的trans_status_转换状态字段");
      updateTransStatus(dbConfigureName, dataTime);

      writeLocalLog("校验FTP抽取数据行数和转换后数据行数是否一致");
      updateRecordValidateStatus(dbConfigureName);

      String statusColumn = "HIVE_LOAD_STATUS_";
      String endTimeColumn = "HIVE_LOAD_END_TIME_";
      String loadTimeColumn = "HIVE_LOAD_TIME_";
      updateStatusTable(env, dbConfigureName, statusColumn, endTimeColumn, loadTimeColumn, dataTime);

      statusColumn = "GBASE_LOAD_STATUS_";
      endTimeColumn = "GBASE_LOAD_END_TIME_";
      loadTimeColumn = "GBASE_LOAD_TIME_";
      updateStatusTable(env, dbConfigureName, statusColumn, endTimeColumn, loadTimeColumn, dataTime);

      statusColumn = "GP_LOAD_STATUS_";
      endTimeColumn = "GP_LOAD_END_TIME_";
      loadTimeColumn = "GP_LOAD_TIME_";
      updateStatusTable(env, dbConfigureName, statusColumn, endTimeColumn, loadTimeColumn, dataTime);

      statusColumn = "DB2_LOAD_STATUS_";
      endTimeColumn = "DB2_LOAD_END_TIME_";
      loadTimeColumn = "DB2_LOAD_TIME_";
      updateStatusTable(env, dbConfigureName, statusColumn, endTimeColumn, loadTimeColumn, dataTime);

      statusColumn = "ORACLE_LOAD_STATUS_";
      endTimeColumn = "ORACLE_LOAD_END_TIME_";
      loadTimeColumn = "ORACLE_LOAD_TIME_";
      updateStatusTable(env, dbConfigureName, statusColumn, endTimeColumn, loadTimeColumn, dataTime);
    }
    catch (Throwable e) {
      String errMsg = "文件校验异常: ";
      writeLocalLog(e, errMsg);
      throw new AICloudETLRuntimeException(errMsg, e);
    } finally {
      try {
        closeConnection();
      } catch (Exception e) {
        writeLocalLog(e, null);
      }
    }
    return this.retMap;
  }

  private void initLocalLog(Environment env, Map<String, String> udfParams) {
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

  private void writeLocalLog(Throwable e, String errMsg) {
    if (this.singleLog) {
      if (StringUtils.isBlank(errMsg)) {
        errMsg = "execute " + getClass().getName() + " activity error." + AICloudETLExceptionUtil.getErrMsg(e);
      }
      this.localLogger.error(errMsg, e);
    }
  }

  private boolean checkExistsCHKFile(String chkFilePath) throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    Path path = new Path(chkFilePath);
    FileStatus[] files = fs.listStatus(path);
    boolean exists = false;
    for (FileStatus file : files) {
      String fileName = file.getPath().getName();
      if (fileName.endsWith(".CHK")) {
        exists = true;
        break;
      }
    }
    return exists;
  }

  private void validateVerfFile(String dbConfigureName, String verfFilePath)
    throws Exception
  {
    FileSystem fs = FileSystem.get(new Configuration());
    Path path = new Path(verfFilePath);
    FileStatus[] files = fs.listStatus(path);

    for (FileStatus file : files) {
      String fileName = file.getPath().getName();
      if (fileName.endsWith(".VERF")) {
        writeLocalLog("校验VERF文件," + file.getPath().toUri().getPath());

        List<String> interfaceInVerfList = parseList(readCheckFile(file.getPath().toString(), null));
        Integer dataTime = Integer.valueOf(fileName.substring(2, 10));

        List<Map<String,String>> interfaceInDBListMap = getIngerfaceFromInterfaceTable(dbConfigureName, dataTime);
        List<String> interfaceInDBList = this.parseListMapToList(interfaceInDBListMap);
        Map<Integer,List<String>> checkResult = new HashMap<Integer,List<String>>();
        List<String> successList = new ArrayList<String>();
        List<String> failureList = new ArrayList<String>();
        for (String ifNo : interfaceInVerfList) {
          if (interfaceInDBList.contains(ifNo))
            successList.add(ifNo);
          else {
            failureList.add(ifNo);
          }
        }
        checkResult.put(Integer.valueOf(1), successList);
        checkResult.put(Integer.valueOf(0), failureList);

        updateInterfaceValidateStatusToDB(dbConfigureName, checkResult, dataTime);
        this.retMap.put("VERF_VALIDATE_SUCCESS_LIST", successList);
        this.retMap.put("VERF_VALIDATE_FAILURE_LIST", failureList);
      }
    }
  }

  private void validateChkFile(String dbConfigureName, String chkFilePath, String dataTime)
    throws Exception
  {
    if (!checkExistsCHKFile(chkFilePath)) {
      writeLocalLog("目录" + chkFilePath + "下没有chkFile.");
      return;
    }

    List<Map<String,String>> list = getUnCheckFileFromETTable(dbConfigureName, dataTime);
    writeLocalLog("查询到未校验的文件列表：" + list.size());
    Map<String,List<String>> parseResultMap = parseResult(list);
    writeLocalLog("根据接口号和数据日期进行分组");

    FileSystem fs = FileSystem.get(new Configuration());

    for (Map.Entry<String,List<String>> entry : parseResultMap.entrySet()) {
      String key = entry.getKey();
      List<String> fileNames = entry.getValue();
      String chkFileName = key + ".CHK";
      String chkFile = chkFilePath + File.separator + chkFileName;
      Path path = new Path(chkFile);

      if (!fs.exists(path)) {
        chkFile = chkFilePath + File.separator + key + "000001.CHK";
        path = new Path(chkFile);
      }
      if ((fs.exists(path)) && (!isChecked(dbConfigureName, chkFileName)))
        validateOneFile(dbConfigureName, chkFile, chkFileName, fileNames);
    }
  }

  private void validateOneFile(String dbConfigureName, String chkFile, String chkFileName, List<String> fileNames)
    throws Exception
  {
    writeLocalLog("校验文件：" + chkFile);
    Map<Integer,List<String>> checkResult = new HashMap<Integer,List<String>>();
    Map<String,Integer> interfaceCheckStatus = new HashMap<String,Integer>();
    List<String> successList = new ArrayList<String>();
    List<String> failureList = new ArrayList<String>();
    List<List<String>> fileInChkList = readCheckFile(chkFile, "\t");

    List<String> fileNameList = parseList(fileInChkList);

    if ((fileNames != null) && (fileNameList != null) && (fileNameList.size() > 0) && (fileNames.size() > 0)) {
      interfaceCheckStatus.put(chkFileName, Integer.valueOf(1));

      for (String fileName : fileNameList) {
        if (fileNames.contains(fileName)) {
          successList.add(fileName);
        } else {
          interfaceCheckStatus.put(chkFileName, Integer.valueOf(0));
          failureList.add(fileName);
        }
      }
    }
    checkResult.put(Integer.valueOf(1), successList);
    checkResult.put(Integer.valueOf(0), failureList);

    updateValidateStatusToDB(dbConfigureName, checkResult, interfaceCheckStatus);
    if (this.retMap.get(CHK_VALIDATE_SUCCESS_LIST) == null)
      this.retMap.put(CHK_VALIDATE_SUCCESS_LIST, successList);
    else {
      ((List<String>)retMap.get(CHK_VALIDATE_SUCCESS_LIST)).addAll(successList);
    }
    if (this.retMap.get(CHK_VALIDATE_FAILURE_LIST) == null)
      this.retMap.put(CHK_VALIDATE_FAILURE_LIST, failureList);
    else
      ((List)this.retMap.get(CHK_VALIDATE_FAILURE_LIST)).addAll(failureList);
  }

  private void updateInterfaceValidateStatusToDB(String dbConfigureName, Map<Integer, List<String>> checkResult, Integer dataTime) throws Exception
  {
    StringBuffer updateStatusInterfaceSql = new StringBuffer("update STATUS_INTERFACE set VERF_STATUS_=?,DESC_=? where IF_NO_=? and DATA_TIME_=?");
    StringBuffer insertStatusInterfaceSql = new StringBuffer("insert into STATUS_INTERFACE(IF_NO_,DATA_TIME_,CHK_FILE_NAME_,VERF_STATUS_,DESC_) values (?,?,?,?,?)");
    PreparedStatement ps = null;
    PreparedStatement ups = null;
    try {
      ups = this.con.prepareStatement(updateStatusInterfaceSql.toString());
      ps = this.con.prepareStatement(insertStatusInterfaceSql.toString());

      for (String interfaceName : checkResult.get(0)) {
        ups.setInt(1, 0);
        ups.setString(2, interfaceName + "接口还未抽取到数据或抽取校验不成功");
        ups.setString(3, interfaceName);
        ups.setInt(4, dataTime.intValue());
        int num = ups.executeUpdate();
        ups.clearParameters();
        if (num == 0) {
          ps.setString(1, interfaceName);
          ps.setInt(2, dataTime.intValue());
          ps.setString(3, interfaceName + dataTime + ".CHK");
          ps.setInt(4, 0);
          ps.setString(5, interfaceName + "接口还未抽取到数据或抽取校验不成功");
          ps.execute();
          ps.clearParameters();
        }
        this.con.commit();
      }

      for (String interfaceName : checkResult.get(1)) {
        ups.setInt(1, 1);
        ups.setString(2, interfaceName + "接口抽取完成");
        ups.setString(3, interfaceName);
        ups.setInt(4, dataTime.intValue());
        ups.execute();
        this.con.commit();
        ups.clearParameters();
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (ps != null) {
        ps.close();
      }
      if (ups != null)
        ups.close();
    }
  }

  private void updateTransStatus(String dbConfigureName, String data_time)
    throws Exception
  {
    StringBuffer updateStatusInterfaceSql = new StringBuffer();
    updateStatusInterfaceSql.append(" update STATUS_INTERFACE sii,(select si.IF_NO_,si.DATA_TIME_,(case when sum(sef.TRANS_STATUS_)=count(1)  then 1 else 0 end) as col2 from STATUS_ET_FILE sef left join cfg_table_interface cfg on (sef.if_no_=cfg.if_no_) ");
    updateStatusInterfaceSql.append(" left join STATUS_INTERFACE si on (sef.IF_NO_=si.IF_NO_ and sef.DATA_TIME_=si.DATA_TIME_ ) where  sef.data_time_ = " + data_time + " and  sef.FILE_NAME_ not like '%.CHK' and sef.FILE_NAME_ not like '%.VERF' and ((cfg.has_chkfile_ = 1 and si.CHK_STATUS_ = 1) or (cfg.has_chkfile_ = 0 and si.CHK_STATUS_ is null)) group by si.IF_NO_,si.DATA_TIME_) p  ");
    updateStatusInterfaceSql.append("  set sii.TRANS_STATUS_ = p.col2 where (p.if_no_ = sii.if_no_ and p.data_time_= sii.data_time_ and (sii.TRANS_STATUS_=0 or sii.TRANS_STATUS_ is null )) ");
    log.info("====20000update transStatus SQL:===" + updateStatusInterfaceSql);

    PreparedStatement ps = null;
    try {
      ps = this.con.prepareStatement(updateStatusInterfaceSql.toString());
      ps.execute();
      this.con.commit();
    } catch (Exception e) {
      throw e;
    } finally {
      if (ps != null)
        ps.close();
    }
  }

  private void updateRecordValidateStatus(String dbConfigureName)
    throws Exception
  {
    StringBuffer updateStatusInterfaceSql = new StringBuffer();
    updateStatusInterfaceSql.append("update STATUS_LD_FILE ld set VALIDATE_STATUS_=(");
    updateStatusInterfaceSql.append("  select case when tmp.NEED_CHECK_RECORD_NUM_=1 then case when tmp.ET_NUM_=tmp.LD_NUM_ then 1 else 0 end else 1 end from (");
    updateStatusInterfaceSql.append("    select sef.IF_NO_,sum(sef.FILE_RECORD_NUM_) as ET_NUM_,slf.TABLE_NAME_,sum(slf.FILE_RECORD_NUM_) as LD_NUM_,slf.DATA_TIME_,slf.FLOW_INST_ID_,cfg.NEED_CHECK_RECORD_NUM_");
    updateStatusInterfaceSql.append("      from STATUS_ET_FILE sef ,CFG_TABLE_INTERFACE cfg ,STATUS_LD_FILE slf ");
    updateStatusInterfaceSql.append("      where sef.STATUS_=1 and sef.TRANS_STATUS_=1 and sef.IF_NO_=cfg.IF_NO_ and cfg.TABLE_NAME_=slf.TABLE_NAME_ and sef.DATA_TIME_=slf.DATA_TIME_ and sef.FLOW_INST_ID_=slf.FLOW_INST_ID_ and cfg.NEED_CHECK_RECORD_NUM_=1 group by sef.IF_NO_,slf.TABLE_NAME_,sef.DATA_TIME_,slf.DATA_TIME_ ");
    updateStatusInterfaceSql.append("  ) tmp where tmp.DATA_TIME_=ld.DATA_TIME_ and tmp.TABLE_NAME_ = ld.TABLE_NAME_ and tmp.FLOW_INST_ID_ =ld.FLOW_INST_ID_ and (ld.VALIDATE_STATUS_=0 or ld.VALIDATE_STATUS_ is null)");
    updateStatusInterfaceSql.append(") where ld.VALIDATE_STATUS_=0 or ld.VALIDATE_STATUS_ is null");

    PreparedStatement ps = null;
    try {
      ps = this.con.prepareStatement(updateStatusInterfaceSql.toString());
      ps.execute();
      this.con.commit();
    } catch (Exception e) {
      throw e;
    } finally {
      if (ps != null)
        ps.close();
    }
  }

  private void updateStatusTable(Environment env, String dbConfigureName, String statusColumn, String endTimeColumn, String loadTimeColumn, String dataTimeStr)
    throws Exception
  {
    StringBuffer selectSql = new StringBuffer();

    Calendar c = Calendar.getInstance();
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
    c.setTime(dateFormatter.parse(dataTimeStr));
    c.add(5, -1);
    String beforDayStr = dateFormatter.format(c.getTime());

    selectSql.append("select distinct tmp.TABLE_NAME_,tmp.DATA_TIME_,sum(tmp.FILE_RECORD_NUM_) as DATA_RECORD_NUM_,tmp.").append(statusColumn).append(",max(tmp.").append(endTimeColumn).append(") as ").append(loadTimeColumn);
    selectSql.append(" from ( select ld.FILE_NAME_, ld.TABLE_NAME_, ld.DATA_TIME_, ld.").append(statusColumn).append(",ld.").append(endTimeColumn).append(",ld.FILE_RECORD_NUM_,ld.FLOW_INST_ID_,cfg.IF_NO_,si.CHK_STATUS_,si.TRANS_STATUS_ ");
    selectSql.append("\t\tfrom STATUS_LD_FILE ld ");
    selectSql.append("\t\tleft join CFG_TABLE_INTERFACE cfg on lower(ld.TABLE_NAME_)=cfg.TABLE_NAME_ ");
    selectSql.append("\t\tleft join STATUS_INTERFACE si on cfg.IF_NO_=si.IF_NO_ and si.DATA_TIME_=ld.DATA_TIME_ ");
    selectSql.append("\twhere exists (select 1 from STATUS_ET_FILE et where et.FLOW_INST_ID_ is not null and et.FLOW_INST_ID_=ld.FLOW_INST_ID_) ");
    selectSql.append("\tand si.TRANS_STATUS_=1  ");
    selectSql.append("  and ld.data_time_ = " + dataTimeStr);
    selectSql.append("\tand ((si.CHK_STATUS_=1 and cfg.HAS_CHKFILE_=1) or cfg.HAS_CHKFILE_ !=1) ");
    selectSql.append("\tand not exists ( ");
    selectSql.append("\t\tselect 1 from (select case when cti.DAY_OR_MONTH_=1 then date_format(date_add(DATA_TIME_, INTERVAL cti.DATE_OP_VALUE*-1 DAY),'%Y%m%d') else concat(substr(date_format(date_add(DATA_TIME_, INTERVAL cti.DATE_OP_VALUE*-1 MONTH),'%Y%m%d'),1,6),'%') end as DATA_TIME_1,st.* ");
    selectSql.append("\t\tfrom STATUS_TABLE st join CFG_TABLE_INTERFACE cti on lower(st.TABLE_NAME_)=(cti.TABLE_NAME_) and st.data_time_ = " + beforDayStr + "  ) stt ");
    selectSql.append("\t\twhere stt.TABLE_NAME_=lower(ld.TABLE_NAME_) and ld.DATA_TIME_ like stt.DATA_TIME_1 and stt.").append(statusColumn).append(" in (1,99)) ");
    selectSql.append(") tmp ");
    selectSql.append(" group by tmp.TABLE_NAME_,tmp.DATA_TIME_ having count(tmp.TABLE_NAME_)=sum(tmp.").append(statusColumn).append(") ");

    StringBuffer updateSql = new StringBuffer();
    updateSql.append("update STATUS_TABLE set DATA_RECORD_NUM_=?, ").append(statusColumn).append("=?,").append(loadTimeColumn).append("=?");
    updateSql.append(" where TABLE_NAME_=? and DATA_TIME_=? ");

    StringBuffer insertSql = new StringBuffer();
    insertSql.append("insert into STATUS_TABLE (TABLE_NAME_,DATA_TIME_,DATA_RECORD_NUM_,").append(statusColumn).append(",").append(loadTimeColumn).append(") ");
    insertSql.append("values(?,?,?,?,?)");

    PreparedStatement selectPs = null;
    PreparedStatement updatePs = null;
    PreparedStatement insertPs = null;
    ResultSet rs = null;
    try {
      selectPs = this.con.prepareStatement(selectSql.toString());
      updatePs = this.con.prepareStatement(updateSql.toString());
      insertPs = this.con.prepareStatement(insertSql.toString());
      log.info("======300000==in ValidateDataNode  Class=================执行查询表加载状态SQL[" + selectSql + "]");
      writeLocalLog("执行查询表加载状态SQL[" + selectSql + "]");
      rs = selectPs.executeQuery();
      while (rs.next()) {
        String tableName = rs.getString(1);
        Integer dataTime = Integer.valueOf(rs.getInt(2));
        Long redordNum = Long.valueOf(rs.getLong(3));
        String status = rs.getString(4);

        if ((tableName.equalsIgnoreCase("cdr_ismg_dm")) || (tableName.equalsIgnoreCase("cdr_wap_dm")) || (tableName.equalsIgnoreCase("cdr_mms_dm")) || (tableName.equalsIgnoreCase("cdr_mmring_dm"))) {
          status = "99";
        }
        Timestamp endTime = rs.getTimestamp(5);
        DataTimeUtil dtu = new DataTimeUtil();
        Integer newDataTime = Integer.valueOf(dtu.dealDate(env, "", tableName, dataTime+"", "yyyyMMdd"));
        log.info("表名[" + tableName + "],数据日期[" + newDataTime + "],记录数[" + redordNum + "],加载状态[" + status + "],结束时间[" + endTime + "]");
        writeLocalLog("表名[" + tableName + "],数据日期[" + newDataTime + "],记录数[" + redordNum + "],加载状态[" + status + "],结束时间[" + endTime + "]");
        updatePs.setLong(1, redordNum.longValue());
        updatePs.setString(2, status);
        updatePs.setTimestamp(3, endTime);
        updatePs.setString(4, tableName);
        updatePs.setInt(5, newDataTime.intValue());
        log.info("执行更新表加载状态SQL[" + updateSql + "]");
        writeLocalLog("执行更新表加载状态SQL[" + updateSql + "]");
        int updateNum = updatePs.executeUpdate();
        if (updateNum == 0)
        {
          if ((tableName.equalsIgnoreCase("cdr_ismg_dm")) || (tableName.equalsIgnoreCase("cdr_wap_dm")) || (tableName.equalsIgnoreCase("cdr_mms_dm")) || (tableName.equalsIgnoreCase("cdr_mmring_dm"))) {
            status = "99";
          }
          insertPs.setString(1, tableName);
          insertPs.setInt(2, newDataTime.intValue());
          insertPs.setLong(3, redordNum.longValue());
          insertPs.setString(4, status);
          insertPs.setTimestamp(5, endTime);
          log.info("执行插入表加载状态SQL[" + insertSql + "]");
          writeLocalLog("执行插入表加载状态SQL[" + insertSql + "]");
          insertPs.execute();
        }
        this.con.commit();
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (selectPs != null) {
        selectPs.close();
      }
      if (updatePs != null) {
        updatePs.close();
      }
      if (insertPs != null) {
        insertPs.close();
      }
      if (rs != null)
        rs.close();
    }
  }

  private void updateValidateStatusToDB(String dbConfigureName, Map<Integer, List<String>> checkResult, Map<String, Integer> interfaceCheckStatus)
    throws Exception
  {
    StringBuffer updateStatusEtFileSql = new StringBuffer("update STATUS_ET_FILE set VALIDATE_STATUS_=?,DESC_=? where FILE_NAME_=?");
    StringBuffer insertStatusEtFileSql = new StringBuffer("insert into STATUS_ET_FILE(IF_NO_,DATA_TIME_,FILE_NAME_,VALIDATE_STATUS_,DESC_) values (?,?,?,?,?)");
    StringBuffer isnertStatusInterfaceSql = new StringBuffer();
    isnertStatusInterfaceSql.append("insert into STATUS_INTERFACE(IF_NO_,DATA_TIME_,CHK_FILE_NAME_,FILE_NUM_,DATA_RECORD_NUM_) ");
    isnertStatusInterfaceSql.append(" select ");
    isnertStatusInterfaceSql.append("   sef.IF_NO_,");
    isnertStatusInterfaceSql.append("   sef.DATA_TIME_,");
    isnertStatusInterfaceSql.append("   case when sef.IF_NO_ like '%.VERF' then sef.IF_NO_ else concat(sef.IF_NO_, sef.DATA_TIME_, '.CHK') end as CHK_FILE_NAME_,");
    isnertStatusInterfaceSql.append("   count(sef.FILE_NAME_) as FILE_NUM_,");
    isnertStatusInterfaceSql.append("   sum(sef.FILE_RECORD_NUM_) as DATA_RECORD_NUM_ ");
    isnertStatusInterfaceSql.append(" from STATUS_ET_FILE sef ");
    isnertStatusInterfaceSql.append(" where not exists( select 1 ");
    isnertStatusInterfaceSql.append("     from STATUS_INTERFACE si ");
    isnertStatusInterfaceSql.append("     where case when sef.IF_NO_ like '%.VERF' then sef.IF_NO_ else concat(sef.IF_NO_, sef.DATA_TIME_, '.CHK') end = si.CHK_FILE_NAME_) ");
    isnertStatusInterfaceSql.append(" group by sef.IF_NO_ , sef.DATA_TIME_");
    StringBuffer updateStatusInterfaceSql = new StringBuffer("update STATUS_INTERFACE set CHK_STATUS_=?,DESC_=? where CHK_FILE_NAME_=?");
    PreparedStatement ps = null;
    PreparedStatement ups = null;
    PreparedStatement ips = null;
    PreparedStatement ups2 = null;
    try
    {
      ips = this.con.prepareStatement(isnertStatusInterfaceSql.toString());
      ips.execute();
      this.con.commit();

      if ((checkResult.get(Integer.valueOf(0)) != null) && (((List)checkResult.get(Integer.valueOf(0))).size() > 0)) {
        ups = this.con.prepareStatement(updateStatusEtFileSql.toString());
        ps = this.con.prepareStatement(insertStatusEtFileSql.toString());

        for (String fileName : checkResult.get(0)) {
          if ((StringUtils.trimToNull(fileName) != null) && (fileName.length() == 24)) {
            ups.setInt(1, 0);
            ups.setString(2, fileName + "文件还未抽取到");
            ups.setString(3, fileName);
            int num = ups.executeUpdate();
            ups.clearParameters();
            if (num == 0) {
              ps.setString(1, fileName.substring(0, 6));
              ps.setInt(2, Integer.valueOf(fileName.substring(6, 14)).intValue());
              ps.setString(3, fileName);
              ps.setInt(4, 0);
              ps.setString(5, fileName + "文件还未抽取到");
              ps.execute();
              ps.clearParameters();
            }
          }
          this.con.commit();
        }

      }

      if ((checkResult.get(Integer.valueOf(1)) != null) && (((List)checkResult.get(Integer.valueOf(1))).size() > 0)) {
        ups = this.con.prepareStatement(updateStatusEtFileSql.toString());
        for (String fileName : checkResult.get(1)) {
          ups.setInt(1, 1);
          ups.setString(2, fileName + "文件校验成功");
          ups.setString(3, fileName);
          ups.execute();
          this.con.commit();
          ups.clearParameters();
        }

      }

      if ((interfaceCheckStatus.entrySet() != null) && (interfaceCheckStatus.entrySet().size() > 0)) {
        ups2 = this.con.prepareStatement(updateStatusInterfaceSql.toString());
        for (Map.Entry entry : interfaceCheckStatus.entrySet()) {
          ups2.setInt(1, ((Integer)entry.getValue()).intValue());
          ups2.setString(2, (String)entry.getKey() + (((Integer)entry.getValue()).intValue() == 0 ? "文件未抽取到" : "文件抽取成功"));
          ups2.setString(3, (String)entry.getKey());
          ups2.execute();
          this.con.commit();
          ups2.clearParameters();
        }
      }
    }
    catch (Exception e) {
      throw e;
    } finally {
      if (ips != null) {
        ips.close();
      }
      if (ps != null) {
        ps.close();
      }
      if (ups != null) {
        ups.close();
      }
      if (ups2 != null)
        ups2.close();
    }
  }

  private List<Map<String, String>> getUnCheckFileFromETTable(String dbConfigureName, String dateTime)
    throws Exception
  {
    List result = new ArrayList();
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      StringBuffer sql = new StringBuffer();
      sql.append("select IF_NO_,DATA_TIME_,FILE_NAME_ from STATUS_ET_FILE etf where etf.data_time_ = " + dateTime + "  and (STATUS_ = 1 and (VALIDATE_STATUS_ = 0 or VALIDATE_STATUS_ is null)) or exists (select 1 from STATUS_INTERFACE where (CHK_STATUS_ = 0 or CHK_STATUS_ is null) and IF_NO_ = etf.IF_NO_ and DATA_TIME_ = etf.DATA_TIME_ and etf.STATUS_ = 1 and data_time_ = " + dateTime + ")");
      ps = this.con.prepareStatement(sql.toString());
      rs = ps.executeQuery();
      String ifNo = null;
      String dataTime = null;
      String fileName = null;
      log.info("======30000===============   validate ftp file from ftpserver SQL:" + sql);
      while (rs.next()) {
        ifNo = rs.getString(1);
        dataTime = rs.getString(2);
        fileName = rs.getString(3);
        if ((ifNo != null) && (dataTime != null) && (fileName != null)) {
          Map dataMap = new HashMap();
          dataMap.put("INTERFACE_NAME", ifNo);
          dataMap.put("V_DATE", dataTime);
          dataMap.put("FILE_NAME", fileName);
          result.add(dataMap);
        }
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (rs != null) {
        rs.close();
      }
      if (ps != null) {
        ps.close();
      }
    }
    return result;
  }

  private boolean isChecked(String dbConfigureName, String fileName)
    throws Exception
  {
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      StringBuffer sql = new StringBuffer();
      sql.append("select CHK_FILE_NAME_ from STATUS_INTERFACE where CHK_FILE_NAME_=? and CHK_STATUS_=1");
      ps = this.con.prepareStatement(sql.toString());
      ps.setString(1, fileName);
      rs = ps.executeQuery();
      String res = null;
      while (rs.next()) {
        res = rs.getString(1);
      }
      if (StringUtils.trimToNull(res) == null)
        return false;
    }
    catch (Exception e) {
      throw e;
    } finally {
      if (rs != null) {
        rs.close();
      }
      if (ps != null)
        ps.close();
    }
    if (rs != null) {
      rs.close();
    }
    if (ps != null) {
      ps.close();
    }

    return true;
  }

  private List<Map<String, String>> getIngerfaceFromInterfaceTable(String dbConfigureName, Integer dataTime)
    throws Exception
  {
    List result = new ArrayList();
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      StringBuffer sql = new StringBuffer();
      sql.append("select distinct IF_NO_ from STATUS_ET_FILE sef where sef.DATA_TIME_=? and sef.STATUS_=1");
      ps = this.con.prepareStatement(sql.toString());
      ps.setInt(1, dataTime.intValue());
      rs = ps.executeQuery();
      String column = null;
      while (rs.next()) {
        column = rs.getString(1);
        if (column != null) {
          Map dataMap = new HashMap();
          dataMap.put("INTERFACE_NAME", column);
          result.add(dataMap);
        }
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (rs != null) {
        rs.close();
      }
      if (ps != null) {
        ps.close();
      }
    }
    return result;
  }
  private List<String> parseListMapToList(List<Map<String, String>> srcList) {
    List result = new ArrayList(srcList.size());
    for (Map map : srcList) {
      result.add((String)map.get("INTERFACE_NAME"));
    }
    return result;
  }

  private Map<String, List<String>> parseResult(List<Map<String, String>> srcList)
  {
    Map result = new HashMap();
    for (Map map : srcList) {
      String key = (String)map.get("INTERFACE_NAME") + (String)map.get("V_DATE");
      String fileName = (String)map.get("FILE_NAME");
      List list = (List)result.get(key);
      if (list == null) {
        list = new ArrayList();
        list.add(fileName);
        result.put(key, list);
      } else {
        list.add(fileName);
      }
    }
    return result;
  }

  private List<String> parseList(List<List<String>> srcList)
  {
    List result = new ArrayList();
    for (List list : srcList) {
      result.add((String)list.get(0));
    }
    return result;
  }

  private List<List<String>> readCheckFile(String path, String fileColumnSplit)
    throws Exception
  {
    List result = new ArrayList();
    BufferedReader br = null;
    InputStream in = null;
    try {
      if ((fileColumnSplit == null) || ("".equals(fileColumnSplit))) {
        fileColumnSplit = "\t";
      }
      FileSystem fs = FileSystem.get(new Configuration());
      Path filePath = new Path(path);
      in = fs.open(filePath);
      log.info("Read file path[" + filePath + "]");
      writeLocalLog("Read file path[" + filePath + "]");
      br = new BufferedReader(new InputStreamReader(in));
      List lineList = null;
      String line = null;
      while ((line = br.readLine()) != null)
        if (!"".equals(line)) {
          String[] columns = line.split(fileColumnSplit);

          if ((columns != null) && (columns.length == 1)) {
            columns = StringUtils.splitByWholeSeparator(line, " ");
          }
          lineList = Arrays.asList(columns);
          if ((lineList != null) && (lineList.size() > 0))
            result.add(lineList);
        }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    } finally {
      if (br != null) {
        br.close();
      }
      if (in != null) {
        in.close();
      }
    }
    return result;
  }

  public void releaseResource(HisActivity hisAct)
  {
    log.info("releaseResource method is not implement!");
  }

  public JobResult getState()
  {
    return this.result;
  }
}