package com.asiainfo.local.cmc.sd;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.constant.ConstCommonParams;
import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
import com.ailk.cloudetl.commons.internal.tools.StringService;
import com.ailk.cloudetl.commons.utils.VelocityUtil;
import com.ailk.cloudetl.dbservice.platform.Platform;
import com.ailk.cloudetl.dbservice.platform.PlatformProperty;
import com.ailk.cloudetl.dbservice.platform.PlatformTypeProperty;
import com.ailk.cloudetl.dbservice.platform.api.PlatService;
import com.ailk.cloudetl.dbservice.platform.internal.PlatServiceImpl;
import com.ailk.cloudetl.el.common.CommonConf;
import com.ailk.cloudetl.el.common.ShellUtil;
import com.ailk.cloudetl.el.common.db.DbCommon;
import com.ailk.cloudetl.el.common.db.LoaderResult;
import com.ailk.cloudetl.el.common.util.PathUtil;
import com.ailk.cloudetl.el.common.util.PathUtil.PATH_TYPE;
import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
import com.ailk.udf.node.shandong.DataTimeUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

public class GbaseLoadMutilThreadNoMR extends DbCommon
  implements UDFActivity
{
  private static final Log log = LogFactory.getLog(GbaseLoadMutilThreadNoMR.class);
  private ThreadPoolExecutor loadThreadPool;
  private int maxDealTableNum = -1;
  private int loadThreadNumer = 3;
  private int copyThreadNumer = 5;

  private boolean singleLog = true;
  private Logger localLogger = null;

  private JobResult result = JobResult.RIGHT;

  private String loadType = "link";

  private SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
  {
    initLocalLog(env, udfParams);
    writeLocalLog("开始执行gbaseload节点：" + 
      GbaseLoadMutilThreadNoMR.class.getName());
    Map<String, Object> retMap = new HashMap<String, Object>();
    Map<String, String> paramsMap = new HashMap<String, String>();
    paramsMap = udfParams;
    String platName = (String)paramsMap.get("platname");
    PlatService platService = new PlatServiceImpl();
    String fuseHome = ObjectUtils.toString(EnvironmentImpl.getCurrent()
      .get("com.ailk.cloudetl.el.fuseHome"));
    paramsMap.put("fusehome", fuseHome);
    log.info("fusehome=" + fuseHome);
    writeLocalLog("fusehome=" + fuseHome);
    Platform pf = null;
    try {
      pf = platService.selectPlatformFromName(platName);
      for (PlatformProperty pp : pf.getPfProperties()) {
        log.info(pp.getPfTypeProperty().getName() + "=" + 
          pp.getPropValue());
        paramsMap.put(pp.getPfTypeProperty().getName(), 
          pp.getPropValue());
      }
    } catch (Exception e) {
      this.result = JobResult.ERROR;
      log.error(e.getMessage(), e);
      writeLocalLog(e, e.getMessage());
      retMap.put("ERROR", e);
      return retMap;
    }

    String maxDealNum = (String)paramsMap.get("maxDealTableNum");
    if (StringUtils.isNotBlank(maxDealNum)) {
      this.maxDealTableNum = Integer.parseInt(maxDealNum);
    }

    String threadNum = (String)paramsMap.get("loadthreadNum");
    if (StringUtils.isNotBlank(threadNum)) {
      this.loadThreadNumer = Integer.parseInt(threadNum);
    }

    String cpThreadNum = (String)paramsMap.get("copythreadNum");
    if (StringUtils.isNotBlank(cpThreadNum)) {
      this.copyThreadNumer = Integer.parseInt(cpThreadNum);
    }

    String gbaseLoadType = (String)paramsMap.get("gbaseloadType");
    if (StringUtils.isNotBlank(gbaseLoadType)) {
      this.loadType = gbaseLoadType;
    }
    String isReloadFailTable = (String)paramsMap.get("IS_RELOAD_FAIL_TABLE");
    String appendSql = Boolean.valueOf(isReloadFailTable).booleanValue() ? " a.GBASE_LOAD_STATUS_=0 or " : 
      "";
    try
    {
      getLoadResults(paramsMap, appendSql);
    } catch (Exception e) {
      this.result = JobResult.ERROR;
      retMap.put("ERROR", e);
    }
    return retMap;
  }

  private void getLoadResults(Map<String, String> paramsMap, String appendSql) {
    Map<String,List> tempMap = new HashMap<String,List>();
    log.info("开始扫描待加载表...");
    writeLocalLog("开始扫描待加载表...");
    ResultSet rs = null;
    Connection conn = null;
    Session session = null;
    PreparedStatement ps = null;
    try {
      session = HibernateUtils.getSessionFactory().openSession();
      conn = session.connection();
      String tableNameFromWeb = (String)paramsMap.get("tablename");
      String datatime = (String)paramsMap.get("datatime");
      String important = (String)paramsMap.get("important");
      StringBuilder querySql = new StringBuilder(
        "select distinct a.TABLE_NAME_, a.DATA_TIME_, a.FILE_NAME_, a.FILE_PATH_, b.LOAD_ACTION_, b.TIME_WINDOW_, b.DAY_FLAG_, a.VALIDATE_STATUS_,b.NEED_CHECK_RECORD_NUM_, a.FILE_RECORD_NUM_  from STATUS_LD_FILE a, (select distinct IF_NO_,TABLE_NAME_,LOAD_STYLE_,GBASE_STATUS_,NEED_CHECK_RECORD_NUM_,DAY_FLAG_,TIME_WINDOW_,IS_PARTITION_,GBASE_LOAD_ACTION_ AS LOAD_ACTION_, important_ as  important,PRI_  from CFG_TABLE_INTERFACE) b where upper(a.TABLE_NAME_)=upper(b.TABLE_NAME_) and (" + 
        appendSql + 
        " a.GBASE_LOAD_STATUS_ is null) " + 
        "and ((b.NEED_CHECK_RECORD_NUM_= 1 and a.VALIDATE_STATUS_ = 1) or (b.NEED_CHECK_RECORD_NUM_ is null)) " + 
        "and b.GBASE_STATUS_=1 " + 
        "and b.LOAD_STYLE_!=3 " + 
        "and b.important  = " + important + 
        " and exists (select 1 from STATUS_ET_FILE where FLOW_INST_ID_ is not null and a.FLOW_INST_ID_=FLOW_INST_ID_) " + 
        "and not exists (select 1 from STATUS_INTERFACE where b.LOAD_ACTION_=0 and upper(a.TABLE_NAME_)=upper(b.TABLE_NAME_) and b.IF_NO_=IF_NO_ and DATA_TIME_=a.DATA_TIME_ and (TRANS_STATUS_=0 or TRANS_STATUS_ is null))");
      if (StringUtils.isNotBlank(tableNameFromWeb)) {
        querySql.append("and a.table_name_='" + tableNameFromWeb + "'");
      }
      if (StringUtils.isNotBlank(datatime)) {
        querySql.append("and a.DATA_TIME_='" + datatime + "'");
      }
      querySql.append("order by b.PRI_ desc");
      ps = conn.prepareStatement(querySql.toString());
      rs = ps.executeQuery();
      log.info("sql execute success");
      String LOAD_ACTION_;
      String VALIDATE_STATUS_;
      if (rs != null) {
        int i = 0;
        while (rs.next()) {
          i++; log.info("to load files : " + i);
          Map<String,String> dataMap = new HashMap<String,String>();
          dataMap.putAll(paramsMap);

          String TABLE_NAME_ = rs.getString(1);
          String DATA_TIME_ = rs.getString(2);
          String FILE_NAME_ = rs.getString(3);
          String FILE_PATH_ = rs.getString(4);
          LOAD_ACTION_ = rs.getString(5);

          String TIME_WINDOW_ = rs.getString(6);
          String DAY_FLAG_ = rs.getString(7);
          VALIDATE_STATUS_ = rs.getString(8);
          String NEED_CHECK_RECORD_NUM_ = rs.getString(9);
          String FILE_RECORD_NUM_ = rs.getString(10);

          dataMap.put("TABLE_NAME_", TABLE_NAME_);
          dataMap.put("DATA_TIME_", DATA_TIME_);
          dataMap.put("FILE_NAME_", FILE_NAME_);
          dataMap.put("FILE_PATH_", FILE_PATH_);
          dataMap.put("LOAD_ACTION_", LOAD_ACTION_);

          dataMap.put("TIME_WINDOW_", TIME_WINDOW_);
          dataMap.put("DAY_FLAG_", DAY_FLAG_);
          dataMap.put("FILE_RECORD_NUM_", FILE_RECORD_NUM_);
          String info = "配置信息: 目标表[" + TABLE_NAME_ + "]数据日期[" + 
            DATA_TIME_ + "]路径[" + FILE_PATH_ + "]文件名[" + 
            FILE_NAME_ + "]是否追加[" + LOAD_ACTION_ + "]需要加载时间[" + 
            TIME_WINDOW_ + "]月接口加载时间[" + DAY_FLAG_ + 
            "]是否校验记录行数[" + NEED_CHECK_RECORD_NUM_ + "]校验状态[" + 
            VALIDATE_STATUS_ + "]";
          log.info(info);
          writeLocalLog(info);
          Date d = new Date();
          Date compare = new Date();
          if ((TIME_WINDOW_ != null) && (DAY_FLAG_ != null)) {
            log.info("TIME_WINDOW_ is  " + TIME_WINDOW_ + 
              " DAY_FLAG_ is " + DAY_FLAG_);
            writeLocalLog("TIME_WINDOW_ is  " + TIME_WINDOW_ + 
              " DAY_FLAG_ is " + DAY_FLAG_);
            String[] ss = TIME_WINDOW_.toString().split(":");
            compare.setHours(Integer.valueOf(ss[0]).intValue());
            compare.setMinutes(Integer.valueOf(ss[1]).intValue());
            compare.setSeconds(Integer.valueOf(ss[2]).intValue());
            if (!d.after(compare))
              continue;
            if (Integer.valueOf(DAY_FLAG_.toString()).intValue() <= d
              .getDate())
              splitMap(dataMap, tempMap);
          }
          else if (TIME_WINDOW_ != null) {
            log.info("TIME_WINDOW_ is  " + TIME_WINDOW_ + 
              " DAY_FLAG_ is " + DAY_FLAG_);
            writeLocalLog("TIME_WINDOW_ is  " + TIME_WINDOW_ + 
              " DAY_FLAG_ is " + DAY_FLAG_);
            String[] ss = TIME_WINDOW_.toString().split(":");
            compare.setHours(Integer.valueOf(ss[0]).intValue());
            compare.setMinutes(Integer.valueOf(ss[1]).intValue());
            compare.setSeconds(Integer.valueOf(ss[2]).intValue());
            if (d.after(compare))
              splitMap(dataMap, tempMap);
          }
          else if (DAY_FLAG_ != null) {
            log.info("TIME_WINDOW_ is  " + TIME_WINDOW_ + 
              " DAY_FLAG_ is " + DAY_FLAG_);
            writeLocalLog("TIME_WINDOW_ is  " + TIME_WINDOW_ + 
              " DAY_FLAG_ is " + DAY_FLAG_);

            if (Integer.valueOf(DAY_FLAG_.toString()).intValue() <= d
              .getDate())
              splitMap(dataMap, tempMap);
          }
          else {
            log.info("TIME_WINDOW_ is  null DAY_FLAG_ is null");
            writeLocalLog("TIME_WINDOW_ is  null DAY_FLAG_ is null");
            splitMap(dataMap, tempMap);
          }
        }
      }
      Map tableCfgs = getAllTableCfg();

      String shellPlatform = (String)paramsMap.get("shellPlatform");
      PlatService platService = new PlatServiceImpl();
      Platform pf = null;
      Map shellMap = new HashMap();
      try {
        pf = platService.selectPlatformFromName(shellPlatform);
        for (PlatformProperty pp : pf.getPfProperties()) {
          log.info(pp.getPfTypeProperty().getName() + "=" + 
            pp.getPropValue());
          writeLocalLog(pp.getPfTypeProperty().getName() + "=" + 
            pp.getPropValue());
          shellMap.put(pp.getPfTypeProperty().getName(), 
            pp.getPropValue());
        }
      } catch (Exception e) {
        throw e;
      }

      List<TableLoadThread> loadThreadList = new ArrayList<TableLoadThread>();
      if (tempMap.size() > 0) {
        LinkedBlockingQueue<Runnable> linkedBlockingQueue = new LinkedBlockingQueue<Runnable>();
        this.loadThreadPool = 
          new ThreadPoolExecutor(this.loadThreadNumer, 
          2147483647, 1L, TimeUnit.SECONDS, 
          linkedBlockingQueue);
        writeLocalLog("待处理任务数为：" + tempMap.size() + 
          ",当前配置的最大load线程数：" + this.loadThreadNumer);
        writeLocalLog("开始利用多线程执行gbaseload...");
        int index = 0;
        for (List<Map<String,String>> list : tempMap.values()) {
        	TableLoadThread loadThread = new TableLoadThread(session,list, paramsMap, shellMap, tableCfgs, ""+(index++), EnvironmentImpl.getCurrent());
          this.loadThreadPool.execute(loadThread);
          loadThreadList.add(loadThread);
        }
//        TableLoadThread loadThread;
        while (true) {
          boolean allDone = true;
          for (TableLoadThread loadThread : loadThreadList) { 
//         for (loadThread = loadThreadList.iterator(); loadThread.hasNext(); ) { 
//        	  loadThread = (TableLoadThread)loadThread.next();
            if (!loadThread.isFinish) {
              allDone = false;
              break;
            }
          }
          if (allDone) {
            break;
          }
          Thread.sleep(1000L);
        }

        for (TableLoadThread loadThread : loadThreadList) {
          LoaderResult loadResult = loadThread.getLoadResult();
          if (!loadResult.isSuccess()) {
            this.result = JobResult.ERROR;
            this.retMap.put("ERROR", loadResult.getDetail());
          }
        }
      }
    } catch (Exception e) {
      if (session.getTransaction().isActive())
        session.getTransaction().rollback();
    }
    finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (ps != null) {
          ps.close();
        }
        if (session != null)
          session.close();
      }
      catch (SQLException e) {
        log.error(e.getMessage(), e);
        writeLocalLog(e, e.getMessage());
      }
      if ((this.loadThreadPool != null) && (!this.loadThreadPool.isShutdown()))
        this.loadThreadPool.shutdown();
    }
  }

  private Map<String, String[]> getAllTableCfg()
  {
    Map result = new HashMap();
    try {
      List<Object[]> cfgList = 
        excuteQuerySQL("select distinct TABLE_NAME_,GBASE_TABLE_NAME_,DELETE_KEYS_,ALLOW_ERROR_PERCENT_,EXTRA_LOADER_ARGS_ from CFG_TABLE_INTERFACE");
      for (Object[] objs : cfgList)
        result.put(String.valueOf(objs[0]), new String[] { 
          objs[1] == null ? null : String.valueOf(objs[1]), 
          objs[2] == null ? null : String.valueOf(objs[2]), 
          objs[3] == null ? null : String.valueOf(objs[3]), 
          objs[4] == null ? null : String.valueOf(objs[4]) });
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }

  private String getGbaseTableName(String tableName, Map<String, String[]> tableCfgs, String dataTime, String dataTimeFormat)
  {
    String gbaseTableName = tableName;
    try {
      String[] cfgData = (String[])tableCfgs.get(tableName.toLowerCase());
      String tableFormat = cfgData == null ? null : cfgData[0];
      if (tableFormat != null) {
        gbaseTableName = tableFormat;
        String[] dateFormatStrs = StringUtils.substringsBetween(
          tableFormat, "{", "}");
        if (dateFormatStrs != null) {
          SimpleDateFormat in = new SimpleDateFormat(dataTimeFormat);
          for (String format : dateFormatStrs) {
            SimpleDateFormat out = new SimpleDateFormat(format);
            gbaseTableName = gbaseTableName.replace("{" + format + 
              "}", out.format(in.parse(dataTime)));
          }
        }
      }
    } catch (ParseException e) {
      e.printStackTrace();
      log.error("GBase表名转换出错", e);
      writeLocalLog(e, "GBase表名转换出错");
      throw new RuntimeException("GBase表名转换出错", e);
    }
    return gbaseTableName;
  }

  private void splitMap(Map dataMap, Map<String, List> tempMap)
  {
    String TABLE_NAME_ = (String)dataMap.get("TABLE_NAME_");
    String LOAD_ACTION_ = (String)dataMap.get("LOAD_ACTION_");

    String DATA_TIME_ = (String)dataMap.get("DATA_TIME_");

    if ("0".equals(LOAD_ACTION_)) {
      if (tempMap.get(TABLE_NAME_ + DATA_TIME_) == null) {
        if ((this.maxDealTableNum < 0) || (tempMap.size() < this.maxDealTableNum)) {
          List list = new ArrayList();
          list.add(dataMap);
          tempMap.put(TABLE_NAME_ + DATA_TIME_, list);
        }
      }
      else ((List)tempMap.get(TABLE_NAME_ + DATA_TIME_)).add(dataMap);
    }
    else if ("1".equals(LOAD_ACTION_))
      if (tempMap.get(TABLE_NAME_ + DATA_TIME_) == null) {
        if ((this.maxDealTableNum < 0) || (tempMap.size() < this.maxDealTableNum)) {
          List list = new ArrayList();
          list.add(dataMap);
          tempMap.put(TABLE_NAME_ + DATA_TIME_, list);
        }
      }
      else ((List)tempMap.get(TABLE_NAME_ + DATA_TIME_)).add(dataMap);
  }

  private List<Object[]> excuteQuerySQL(String sql)
    throws Exception
  {
    Session session = null;
    List list = new ArrayList();
    try {
      session = HibernateUtils.getSessionFactory().openSession();
      SQLQuery q = session.createSQLQuery(sql);
      list = q.list();
    } catch (Exception e) {
      e.printStackTrace();
      log.error("excute SQL[" + sql + "] Failed", e);
      writeLocalLog(e, null);
      throw e;
    } finally {
      if (session != null) {
        session.close();
      }
    }
    return list;
  }

  private void start_updateTable(Session session, List list, String threadName)
    throws SQLException
  {
    Connection conn = null;
    PreparedStatement ps = null;
    log.info(threadName + "start update STATUS_LD_FILE!!!");
    try
    {
      conn = session.connection();
      conn.setAutoCommit(false);
      String sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set GBASE_LOAD_STATUS_=2,GBASE_LOAD_START_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      ps = conn.prepareStatement(sql4sqlSTATUS_LD_FILE);
      java.util.Date d = new java.util.Date();
      java.sql.Date date = new java.sql.Date(d.getTime());
      for (int i = 0; i < list.size(); i++) {
        Map dataMap = (Map)list.get(i);
        String file_path_ = (String)dataMap.get("FILE_PATH_");
        String file_name_ = (String)dataMap.get("FILE_NAME_");
        ps.setString(1, this.dateformatter.format(date));
        ps.setString(2, file_path_);
        ps.setString(3, file_name_);
        ps.execute();
        conn.commit();
      }
    } finally {
      try {
        if (ps != null)
          ps.close();
      }
      catch (Exception e) {
        log.error(e.getMessage(), e);
        writeLocalLog(e, null);
      }
    }
  }

  private void updateLoadStatus(Session session, List list, String threadName) throws SQLException {
    Connection conn = null;
    PreparedStatement ps = null;
    log.info(threadName + "start update STATUS_LD_FILE 文件gbase_load_status为null,等待下一次加载!!!");
    try
    {
      conn = session.connection();
      conn.setAutoCommit(false);
      String sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set GBASE_LOAD_STATUS_=null  where TABLE_NAME_=? AND DATA_TIME_=? and FILE_NAME_=?";
      ps = conn.prepareStatement(sql4sqlSTATUS_LD_FILE);
      for (int i = 0; i < list.size(); i++) {
        Map dataMap = (Map)list.get(i);
        String table_name_ = (String)dataMap.get("TABLE_NAME_");
        String data_time_ = (String)dataMap.get("DATA_TIME_");
        String file_name_ = (String)dataMap.get("FILE_NAME_");
        ps.setString(1, table_name_);
        ps.setString(2, data_time_);
        ps.setString(3, file_name_);
        ps.execute();
        conn.commit();
      }
    } finally {
      try {
        if (ps != null)
          ps.close();
      }
      catch (Exception e) {
        log.error(e.getMessage(), e);
        writeLocalLog(e, null);
      }
    }
  }

  private void end_updateTable(Session session, Map udfParams, List list, LoaderResult loadResult, String tableName, String threadName)
    throws SQLException
  {
    log.info(threadName + "start update STATUS_LD_FILE ,loadState is  " + loadResult.isSuccess() + "!!!");
    log.info(threadName + "tableName:" + tableName);
    log.info(threadName + StringService.stringifyObject(loadResult));
    Connection conn = null;
    PreparedStatement ps = null;

    Long tableRecords = Long.valueOf(0L);
    Integer dataTime = Integer.valueOf(0);
    conn = session.connection();
    conn.setAutoCommit(false);
    try {
      String sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set GBASE_LOAD_STATUS_=?,GBASE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      ps = conn.prepareStatement(sql4sqlSTATUS_LD_FILE);
      java.util.Date d = new java.util.Date();
      java.sql.Date date = new java.sql.Date(d.getTime());
      for (int i = 0; i < list.size(); i++) {
        Map dataMap = (Map)list.get(i);
        String file_path_ = (String)dataMap.get("FILE_PATH_");
        String file_name_ = (String)dataMap.get("FILE_NAME_");
        String file_record_num_ = (String)dataMap.get("FILE_RECORD_NUM_") == null ? "0" : (String)dataMap.get("FILE_RECORD_NUM_");
        tableRecords = Long.valueOf(tableRecords.longValue() + Long.valueOf(file_record_num_).longValue());
        ps.setString(1, loadResult.isSuccess() ? "1" : "0");
        ps.setString(2, this.dateformatter.format(date));
        ps.setString(3, file_path_);
        ps.setString(4, file_name_);
        ps.execute();
        conn.commit();
        if (i == list.size() - 1)
          dataTime = Integer.valueOf((String)dataMap.get("DATA_TIME_"));
      }
    }
    catch (Exception e) {
      tableRecords = Long.valueOf(0L);
      String sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set GBASE_LOAD_STATUS_=?,GBASE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      ps = conn.prepareStatement(sql4sqlSTATUS_LD_FILE);
      java.util.Date d = new java.util.Date();
      java.sql.Date date = new java.sql.Date(d.getTime());
      for (int i = 0; i < list.size(); i++) {
        Map dataMap = (Map)list.get(i);
        String file_path_ = (String)dataMap.get("FILE_PATH_");
        String file_name_ = (String)dataMap.get("FILE_NAME_");
        String file_record_num_ = (String)dataMap.get("FILE_RECORD_NUM_");
        tableRecords = Long.valueOf(tableRecords.longValue() + Long.valueOf(file_record_num_).longValue());
        ps.setString(1, loadResult.isSuccess() ? "1" : "0");
        ps.setString(2, this.dateformatter.format(date));
        ps.setString(3, file_path_);
        ps.setString(4, file_name_);
        ps.execute();
        conn.commit();
        if (i == list.size() - 1) {
          dataTime = Integer.valueOf((String)dataMap.get("DATA_TIME_"));
        }
      }
      log.info(threadName + "tableName:" + tableName + "-----------更新STATUS_LD_FILE表状态失败，再次更新该表状态------------");
      loadResult.setDetail(getErrorMsg(e.getMessage()));
      e.printStackTrace();
      try
      {
        if (ps != null)
          ps.close();
      }
      catch (Exception x) {
        x.printStackTrace();
      }
    }
    catch (Throwable e1)
    {
      tableRecords = Long.valueOf(0L);
      String sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set GBASE_LOAD_STATUS_=?,GBASE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
      ps = conn.prepareStatement(sql4sqlSTATUS_LD_FILE);
      java.util.Date d = new java.util.Date();
      java.sql.Date date = new java.sql.Date(d.getTime());
      for (int i = 0; i < list.size(); i++) {
        Map dataMap = (Map)list.get(i);
        String file_path_ = (String)dataMap.get("FILE_PATH_");
        String file_name_ = (String)dataMap.get("FILE_NAME_");
        String file_record_num_ = (String)dataMap.get("FILE_RECORD_NUM_");
        tableRecords = Long.valueOf(tableRecords.longValue() + Long.valueOf(file_record_num_).longValue());
        ps.setString(1, loadResult.isSuccess() ? "1" : "0");
        ps.setString(2, this.dateformatter.format(date));
        ps.setString(3, file_path_);
        ps.setString(4, file_name_);
        ps.execute();
        conn.commit();
        if (i == list.size() - 1) {
          dataTime = Integer.valueOf((String)dataMap.get("DATA_TIME_"));
        }
      }
      log.info(threadName + "tableName:" + tableName + "-----------更新STATUS_LD_FILE表状态失败，再次更新该表状态------------");
      loadResult.setDetail(getErrorMsg(e1.getMessage()));
      e1.printStackTrace();
      try
      {
        if (ps != null)
          ps.close();
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
    finally
    {
      try
      {
        if (ps != null)
          ps.close();
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
    updateStatusTable(session, udfParams, tableName, tableRecords, dataTime, loadResult, threadName);
  }

  private void updateStatusTable(Session session, Map udfParams, String tableName, Long tableRecords, Integer dataTime, LoaderResult lr, String threadName)
    throws SQLException
  {
    log.info(threadName + "start update STATUS_TABLE ,loadState is  " + lr.isSuccess() + "!!!");
    log.info(threadName + "tableName:" + tableName);
    log.info(threadName + StringService.stringifyObject(lr));
    Connection conn = null;

    PreparedStatement selectPs = null;
    PreparedStatement ps = null;
    PreparedStatement insertPs = null;
    ResultSet rs = null;
    String desc = "";
    if ((!lr.isSuccess()) && (lr.getErrorLineCount() > 0L)) {
      desc = "Hive[" + tableName + "]表此次加载到GBase数据库共成功[" + 
        lr.getSuccessLineCount() + "]行,失败[" + 
        lr.getErrorLineCount() + "]行.\n";
      Object obj = udfParams.get("allowErrorPercent");
      Double percent = Double.valueOf(0.0D);
      Long allowNumber = Long.valueOf(0L);
      if (obj != null) {
        percent = Double.valueOf(Double.parseDouble(obj.toString()));
        allowNumber = Long.valueOf(Double.valueOf(percent.doubleValue() * tableRecords.longValue()).longValue());
      }
      if (lr.getErrorLineCount() > allowNumber.longValue()) {
        desc = desc + "加载错误行数[" + lr.getErrorLineCount() + "]超过允许最大错误行数[" + 
          allowNumber + "]\n";
      }
      log.info(threadName + desc);
      writeLocalLog(threadName + desc);
    }

    conn = session.connection();
    conn.setAutoCommit(false);
    DataTimeUtil dtu = new DataTimeUtil();
    String newDate = dtu.dealDate(EnvironmentImpl.getCurrent(), "", tableName, dataTime+"", "yyyyMMdd");
    try {
      String sqlSelect = "select  GBASE_LOAD_STATUS_ from STATUS_TABLE  where TABLE_NAME_=? and DATA_TIME_ =?";
      selectPs = conn.prepareStatement(sqlSelect);
      selectPs.setString(1, tableName);
      selectPs.setInt(2, Integer.parseInt(newDate));
      rs = selectPs.executeQuery();
      String gbase_load_status_ = null;
      try {
        if ((rs != null) && (rs.next()))
          gbase_load_status_ = rs.getString(1);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      String sqlUpdate = "update STATUS_TABLE set SKIPPED_NUM=SKIPPED_NUM+?,DATA_RECORD_NUM_=DATA_RECORD_NUM_+?,DESC_=?,GBASE_LOAD_STATUS_=? where TABLE_NAME_=? and DATA_TIME_ =?";
      ps = conn.prepareStatement(sqlUpdate);
      ps.setLong(1, lr.getErrorLineCount());
      ps.setLong(2, tableRecords.longValue());
      ps.setString(3, desc + StringUtils.trimToEmpty(lr.getDetail()));
      if (!lr.isSuccess())
        ps.setString(4, "0");
      else {
        ps.setString(4, gbase_load_status_);
      }
      ps.setString(5, tableName);
      ps.setInt(6, Integer.parseInt(newDate));
      int num = ps.executeUpdate();
      log.info(threadName + "更新STATUS_TABLE表,SKIPPED_NUM=" + lr.getErrorLineCount() + ",DATA_RECORD_NUM_=" + tableRecords + ",GBASE_LOAD_STATUS_=" + lr.isSuccess() + ",DATA_TIME_=" + newDate + ".更新记录条数:" + num);
      if (num == 0) {
        String insertSql = "insert into STATUS_TABLE (TABLE_NAME_,DATA_TIME_,DATA_RECORD_NUM_,SKIPPED_NUM,DESC_,GBASE_LOAD_STATUS_) values(?,?,?,?,?,?)";
        insertPs = conn.prepareStatement(insertSql);
        insertPs.setString(1, tableName);
        insertPs.setInt(2, Integer.parseInt(newDate));
        insertPs.setLong(3, tableRecords.longValue());
        insertPs.setLong(4, lr.getErrorLineCount());
        insertPs.setString(5, desc + StringUtils.trimToEmpty(lr.getDetail()));
        if (!lr.isSuccess())
          insertPs.setString(6, "0");
        else {
          insertPs.setString(6, gbase_load_status_);
        }
        insertPs.execute();
      }
      conn.commit();
    } catch (Exception exec) {
      String sqlSelect = "select  GBASE_LOAD_STATUS_ from STATUS_TABLE  where TABLE_NAME_=? and DATA_TIME_ =?";
      selectPs = conn.prepareStatement(sqlSelect);
      selectPs.setString(1, tableName);
      selectPs.setInt(2, Integer.parseInt(newDate));
      rs = selectPs.executeQuery();
      String gbase_load_status_ = null;
      try {
        if ((rs != null) && (rs.next()))
          gbase_load_status_ = rs.getString(1);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      String sqlUpdate = "update STATUS_TABLE set SKIPPED_NUM=SKIPPED_NUM+?,DATA_RECORD_NUM_=DATA_RECORD_NUM_+?,DESC_=?,GBASE_LOAD_STATUS_=? where TABLE_NAME_=? and DATA_TIME_ =?";
      ps = conn.prepareStatement(sqlUpdate);
      ps.setLong(1, lr.getErrorLineCount());
      ps.setLong(2, tableRecords.longValue());
      ps.setString(3, desc + StringUtils.trimToEmpty(lr.getDetail()));
      if (!lr.isSuccess())
        ps.setString(4, "0");
      else {
        ps.setString(4, gbase_load_status_);
      }
      ps.setString(5, tableName);
      ps.setInt(6, Integer.parseInt(newDate));
      int num = ps.executeUpdate();
      log.info(threadName + "更新STATUS_TABLE表,SKIPPED_NUM=" + lr.getErrorLineCount() + ",DATA_RECORD_NUM_=" + tableRecords + ",GBASE_LOAD_STATUS_=" + lr.isSuccess() + ",DATA_TIME_=" + newDate + ".更新记录条数:" + num);
      if (num == 0) {
        String insertSql = "insert into STATUS_TABLE (TABLE_NAME_,DATA_TIME_,DATA_RECORD_NUM_,SKIPPED_NUM,DESC_,GBASE_LOAD_STATUS_) values(?,?,?,?,?,?)";
        insertPs = conn.prepareStatement(insertSql);
        insertPs.setString(1, tableName);
        insertPs.setInt(2, Integer.parseInt(newDate));
        insertPs.setLong(3, tableRecords.longValue());
        insertPs.setLong(4, lr.getErrorLineCount());
        insertPs.setString(5, desc + StringUtils.trimToEmpty(lr.getDetail()));
        if (!lr.isSuccess())
          insertPs.setString(6, "0");
        else {
          insertPs.setString(6, gbase_load_status_);
        }
        insertPs.execute();
      }
      conn.commit();
      log.info(threadName + "tableName:" + tableName + "-----------更新STATUS_TABLE表状态失败------------");
      exec.printStackTrace();
      try
      {
        if (ps != null) {
          ps.close();
        }
        if (insertPs != null)
          insertPs.close();
      }
      catch (SQLException e) {
        e.printStackTrace();
      }
    }
    catch (Throwable exec1)
    {
      String sqlSelect = "select  GBASE_LOAD_STATUS_ from STATUS_TABLE  where TABLE_NAME_=? and DATA_TIME_ =?";
      selectPs = conn.prepareStatement(sqlSelect);
      selectPs.setString(1, tableName);
      selectPs.setInt(2, Integer.parseInt(newDate));
      rs = selectPs.executeQuery();
      String gbase_load_status_ = null;
      try {
        if ((rs != null) && (rs.next()))
          gbase_load_status_ = rs.getString(1);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      String sqlUpdate = "update STATUS_TABLE set SKIPPED_NUM=SKIPPED_NUM+?,DATA_RECORD_NUM_=DATA_RECORD_NUM_+?,DESC_=?,GBASE_LOAD_STATUS_=? where TABLE_NAME_=? and DATA_TIME_ =?";
      ps = conn.prepareStatement(sqlUpdate);
      ps.setLong(1, lr.getErrorLineCount());
      ps.setLong(2, tableRecords.longValue());
      ps.setString(3, desc + StringUtils.trimToEmpty(lr.getDetail()));
      if (!lr.isSuccess())
        ps.setString(4, "0");
      else {
        ps.setString(4, gbase_load_status_);
      }
      ps.setString(5, tableName);
      ps.setInt(6, Integer.parseInt(newDate));
      int num = ps.executeUpdate();
      log.info(threadName + "更新STATUS_TABLE表,SKIPPED_NUM=" + lr.getErrorLineCount() + ",DATA_RECORD_NUM_=" + tableRecords + ",GBASE_LOAD_STATUS_=" + lr.isSuccess() + ",DATA_TIME_=" + newDate + ".更新记录条数:" + num);
      if (num == 0) {
        String insertSql = "insert into STATUS_TABLE (TABLE_NAME_,DATA_TIME_,DATA_RECORD_NUM_,SKIPPED_NUM,DESC_,GBASE_LOAD_STATUS_) values(?,?,?,?,?,?)";
        insertPs = conn.prepareStatement(insertSql);
        insertPs.setString(1, tableName);
        insertPs.setInt(2, Integer.parseInt(newDate));
        insertPs.setLong(3, tableRecords.longValue());
        insertPs.setLong(4, lr.getErrorLineCount());
        insertPs.setString(5, desc + StringUtils.trimToEmpty(lr.getDetail()));
        if (!lr.isSuccess())
          insertPs.setString(6, "0");
        else {
          insertPs.setString(6, gbase_load_status_);
        }
        insertPs.execute();
      }
      conn.commit();
      log.info(threadName + "tableName:" + tableName + "-----------更新STATUS_TABLE表状态失败------------");
      exec1.printStackTrace();
      try
      {
        if (ps != null) {
          ps.close();
        }
        if (insertPs != null)
          insertPs.close();
      }
      catch (SQLException e) {
        e.printStackTrace();
      }
    }
    finally
    {
      try
      {
        if (ps != null) {
          ps.close();
        }
        if (insertPs != null)
          insertPs.close();
      }
      catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private String getErrorMsg(String errMsg) {
    Integer maxMsgLength = Integer.valueOf(1800);
    if ((StringUtils.trimToNull(errMsg) != null) && 
      (errMsg.length() > maxMsgLength.intValue())) {
      errMsg = errMsg.substring(0, maxMsgLength.intValue()) + "... " + (
        errMsg.length() - maxMsgLength.intValue()) + " more";
    }
    return errMsg;
  }

  private LoaderResult logAnalyze(Session session, Map platParam, String threadName) throws Exception
  {
    log.info(threadName + "开始进行日志分析...");
    writeLocalLog(threadName + "开始进行日志分析...");
    LoaderResult loadResult = new LoaderResult();
    String logStr = null;
    StringBuffer stringBuffer = null;
    File file = new File((String)platParam.get(CommonConf.dbLogPath));
    if (file.exists()) {
      stringBuffer = new StringBuffer();

      BufferedReader bufferedReader = null;
      String temp = "";
      try
      {
        bufferedReader = new BufferedReader(
          new InputStreamReader(new FileInputStream(file), "utf-8"));
        while ((temp = bufferedReader.readLine()) != null) {
          stringBuffer.append(temp + "\n");
        }
        logStr = stringBuffer.toString();
      } 
      catch (FileNotFoundException e) 
      {
        e.printStackTrace();
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
      finally
      {
        try
        {
          bufferedReader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    if (!StringUtils.isEmpty(logStr)) {
      long skippedNum = 0L;

      if (logStr.indexOf("ERROR:") > -1) {
        loadResult.setSuccess(false);
        loadResult
          .setDetail(logStr.substring(logStr.indexOf("ERROR:")));
        log.error(loadResult.getDetail());
        writeLocalLog(loadResult.getDetail());
      } else {
        int skipped_start = logStr.lastIndexOf("skipped") + 
          "skipped".length();
        int skipped_end = logStr.lastIndexOf("records");
        skippedNum = Long.parseLong(logStr.substring(skipped_start, 
          skipped_end).trim());
        log.info(threadName + "skippedNum===" + skippedNum);
        writeLocalLog(threadName + "skippedNum===" + skippedNum);
        int loaded_start = logStr.lastIndexOf("loaded") + 
          "loaded".length();
        int loaded_end = logStr.lastIndexOf("records", 
          logStr.lastIndexOf("records") - "records".length());
        int loadedNum = Integer.parseInt(logStr.substring(loaded_start, 
          loaded_end).trim());
        log.info(threadName + "loadedNum===" + loadedNum);
        writeLocalLog(threadName + "loadedNum===" + loadedNum);
        loadResult.setSuccessLineCount(loadedNum);
        loadResult.setErrorLineCount(skippedNum);
      }
    }
    log.info(threadName + "日志分析结束");
    writeLocalLog(threadName + "日志分析结束");
    return loadResult;
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
        errMsg = "execute " + getClass().getName() + 
          " activity error." + 
          AICloudETLExceptionUtil.getErrMsg(e);
      }
      this.localLogger.error(errMsg, e);
    }
  }

  private void writeThrowableLog(Throwable e, String errMsg) {
    if (this.singleLog) {
      if (StringUtils.isBlank(errMsg)) {
        errMsg = "execute " + getClass().getName() + 
          " activity error." + 
          AICloudETLExceptionUtil.getErrMsg(e);
      }
      this.localLogger.error(errMsg, e);
    }
  }

  public void releaseResource(HisActivity hisAct)
  {
  }

  public JobResult getState()
  {
    return this.result;
  }

  class FileCopyThread
    implements Runnable
  {
    private Map<String, String> shellParams = new HashMap();
    private boolean isFinish = false;
    private EnvironmentImpl env = null;
    private String threadName = null;

    public FileCopyThread(Map<String, String> shellParams,EnvironmentImpl  env, String parentThreadName,String threadName)
    {
      this.shellParams = shellParams;
      this.env = env;
      this.threadName = (parentThreadName + "-[拷贝线程#" + threadName + "]");
    }

    public void run()
    {
      GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "被调度到开始执行...");
      EnvironmentImpl.setCurrent(this.env);
      try {
        new ShellBehaver.RmtShell().execute(EnvironmentImpl.getCurrent(), 
          this.shellParams);
      } catch (Exception e) {
        GbaseLoadMutilThreadNoMR.log.error(e);
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, e.getMessage());
      }
      this.isFinish = true;
    }
  }

  class TableLoadThread implements Runnable
  {
    private String threadName = null;
    private Session session = null;
    private List<Map<String, String>> list = null;
    private Map<String, String> udfParams = new HashMap<String,String>();
    private Map<String, String> shellParams = new HashMap<String,String>();
    Map<String, String[]> tableCfgs = null;
    private LoaderResult loadResult = null;
    private boolean isFinish = false;
    private EnvironmentImpl env = null;
    private ThreadPoolExecutor copyThreadPool = null;

    TableLoadThread(Session session, List<Map<String, String>> list,
			Map<String, String> udfParams, Map<String, String> shellParams,
			Map<String, String[]> tableCfgs, String threadName,
			EnvironmentImpl env){
      this.session = session;
      this.list = list;
      this.udfParams.putAll(udfParams);
      this.tableCfgs = tableCfgs;
      this.shellParams.putAll(shellParams);
      this.threadName = "[load线程#" + threadName + "]";
      this.env = env;
    }

    public void run()
    {
      GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "被调度到开始执行...");
      EnvironmentImpl.setCurrent(this.env);
      try {
        GbaseLoad();
      } catch (Exception e) {
        this.loadResult = new LoaderResult();
        this.loadResult.setSuccess(false);
        this.loadResult.setDetail(e.getMessage());
      }
      this.isFinish = true;
    }

    private void GbaseLoad()
      throws SQLException
    {
      LoaderResult loadResult = new LoaderResult();
      Map dataMap = (Map)this.list.get(0);
      String TABLE_NAME_ = (String)dataMap.get("TABLE_NAME_");
      String DATA_TIME_ = (String)dataMap.get("DATA_TIME_");
      String LOAD_ACTION_ = (String)dataMap.get("LOAD_ACTION_");

      DataTimeUtil dtu = new DataTimeUtil();
      DATA_TIME_ = dtu.dealDate(EnvironmentImpl.getCurrent(), "", TABLE_NAME_, DATA_TIME_, "yyyyMMdd");
      String gbaseTableName = GbaseLoadMutilThreadNoMR.this.getGbaseTableName(TABLE_NAME_, this.tableCfgs, DATA_TIME_, "yyyyMMdd");
      this.udfParams.put("gbaseTableName", gbaseTableName);

      String[] cfgData = (String[])this.tableCfgs.get(TABLE_NAME_.toLowerCase());
      String allowErrorPercent = cfgData == null ? null : cfgData[2];
      if (allowErrorPercent != null) {
        this.udfParams.put("allowErrorPercent", allowErrorPercent);
      }
      String extraLoaderArgs = cfgData == null ? null : cfgData[3];
      if (extraLoaderArgs != null) {
        this.udfParams.put("EXTRA_LOADER_ARGS_", extraLoaderArgs);
      }

      if (LOAD_ACTION_ != null && "0".equals(LOAD_ACTION_.toString())) {
    	java.sql.Connection  conn = null;
        Statement stmt = null;
        try {
          String deleteKey = null;
          String[] cfgs = (String[])this.tableCfgs.get(TABLE_NAME_);
          if ((cfgs != null) && (cfgs.length >= 2)) {
            GbaseLoadMutilThreadNoMR.log.info("GBase清理工作参数个数====" + cfgs.length);
            deleteKey = cfgs[1];
          }
          Class.forName("com.gbase.jdbc.Driver");
          String username = (String)this.udfParams.get("db_user");
          String pwd = (String)this.udfParams.get("db_password");
          String url = (String)this.udfParams.get("gbaseurl");
          GbaseLoadMutilThreadNoMR.log.info("GbaseDB URL is " + url);
          GbaseLoadMutilThreadNoMR.this.writeLocalLog("GbaseDB URL is " + url);
          conn = DriverManager.getConnection(url, username, pwd);
          stmt = conn.createStatement();
          conn.setAutoCommit(false);

          String sql = null;
          if (StringUtils.trimToNull(deleteKey) != null)
            sql = "delete from " + gbaseTableName + " where " + deleteKey + " = " + DATA_TIME_;
          else {
            sql = "truncate table " + gbaseTableName;
          }
          GbaseLoadMutilThreadNoMR.log.info(this.threadName + "覆盖式加载,删除数据sql:[" + sql + "]");
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "覆盖式加载,删除数据sql:[" + sql + "]");
          stmt.execute(sql);
          conn.commit();
        } catch (Exception e) {
          loadResult.setSuccess(false);
          loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(e.getMessage()));
          GbaseLoadMutilThreadNoMR.log.error(e.getMessage(), e);
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, null);
          try
          {
            if (stmt != null) {
              stmt.close();
            }
            if (conn != null)
              conn.close();
          }
          catch (Exception x) {
            x.printStackTrace();
          }
        }
        finally
        {
          try
          {
            if (stmt != null) {
              stmt.close();
            }
            if (conn != null)
              conn.close();
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        }

      }

      if ((LOAD_ACTION_ != null) && (("0".equals(LOAD_ACTION_.toString())) || ("1".equals(LOAD_ACTION_.toString())))) {
        GbaseLoadMutilThreadNoMR.this.start_updateTable(this.session, this.list, this.threadName);
        try {
          if ("link".equals(GbaseLoadMutilThreadNoMR.this.loadType)) {
        	
            loadResult = loadByLink();
          } 
//-----------------------------------------------------------------------------         
          else if ("jdbc".equals(GbaseLoadMutilThreadNoMR.this.loadType)){
        	loadResult = loadByJDBC();  
          }
//-----------------------------------------------------------------------------         
          else {
            this.copyThreadPool = 
              new ThreadPoolExecutor(GbaseLoadMutilThreadNoMR.this.copyThreadNumer, 2147483647, 1L, 
              TimeUnit.SECONDS, 
              new LinkedBlockingQueue());
            try {
              loadResult = loadByCopyToLocal();
            } finally {
              if ((this.copyThreadPool != null) && (!this.copyThreadPool.isShutdown()))
                this.copyThreadPool.shutdown();
            }
          }
        }
        catch (Exception e)
        {
          loadResult.setSuccess(false);
          loadResult.setDetail(e.getMessage());
        }
      }
      GbaseLoadMutilThreadNoMR.this.end_updateTable(this.session, this.udfParams, this.list, loadResult, TABLE_NAME_, this.threadName);
    }

    private LoaderResult loadByLink() {
      LoaderResult loadResult = new LoaderResult();
      GbaseLoadMutilThreadNoMR.log.info(this.threadName + "==========loadResult====0======" + StringService.stringifyObject(loadResult));
      Configuration conf = new Configuration();
      FileSystem fs = null;
      String loadTempPath = "";
      try {
        fs = FileSystem.get(conf);
        String fuseHome = (String)this.udfParams.get("fusehome");
        String GbaseRootCmd = (String)this.udfParams.get("dbCmd");

        String disp_server = (String)this.udfParams.get("disp_server");
        String format = (String)this.udfParams.get("format");
        String db_user = (String)this.udfParams.get("db_user");
        String db_name = (String)this.udfParams.get("db_name");
        String string_qualifier = (String)this.udfParams.get("string_qualifier");
        String hash_parallel = (String)this.udfParams.get("hash_parallel");
        String send_block_size = (String)this.udfParams.get("send_block_size");
        String extra_loader_args = (String)this.udfParams.get("extra_loader_args");
        String delimiter = (String)this.udfParams.get("separate");
        String socket = (String)this.udfParams.get("socket");
        String gbaseTableName = (String)this.udfParams.get("gbaseTableName");
        String localFilePath = (String)this.udfParams.get("localFilePath");
        String using_direct_io = (String)this.udfParams.get("using_direct_io");

        String tableExtraLoaderArgs = 
          (String)this.udfParams
          .get("EXTRA_LOADER_ARGS_");

        Long tableRecords = Long.valueOf(0L);
        StringBuffer sbf = new StringBuffer();

        StringBuffer shellCmds = new StringBuffer();
        Map pathMap = new HashMap();
        for (int i = 0; i < this.list.size(); i++) {
          Map map = (Map)this.list.get(i);
          String file_name_ = (String)map.get("FILE_NAME_");
          String file_path_ = (String)map.get("FILE_PATH_");
          String file_record_num_ = (String)map.get("FILE_RECORD_NUM_") == null ? "0" : (String)map.get("FILE_RECORD_NUM_");
          String tablename = (String)map.get("TABLE_NAME_");
          String dataTime = (String)map.get("DATA_TIME_");
          tableRecords = Long.valueOf(tableRecords.longValue() + Long.valueOf(file_record_num_).longValue());
          String link = localFilePath + "/" + tablename + "_" + 
            dataTime + "/" + file_name_;

          if (pathMap.get(file_path_) == null) {
            pathMap.put(file_path_, localFilePath + "/" + tablename + 
              "_" + dataTime + "/");
            shellCmds.append("ln -sf " + fuseHome + "/" + 
              file_path_ + "/* " + (String)pathMap.get(file_path_) + 
              ";");
          }
          if (i == this.list.size() - 1) {
            loadTempPath = localFilePath + "/" + tablename + "_" + 
              dataTime;
            sbf.append(link);
          } else {
            sbf.append(link).append(",");
          }
        }
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "开始创建软连接...");
        pathMap = null;

        this.shellParams.put("cmd", "mkdir -p " + loadTempPath + " ;" + 
          shellCmds.toString());
        try {
          new ShellBehaver.RmtShell().execute(EnvironmentImpl.getCurrent(), 
            this.shellParams);
        } catch (Exception e) {
          GbaseLoadMutilThreadNoMR.this.updateLoadStatus(this.session, this.list, this.threadName);
          GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
          GbaseLoadMutilThreadNoMR.log.error(e);
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, e.getMessage());
          loadResult.setSuccess(false);
          loadResult.setDetail(e.getMessage());
//          localLoaderResult1 = loadResult;

          GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "执行软连接清理工作...");

          this.shellParams.put("cmd", "rm -rf " + loadTempPath + ";");
          try {
            new ShellBehaver.RmtShell().execute(EnvironmentImpl.getCurrent(), 
              this.shellParams);
          } catch (Exception ex) {
            GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
            GbaseLoadMutilThreadNoMR.log.error(ex);
            GbaseLoadMutilThreadNoMR.this.writeLocalLog(ex, null);
            loadResult.setSuccess(false);
            loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(ex.getMessage()));
          }
          GbaseLoadMutilThreadNoMR.log.info(this.threadName + "==========loadResult====5======" + StringService.stringifyObject(loadResult));
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "软连接清理完毕.");

          return loadResult;
//          return localLoaderResult1;
        } catch (Throwable e1) {
          GbaseLoadMutilThreadNoMR.this.updateLoadStatus(this.session, this.list, this.threadName);
          GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
          GbaseLoadMutilThreadNoMR.log.error(e1);
          GbaseLoadMutilThreadNoMR.this.writeThrowableLog(e1, e1.getMessage());
          loadResult.setSuccess(false);
          loadResult.setDetail(e1.getMessage());
          LoaderResult localLoaderResult1 = loadResult;

          GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "执行软连接清理工作...");

          this.shellParams.put("cmd", "rm -rf " + loadTempPath + ";");
          try {
            new ShellBehaver.RmtShell().execute(EnvironmentImpl.getCurrent(), 
              this.shellParams);
          } catch (Exception e) {
            GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
            GbaseLoadMutilThreadNoMR.log.error(e);
            GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, null);
            loadResult.setSuccess(false);
            loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(e.getMessage()));
          }
          GbaseLoadMutilThreadNoMR.log.info(this.threadName + "==========loadResult====5======" + StringService.stringifyObject(loadResult));
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "软连接清理完毕.");

          return localLoaderResult1;
        }

        GbaseLoadMutilThreadNoMR.log.info(this.threadName + "==========loadResult====1======" + StringService.stringifyObject(loadResult));
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "开始拼装ctl文件：gbasetable=" + 
          gbaseTableName);
        String file_list = sbf.toString();

        StringBuffer ctrlBuffer = new StringBuffer();
        if (StringUtils.isNotBlank(gbaseTableName)) {
          ctrlBuffer.append("[" + gbaseTableName + "]")
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(disp_server)) {
          ctrlBuffer.append("disp_server=").append(disp_server)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(file_list)) {
          ctrlBuffer.append("file_list=").append(file_list)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(format)) {
          ctrlBuffer.append("format=").append(format).append("\r\n");
        }
        if (StringUtils.isNotBlank(db_user)) {
          ctrlBuffer.append("db_user=").append(db_user)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(db_name)) {
          ctrlBuffer.append("db_name=").append(db_name)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(gbaseTableName)) {
          ctrlBuffer.append("table_name=").append(gbaseTableName)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(tableExtraLoaderArgs)) {
          ctrlBuffer.append("extra_loader_args=")
            .append(tableExtraLoaderArgs).append("\r\n");
        }
        else if (StringUtils.isNotBlank(extra_loader_args)) {
          ctrlBuffer.append("extra_loader_args=")
            .append(extra_loader_args).append("\r\n");
        }

        if (StringUtils.isNotBlank(delimiter)) {
          ctrlBuffer.append("delimiter='").append(delimiter)
            .append("'\r\n");
        }
        if (StringUtils.isNotBlank(socket)) {
          ctrlBuffer.append("socket=").append(socket).append("\r\n");
        }
        if (StringUtils.isNotBlank(string_qualifier)) {
          ctrlBuffer.append("string_qualifier=")
            .append(string_qualifier).append("\r\n");
        }
        if (StringUtils.isNotBlank(hash_parallel)) {
          ctrlBuffer.append("hash_parallel=").append(hash_parallel)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(send_block_size)) {
          ctrlBuffer.append("send_block_size=")
            .append(send_block_size).append("\r\n");
        }
        if (StringUtils.isNotBlank(using_direct_io)) {
          ctrlBuffer.append("using_direct_io=")
            .append(using_direct_io).append("\r\n");
        }

        ctrlBuffer.append("gcluster_data_consistent_level=0").append("\r\n");

        String ctrlString = ctrlBuffer.toString();
        this.udfParams.put(ConstCommonParams.ctrl, ctrlString);

        String dateDirString = DateFormatUtils.format(new java.util.Date(), "yyyyMMdd");
        String fileNameUU = UUID.randomUUID().toString();
        String fileName = dateDirString + "/" + fileNameUU;
        String logMkdir = "/home/ocdc/app/logPath/" + dateDirString;
        try
        {
          File myFilePath = new File(logMkdir);
          if (!myFilePath.exists())
            myFilePath.mkdir();
        }
        catch (Exception e)
        {
          System.out.println("新建目录操作出错");
          e.printStackTrace();
        }

        String logStr = "/home/ocdc/app/logPath/" + fileName + ".log";
        File filePathLocal = new File(logStr);
        if (!filePathLocal.exists())
        {
          try
          {
            filePathLocal.createNewFile();
          }
          catch (Exception e)
          {
            GbaseLoadMutilThreadNoMR.log.info("文件不存在，创建失败！" + logStr);
          }

        }

        String scriptPath = PathUtil.getInstance().getPath(fs, 
          PathUtil.PATH_TYPE.DBCTRLPATH, conf);
        VelocityUtil.getInstance().evaluate(fs, this.udfParams, ctrlString, 
          scriptPath);

        this.udfParams.put(CommonConf.ctrlFile, fuseHome + scriptPath);

        this.udfParams.put(CommonConf.dbLogPath, logStr);
        String cmdStr = VelocityUtil.getInstance().evaluate(this.udfParams, 
          GbaseRootCmd);
        this.udfParams.put("finishedcmdStr", cmdStr);

        String cmdPath = PathUtil.getInstance().getPath(fs, 
          PathUtil.PATH_TYPE.DBCMDPATH, conf);
        VelocityUtil.getInstance().evaluate(fs, this.udfParams, 
          "${finishedcmdStr}", cmdPath);
        GbaseLoadMutilThreadNoMR.log.info(this.threadName + "cmdPath===" + cmdPath);
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "cmdPath===" + cmdPath);
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "开始加载gbase数据...");
        ShellUtil shellUtil = new ShellUtil();
        shellUtil.procShell(fs, cmdPath, null);

        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "gbase数据加载完毕.");
        try {
          loadResult = GbaseLoadMutilThreadNoMR.this.logAnalyze(this.session, this.udfParams, this.threadName);
          GbaseLoadMutilThreadNoMR.log.info(this.threadName + "==========loadResult====2======" + StringService.stringifyObject(loadResult));
        } catch (Exception e) {
          loadResult.setSuccess(false);
          loadResult.setDetail(e.getMessage());
        }
        GbaseLoadMutilThreadNoMR.log.info(this.threadName + "==========loadResult====3======" + StringService.stringifyObject(loadResult));
        Object obj = this.udfParams.get("allowErrorPercent");
        Double percent = Double.valueOf(0.0D);
        Long allowNumber = Long.valueOf(0L);
        if (obj != null) {
          percent = Double.valueOf(Double.parseDouble(obj.toString()));
          allowNumber = 
            Long.valueOf(Double.valueOf(percent.doubleValue() * tableRecords.longValue())
            .longValue());
        }
        if (loadResult.getErrorLineCount() > allowNumber.longValue()) {
          GbaseLoadMutilThreadNoMR.this.writeLocalLog("===============" + allowNumber);
          loadResult.setSuccess(false);
        }
        GbaseLoadMutilThreadNoMR.log.info(this.threadName + "loadResult:" + loadResult.isSuccess());
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "loadResult:" + loadResult.isSuccess());
        GbaseLoadMutilThreadNoMR.log.info(this.threadName + "==========loadResult====4======" + StringService.stringifyObject(loadResult));
      }
      catch (Exception e) {
        GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
        GbaseLoadMutilThreadNoMR.log.error(e);
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, null);
        loadResult.setSuccess(false);
        loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(e.getMessage()));
      } catch (Throwable e1) {
        GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
        GbaseLoadMutilThreadNoMR.log.error(e1);
        GbaseLoadMutilThreadNoMR.this.writeThrowableLog(e1, null);
        loadResult.setSuccess(false);
        loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(e1.getMessage()));
      } finally {
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "执行软连接清理工作...");

        this.shellParams.put("cmd", "rm -rf " + loadTempPath + ";");
        try {
          new ShellBehaver.RmtShell().execute(EnvironmentImpl.getCurrent(), 
            this.shellParams);
        } catch (Exception e) {
          GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
          GbaseLoadMutilThreadNoMR.log.error(e);
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, null);
          loadResult.setSuccess(false);
          loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(e.getMessage()));
        }
        GbaseLoadMutilThreadNoMR.log.info(this.threadName + "==========loadResult====5======" + StringService.stringifyObject(loadResult));
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "软连接清理完毕.");
      }
      return loadResult;
    }

    private LoaderResult loadByCopyToLocal() {
      LoaderResult loadResult = new LoaderResult();
      Configuration conf = new Configuration();
      FileSystem fs = null;
      String loadTempPath = "";
      try {
        fs = FileSystem.get(conf);
        String fusehome = (String)this.udfParams.get("fusehome");
        String GbaseRootCmd = (String)this.udfParams.get("dbCmd");

        String disp_server = (String)this.udfParams.get("disp_server");
        String format = (String)this.udfParams.get("format");
        String db_user = (String)this.udfParams.get("db_user");
        String db_name = (String)this.udfParams.get("db_name");
        String string_qualifier = (String)this.udfParams.get("string_qualifier");
        String hash_parallel = (String)this.udfParams.get("hash_parallel");
        String send_block_size = (String)this.udfParams.get("send_block_size");
        String extra_loader_args = (String)this.udfParams.get("extra_loader_args");
        String delimiter = (String)this.udfParams.get("separate");
        String socket = (String)this.udfParams.get("socket");
        String gbaseTableName = (String)this.udfParams.get("gbaseTableName");
        String localFilePath = (String)this.udfParams.get("localFilePath");
        String using_direct_io = (String)this.udfParams.get("using_direct_io");

        String tableExtraLoaderArgs = 
          (String)this.udfParams
          .get("EXTRA_LOADER_ARGS_");

        List<FileCopyThread> fileCopyThreadList = new ArrayList<FileCopyThread>();

        Long tableRecords = Long.valueOf(0L);
        StringBuffer sbf = new StringBuffer();
        Map<String, String> shellMaps;
        String shellCmds;
        for (int i = 0; i < this.list.size(); i++) {
          shellMaps = new HashMap<String, String>();
          shellMaps.putAll(this.shellParams);
          Map map = (Map)this.list.get(i);
          String file_name_ = (String)map.get("FILE_NAME_");
          String file_path_ = (String)map.get("FILE_PATH_");
          String file_record_num_ = 
            (String)map
            .get("FILE_RECORD_NUM_");
          String tablename = (String)map.get("TABLE_NAME_");
          String dataTime = (String)map.get("DATA_TIME_");
          tableRecords = Long.valueOf(tableRecords.longValue() + Long.valueOf(file_record_num_).longValue());
          String link = localFilePath + "/" + tablename + "_" + 
            dataTime + "/" + file_name_;

          shellCmds = 
            new StringBuilder("hadoop fs -get ").append(file_path_).append("/")
            .append(file_name_).append(" ").append(link).append(";").toString();

          if (i == this.list.size() - 1) {
            loadTempPath = localFilePath + "/" + tablename + "_" + 
              dataTime;
            sbf.append(link);
          } else {
            sbf.append(link).append(",");
          }
          shellMaps.put("cmd", shellCmds);

          GbaseLoadMutilThreadNoMR.FileCopyThread fileCopyThread = new FileCopyThread(shellMaps, EnvironmentImpl.getCurrent(),
					this.threadName, "" + i);
//          GbaseLoadMutilThreadNoMR.FileCopyThread fileCopyThread = new GbaseLoadMutilThreadNoMR.FileCopyThread(GbaseLoadMutilThreadNoMR.this, 
//        		  shellMaps, EnvironmentImpl.getCurrent(), 
//        		  this.threadName, i);
          fileCopyThreadList.add(fileCopyThread);
        }

        this.shellParams.put("cmd", "mkdir -p " + loadTempPath + " ;");
        try {
          new ShellBehaver.RmtShell().execute(EnvironmentImpl.getCurrent(), 
            this.shellParams);
        } catch (Exception e) {
          GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
          GbaseLoadMutilThreadNoMR.log.error(e);
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, null);
          loadResult.setSuccess(false);
          loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(e.getMessage()));
          LoaderResult localLoaderResult1 = loadResult;

          GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "执行临时文件删除工作...");

          this.shellParams.put("cmd", "rm -rf " + loadTempPath + ";");
          try {
            new ShellBehaver.RmtShell().execute(EnvironmentImpl.getCurrent(), 
              this.shellParams);
          } catch (Exception ex) {
            GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
            GbaseLoadMutilThreadNoMR.log.error(ex);
            GbaseLoadMutilThreadNoMR.this.writeLocalLog(ex, null);
            loadResult.setSuccess(false);
            loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(ex.getMessage()));
          }
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "临时文件清理完毕.");

          return localLoaderResult1;
        }

        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "启动多线程异步执行文件拷贝...");
        if (fileCopyThreadList != null && fileCopyThreadList.size() > 0) {
          for (FileCopyThread fileCopyThread : fileCopyThreadList) {
            this.copyThreadPool.execute(fileCopyThread);
          }
        }

        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "开始拼装ctl文件：gbasetable=" + 
          gbaseTableName);
        String file_list = sbf.toString();

        StringBuffer ctrlBuffer = new StringBuffer();
        if (StringUtils.isNotBlank(gbaseTableName)) {
          ctrlBuffer.append("[" + gbaseTableName + "]")
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(disp_server)) {
          ctrlBuffer.append("disp_server=").append(disp_server)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(file_list)) {
          ctrlBuffer.append("file_list=").append(file_list)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(format)) {
          ctrlBuffer.append("format=").append(format).append("\r\n");
        }
        if (StringUtils.isNotBlank(db_user)) {
          ctrlBuffer.append("db_user=").append(db_user)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(db_name)) {
          ctrlBuffer.append("db_name=").append(db_name)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(gbaseTableName)) {
          ctrlBuffer.append("table_name=").append(gbaseTableName)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(tableExtraLoaderArgs)) {
          ctrlBuffer.append("extra_loader_args=")
            .append(tableExtraLoaderArgs).append("\r\n");
        }
        else if (StringUtils.isNotBlank(extra_loader_args)) {
          ctrlBuffer.append("extra_loader_args=")
            .append(extra_loader_args).append("\r\n");
        }

        if (StringUtils.isNotBlank(delimiter)) {
          ctrlBuffer.append("delimiter='").append(delimiter)
            .append("'\r\n");
        }
        if (StringUtils.isNotBlank(socket)) {
          ctrlBuffer.append("socket=").append(socket).append("\r\n");
        }
        if (StringUtils.isNotBlank(string_qualifier)) {
          ctrlBuffer.append("string_qualifier=")
            .append(string_qualifier).append("\r\n");
        }
        if (StringUtils.isNotBlank(hash_parallel)) {
          ctrlBuffer.append("hash_parallel=").append(hash_parallel)
            .append("\r\n");
        }
        if (StringUtils.isNotBlank(send_block_size)) {
          ctrlBuffer.append("send_block_size=")
            .append(send_block_size).append("\r\n");
        }
        if (StringUtils.isNotBlank(using_direct_io)) {
          ctrlBuffer.append("using_direct_io=")
            .append(using_direct_io).append("\r\n");
        }

        String ctrlString = ctrlBuffer.toString();
        this.udfParams.put(ConstCommonParams.ctrl, ctrlString);

        String logStr = PathUtil.getInstance().getPath(fs, 
          PathUtil.PATH_TYPE.DBLOGPATH, conf);

        String scriptPath = PathUtil.getInstance().getPath(fs, 
          PathUtil.PATH_TYPE.DBCTRLPATH, conf);
        VelocityUtil.getInstance().evaluate(fs, this.udfParams, ctrlString, 
          scriptPath);

        this.udfParams.put(CommonConf.ctrlFile, fusehome + scriptPath);
        this.udfParams.put(CommonConf.dbLogPath, fusehome + logStr);
        String cmdStr = VelocityUtil.getInstance().evaluate(this.udfParams, 
          GbaseRootCmd);
        this.udfParams.put("finishedcmdStr", cmdStr);

        String cmdPath = PathUtil.getInstance().getPath(fs, 
          PathUtil.PATH_TYPE.DBCMDPATH, conf);
        VelocityUtil.getInstance().evaluate(fs, this.udfParams, 
          "${finishedcmdStr}", cmdPath);
        GbaseLoadMutilThreadNoMR.log.info(this.threadName + "cmdPath===" + cmdPath);
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "cmdPath===" + cmdPath);

        if ((fileCopyThreadList != null) && (fileCopyThreadList.size() > 0)) {
          while (true)
          {
            boolean allDone = true;
            for (GbaseLoadMutilThreadNoMR.FileCopyThread fileCopyThread : fileCopyThreadList) {
              if (!fileCopyThread.isFinish) {
                allDone = false;
                break;
              }
            }
            if (allDone) {
              break;
            }
            Thread.sleep(1000L);
          }
        }

        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "开始加载gbase数据...");
        ShellUtil shellUtil = new ShellUtil();
        shellUtil.procShell(fs, cmdPath, null);
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "gbase数据加载完毕.");
        try {
          loadResult = GbaseLoadMutilThreadNoMR.this.logAnalyze(this.session, this.udfParams, this.threadName);
        } catch (Exception e) {
          loadResult.setSuccess(false);
          loadResult.setDetail(e.getMessage());
        }
        Object obj = this.udfParams.get("allowErrorPercent");
        Double percent = Double.valueOf(0.0D);
        Long allowNumber = Long.valueOf(0L);
        if (obj != null) {
          percent = Double.valueOf(Double.parseDouble(obj.toString()));
          allowNumber = 
            Long.valueOf(Double.valueOf(percent.doubleValue() * tableRecords.longValue())
            .longValue());
        }
        if (loadResult.getErrorLineCount() > allowNumber.longValue()) {
          loadResult.setSuccess(false);
        }
        GbaseLoadMutilThreadNoMR.log.info(this.threadName + "loadResult:" + loadResult.isSuccess());
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "loadResult:" + loadResult.isSuccess());
        GbaseLoadMutilThreadNoMR.log.warn(this.threadName + StringService.stringifyObject(loadResult));
      } catch (Exception e) {
        GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
        GbaseLoadMutilThreadNoMR.log.error(e);
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, null);
        loadResult.setSuccess(false);
        loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(e.getMessage()));
      } finally {
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "执行临时文件删除工作...");

        this.shellParams.put("cmd", "rm -rf " + loadTempPath + ";");
        try {
          new ShellBehaver.RmtShell().execute(EnvironmentImpl.getCurrent(), 
            this.shellParams);
        } catch (Exception e) {
          GbaseLoadMutilThreadNoMR.this.result = JobResult.ERROR;
          GbaseLoadMutilThreadNoMR.log.error(e);
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, null);
          loadResult.setSuccess(false);
          loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(e.getMessage()));
        }
        GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "临时文件清理完毕.");
      }
      return loadResult;
    }

    
//------------------------------------------------------------------------------
    private LoaderResult loadByJDBC() {
    	Map dataMap = (Map)this.list.get(0);
    	String TABLE_NAME_ = (String)dataMap.get("TABLE_NAME_");
        String DATA_TIME_ = (String)dataMap.get("DATA_TIME_");
        String FILE_PATH = (String)dataMap.get("FILE_PATH_");
    	DataTimeUtil dtu = new DataTimeUtil();
    	DATA_TIME_ = dtu.dealDate(EnvironmentImpl.getCurrent(), "", TABLE_NAME_, DATA_TIME_, "yyyyMMdd");
    	String gbaseTableName = GbaseLoadMutilThreadNoMR.this.getGbaseTableName(TABLE_NAME_, this.tableCfgs, DATA_TIME_, "yyyyMMdd");
        LoaderResult loadResult = new LoaderResult();
    	java.sql.Connection  conn = null;
        Statement stmt = null;
        try {
          Class.forName("com.gbase.jdbc.Driver");
          String username = (String)this.udfParams.get("db_user");
          String pwd = (String)this.udfParams.get("db_password");
          String url = (String)this.udfParams.get("gbaseurl");
          GbaseLoadMutilThreadNoMR.log.info("GbaseDB URL is " + url);
          GbaseLoadMutilThreadNoMR.this.writeLocalLog("GbaseDB URL is " + url);
          conn = DriverManager.getConnection(url, username, pwd);
          stmt = conn.createStatement();
//          conn.setAutoCommit(false);

          String sql = "load data infile 'hdp://hive@172.19.204.12:50070"+FILE_PATH+"*' into table "
          		+ gbaseTableName+" DATA_FORMAT 3 FIELDS TERMINATED BY '\t' DATETIME FORMAT '%Y-%m-%d %H:%i:%s' DATE FORMAT '%Y-%m-%d' NULL_VALUE '\\\\N'";

          GbaseLoadMutilThreadNoMR.log.info(this.threadName + "加载sql:[" + sql + "]");
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(this.threadName + "加载sql:[" + sql + "]");
          boolean flag=stmt.execute(sql);
//          conn.commit();
          if(flag)
        	  loadResult.setSuccess(true);
          else
        	  loadResult.setSuccess(false);
        } catch (Exception e) {
          loadResult.setSuccess(false);
          loadResult.setDetail(GbaseLoadMutilThreadNoMR.this.getErrorMsg(e.getMessage()));
          GbaseLoadMutilThreadNoMR.log.error(e.getMessage(), e);
          GbaseLoadMutilThreadNoMR.this.writeLocalLog(e, null);
          try
          {
            if (stmt != null) {
              stmt.close();
            }
            if (conn != null)
              conn.close();
          }
          catch (Exception x) {
            x.printStackTrace();
          }
        }
        finally
        {
          try
          {
            if (stmt != null) {
              stmt.close();
            }
            if (conn != null)
              conn.close();
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        }

      
        return loadResult;
      }
//------------------------------------------------------------------------------
    
    
    public LoaderResult getLoadResult() {
      return this.loadResult;
    }

    public String getThreadName() {
      return this.threadName;
    }
  }
}