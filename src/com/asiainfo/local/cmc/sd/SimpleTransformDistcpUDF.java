/*     */ package com.asiainfo.local.cmc.sd;
/*     */ 
/*     */ import com.ailk.cloudetl.commons.api.Environment;
/*     */ import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
/*     */ import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
/*     */ import com.ailk.cloudetl.ndataflow.api.HisActivity;
/*     */ import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
/*     */ import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
/*     */ import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
/*     */ import com.ailk.udf.node.shandong.DataTimeUtil;
/*     */ import com.ailk.udf.node.shandong.trans.FileTransUtil;
/*     */ import com.ailk.udf.node.shandong.util.FileDistCpUtil;
/*     */ import java.io.IOException;
/*     */ import java.sql.Connection;
/*     */ import java.sql.PreparedStatement;
/*     */ import java.sql.ResultSet;
/*     */ import java.sql.SQLException;
/*     */ import java.sql.Statement;
/*     */ import java.util.ArrayList;
/*     */ import java.util.HashMap;
/*     */ import java.util.Iterator;
/*     */ import java.util.List;
/*     */ import java.util.Map;
/*     */ import java.util.Map.Entry;
/*     */ import java.util.Set;
/*     */ import org.apache.commons.lang.StringUtils;
/*     */ import org.apache.commons.logging.Log;
/*     */ import org.apache.commons.logging.LogFactory;
/*     */ import org.apache.hadoop.conf.Configuration;
/*     */ import org.apache.hadoop.fs.FileSystem;
/*     */ import org.apache.hadoop.fs.Path;
/*     */ import org.apache.log4j.Logger;
/*     */ import org.hibernate.Session;
import org.hibernate.SessionFactory;
/*     */ 
/*     */ public class SimpleTransformDistcpUDF
/*     */   implements UDFActivity
/*     */ {
/*  49 */   private static final Log LOGGER = LogFactory.getLog(SimpleTransformDistcpUDF.class);
/*     */ 
/*  51 */   private final String DAY_OR_MONTH = "DAY_OR_MONTH";
/*  52 */   private final String TRANS_LOAD_DIR_OUT = "/asiainfo/ETL/trans/out/$TABLENAME/";
/*  53 */   private final String ERROR_MESSAGE = "ERROR_MESSAGE";
/*     */ 
/*  55 */   private JobResult result = JobResult.RIGHT;
/*     */ 
/*  57 */   private boolean singleLog = true;
/*  58 */   private Logger localLogger = null;
/*  59 */   private int numMapTask = -1;
/*  60 */   private String transLoadDirOut = null;
/*     */ 
/*     */   @SuppressWarnings("unchecked")
public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
/*     */   {
/*  66 */     initLocalLog(env, udfParams);
/*  67 */     writeLocalLog("开始执行节点" + getClass().getName());
/*  68 */     Map executeResult = new HashMap();
/*  69 */     String tmpTransLoadDirOut = (String)udfParams.get("transLoadDirOut");
/*  70 */     if (StringUtils.isNotBlank(tmpTransLoadDirOut))
/*  71 */       this.transLoadDirOut = tmpTransLoadDirOut;
/*     */     else {
/*  73 */       this.transLoadDirOut = "/asiainfo/ETL/trans/out/$TABLENAME/";
/*     */     }
/*     */ 
/*  76 */     String numMapTaskStr = (String)udfParams.get("numMapTask");
/*  77 */     if (StringUtils.isNotBlank(numMapTaskStr)) {
/*  78 */       this.numMapTask = Integer.parseInt(numMapTaskStr);
/*     */     }
/*  80 */     String dayOrMonth = (String)udfParams.get("DAY_OR_MONTH");
/*  81 */     int dayMonth = 0;
/*  82 */     if ((dayOrMonth != null) && (dayOrMonth.trim().length() > 0) && 
/*  83 */       (dayOrMonth.trim().toLowerCase().startsWith("d"))) {
/*  84 */       dayMonth = 1;
/*  85 */     } else if ((dayOrMonth != null) && (dayOrMonth.trim().length() > 0) && 
/*  86 */       (dayOrMonth.trim().toLowerCase().startsWith("m"))) {
/*  87 */       dayMonth = 2;
/*     */     } else {
/*  89 */       this.result = JobResult.ERROR;
/*  90 */       String errMsg = "Parameter [DAY_OR_MONTH]'s value must be one of [day, month], please set the right value.";
/*     */ 
/*  93 */       LOGGER.error(errMsg);
/*  94 */       writeLocalLog(errMsg);
/*  95 */       executeResult.put("ERROR", errMsg);
/*  96 */       return null;
/*     */     }
/*  98 */     Session session = null;
/*  99 */     Connection conn = null;
/* 100 */     PreparedStatement queryStmt = null;
/* 101 */     ResultSet queryRs = null;
/*     */     try {
/* 103 */       session = HibernateUtils.getSessionFactory().openSession();
/* 104 */       conn = session.connection();
/* 105 */       conn.setAutoCommit(true);
/* 106 */       String selectSql = "select cfg.IF_NO_, cfg.TABLE_NAME_, cfg.IS_PARTITION_, ett.FILE_NAME_, ett.FILE_PATH_, ett.DATA_TIME_ from (select et.IF_NO_, et.FILE_NAME_, et.FILE_PATH_, et.DATA_TIME_,si.CHK_STATUS_ from STATUS_ET_FILE et left join STATUS_INTERFACE si on et.IF_NO_=si.IF_NO_ and et.DATA_TIME_=si.DATA_TIME_ where et.STATUS_=1 and et.FILE_NAME_ not like '%CHK' and et.FILE_NAME_ not like '%VERF' and (et.TRANS_STATUS_=0 or et.TRANS_STATUS_ is null)) ett inner join (select IF_NO_, TABLE_NAME_, IS_PARTITION_,LOAD_ACTION_,IF_NAME_ from CFG_TABLE_INTERFACE where LOAD_STYLE_=1 and DAY_OR_MONTH_=" + 
/* 109 */         dayMonth + 
/* 110 */         ") cfg on ett.IF_NO_=cfg.IF_NO_ " + 
/* 111 */         "where (cfg.LOAD_ACTION_=0 and ett.CHK_STATUS_=1) or cfg.LOAD_ACTION_=1 or (cfg.LOAD_ACTION_=0 and cfg.IF_NAME_='DIM')";
/* 112 */       queryStmt = conn.prepareStatement(selectSql);
/* 113 */       LOGGER.info("Query config interface sql ==> " + selectSql);
/* 114 */       writeLocalLog("Query config interface sql ==> " + selectSql);
/* 115 */       queryRs = queryStmt.executeQuery();
/* 116 */       Configuration conf = new Configuration();
/* 117 */       FileSystem fs = FileSystem.get(conf);
/* 118 */       String dfInstanceId = (String)env.get("jobLogId");
/*     */ 
/* 121 */       Map tmpMap = new HashMap();
/* 122 */       while (queryRs.next()) {
/* 123 */         String ifNumber = queryRs.getString(1).trim();
/* 124 */         String tableName = queryRs.getString(2).trim();
/* 125 */         int isPartition = queryRs.getInt(3);
/* 126 */         String fileName = queryRs.getString(4).trim();
/* 127 */         String filePath = queryRs.getString(5).trim();
/* 128 */         int dataTime = queryRs.getInt(6);
/* 129 */         splitMap(ifNumber, tableName, isPartition, fileName, 
/* 130 */           filePath, dataTime, tmpMap, env);
/*     */       }
/*     */ 
/* 133 */       Iterator<Object> tableName = tmpMap.entrySet().iterator();
/*     */ 
/* 132 */       while (tableName.hasNext()) {
/* 133 */         Map.Entry tmpEntry = (Map.Entry)tableName.next();
/*     */         try {
/* 135 */           updateStatusStart(session, (List)tmpEntry.getValue());
/*     */         } catch (Exception e) {
/* 137 */           this.result = JobResult.ERROR;
/* 138 */           LOGGER.error("设置数据开始标识出错", e);
/* 139 */           writeLocalLog(e, "设置数据开始标识出错");
/* 140 */           continue;
/*     */         }
/* 142 */         Map resultMap = copyFile(conf, fs, 
/* 143 */           tmpEntry);
/* 144 */         executeResult = checkResult(resultMap, executeResult);
/* 145 */         if ((resultMap == null) || (
/* 146 */           (!resultMap.containsKey("SKIP")) && 
/* 147 */           (!resultMap
/* 147 */           .containsKey("SUCCESS")))) continue;
/* 148 */         Iterator iterator = 
/* 149 */           ((List)tmpEntry
/* 149 */           .getValue()).iterator();
/* 150 */         Set failSet = 
/* 151 */           (Set)resultMap
/* 151 */           .get("FAIL");
/* 152 */         while (iterator.hasNext()) {
/* 153 */           Map dataMap = (Map)iterator.next();
/* 154 */           String srcPath = (String)dataMap.get("SRC_PATH_");
/* 155 */           String ifNumber = (String)dataMap.get("IF_NUMBER_");
/* 156 */           String tableName_str = (String)dataMap.get("TABLE_NAME_");
/* 157 */           String fileName = (String)dataMap.get("FILE_NAME_");
/* 158 */           int dataTime = Integer.parseInt(
/* 159 */             (String)dataMap
/* 159 */             .get("DATA_TIME_"));
/* 160 */           if ((failSet == null) || (!failSet.contains(srcPath))) {
/* 161 */             boolean update = updateStatus(dfInstanceId, 
/* 162 */               ifNumber, tableName_str, fileName, 
/* 163 */               (String)tmpEntry.getKey(), dataTime);
/* 164 */             if (!update) {
/* 165 */               String errorMsg = "Update file status ERROR ==> IF_NO_:" + 
/* 166 */                 ifNumber + 
/* 167 */                 ", FILE_NAME_:" + 
/* 168 */                 fileName + 
/* 169 */                 ", DATA_TIME_:" + 
/* 170 */                 dataTime + 
/* 171 */                 ", DataFlowInstanceId:" + 
/* 172 */                 dfInstanceId;
/* 173 */               LOGGER.info(errorMsg);
/* 174 */               writeLocalLog(null, errorMsg);
/* 175 */               executeResult = checkResult(errorMsg, 
/* 176 */                 executeResult);
/*     */             }
/*     */           }
/*     */         }
/*     */       }
/*     */     }
/*     */     catch (SQLException e) {
/* 183 */       this.result = JobResult.ERROR;
/* 184 */       String errMsg = "SimpleTransformUDF SQLException";
/* 185 */       LOGGER.error(errMsg, e);
/* 186 */       writeLocalLog(e, errMsg);
///*     */       String errMsg;
/*     */       return null;
/*     */     } catch (IOException e) {
/* 189 */       this.result = JobResult.ERROR;
/* 190 */       String errMsg = "SimpleTransformUDF IOException";
/* 191 */       LOGGER.error(errMsg, e);
/* 192 */       writeLocalLog(e, errMsg);
///*     */       String errMsg;
/*     */       return null;
/*     */     } finally {
/*     */       try {
/* 196 */         if (queryRs != null)
/* 197 */           queryRs.close();
/* 198 */         if (queryStmt != null)
/* 199 */           queryStmt.close();
/* 200 */         if (session != null)
/* 201 */           session.close();
/*     */       } catch (SQLException e) {
/* 203 */         String errMsg = "Close db connection[execute] error.";
/* 204 */         LOGGER.error(errMsg, e);
/* 205 */         writeLocalLog(e, errMsg);
/*     */       }
/*     */     }
/*     */     try
/*     */     {
/* 196 */       if (queryRs != null)
/* 197 */         queryRs.close();
/* 198 */       if (queryStmt != null)
/* 199 */         queryStmt.close();
/* 200 */       if (session != null)
/* 201 */         session.close();
/*     */     } catch (SQLException e) {
/* 203 */       String errMsg = "Close db connection[execute] error.";
/* 204 */       LOGGER.error(errMsg, e);
/* 205 */       writeLocalLog(e, errMsg);
/*     */     }
/*     */ 
/* 208 */     writeLocalLog("节点执行完毕.");
/* 209 */     return executeResult;
/*     */   }
/*     */ 
/*     */   private void splitMap(String ifNumber, String tableName, int isPartition, String fileName, String filePath, int dataTime, Map<String, List<Map<String, String>>> tmpMap, Environment env)
/*     */   {
/* 227 */     String loadOutPath = this.transLoadDirOut.replace("$TABLENAME", tableName);
/* 228 */     String dateString = "";
/* 229 */     DataTimeUtil dtu = new DataTimeUtil();
/* 230 */     if (isPartition == 1)
/*     */     {
/* 232 */       dateString = "PT_TIME_=" + 
/* 233 */         dtu.dealDate(env, "", tableName, 
/* 234 */         fileName.substring(6, 14), "yyyy-MM-dd") + "/";
/* 235 */     } else if (isPartition != 0)
/*     */     {
/* 237 */       this.result = JobResult.ERROR;
/* 238 */       LOGGER.error("Unsupported isPartition value ::" + isPartition);
/* 239 */       writeLocalLog(null, "Unsupported isPartition value ::" + 
/* 240 */         isPartition);
/*     */     }
/* 242 */     loadOutPath = loadOutPath + dateString;
/* 243 */     String srcPath = FileTransUtil.getAbsolutePath(filePath, fileName);
/* 244 */     Map dataMap = new HashMap();
/* 245 */     dataMap.put("IF_NUMBER_", ifNumber);
/* 246 */     dataMap.put("TABLE_NAME_", tableName);
/* 247 */     dataMap.put("FILE_NAME_", fileName);
/* 248 */     dataMap.put("FILE_PATH_", filePath);
/* 249 */     dataMap.put("DATA_TIME_", dataTime);
/* 250 */     dataMap.put("IS_PARTITION_", isPartition);
/* 251 */     dataMap.put("SRC_PATH_", srcPath);
/* 252 */     if (tmpMap.containsKey(loadOutPath)) {
/* 253 */       List list = (List)tmpMap.get(loadOutPath);
/* 254 */       list.add(dataMap);
/*     */     } else {
/* 256 */       List list = new ArrayList();
/* 257 */       list.add(dataMap);
/* 258 */       tmpMap.put(loadOutPath, list);
/*     */     }
/*     */   }
/*     */ 
/*     */   private void initLocalLog(Environment env, Map<String, String> udfParams) {
/* 263 */     this.singleLog = TaskNodeLogger.isSingleLog();
/* 264 */     Object logObj = env.get("taskNodeLogger");
/* 265 */     if (logObj == null) {
/* 266 */       LOGGER.warn("the localLogger is not in Environment");
/* 267 */       this.singleLog = false;
/*     */     } else {
/* 269 */       this.localLogger = ((Logger)logObj);
/*     */     }
/*     */   }
/*     */ 
/*     */   private void writeLocalLog(String info) {
/* 274 */     if (this.singleLog)
/* 275 */       this.localLogger.info(info);
/*     */   }
/*     */ 
/*     */   private void writeLocalLog(Exception e, String errMsg)
/*     */   {
/* 280 */     if (this.singleLog) {
/* 281 */       if (StringUtils.isBlank(errMsg)) {
/* 282 */         errMsg = "execute " + getClass().getName() + 
/* 283 */           " activity error." + 
/* 284 */           AICloudETLExceptionUtil.getErrMsg(e);
/*     */       }
/* 286 */       this.localLogger.error(errMsg, e);
/*     */     }
/*     */   }
/*     */ 
/*     */   public Map<String, Object> checkResult(String copyResult, Map<String, Object> resultMap)
/*     */   {
/* 292 */     if (copyResult != null) {
/* 293 */       this.result = JobResult.ERROR;
/* 294 */       if (resultMap == null) {
/* 295 */         resultMap = new HashMap();
/* 296 */         resultMap.put("ERROR_MESSAGE", copyResult);
/*     */       } else {
/* 298 */         StringBuffer buffer = new StringBuffer(resultMap.get("ERROR_MESSAGE")+"");
/* 300 */         resultMap.put("ERROR_MESSAGE", buffer.append(", \n" + copyResult));
/*     */       }
/*     */     }
/* 304 */     return resultMap;
/*     */   }
/*     */ 
/*     */   public Map<String, Object> checkResult(Map<String, Set<String>> copyResult, Map<String, Object> resultMap)
/*     */   {
/* 309 */     if ((copyResult != null) && 
/* 310 */       (copyResult.containsKey("FAIL"))) {
/* 311 */       this.result = JobResult.ERROR;
/* 312 */       if (resultMap == null) {
/* 313 */         resultMap = new HashMap();
/* 314 */         resultMap.put("ERROR_MESSAGE", 
/* 315 */           "失败文件列表:" + copyResult.get("FAIL"));
/*     */       } else {
/* 317 */         StringBuffer buffer = new StringBuffer(resultMap.get("ERROR_MESSAGE")+"");
/* 319 */         resultMap.put(
/* 320 */           "ERROR_MESSAGE", 
/* 321 */           buffer.append(", \n" + 
/* 322 */           copyResult.get("FAIL")));
/*     */       }
/*     */     }
/* 325 */     return resultMap;
/*     */   }
/*     */ 
/*     */   private void updateStatusStart(Session session, List<Map<String, String>> list)
/*     */     throws Exception
/*     */   {
/* 337 */     String sql = "update STATUS_ET_FILE set TRANS_START_TIME_=now() where IF_NO_=? and FILE_NAME_=? and DATA_TIME_=?";
/*     */ 
/* 339 */     Connection conn = session.connection();
/* 340 */     conn.setAutoCommit(false);
/*     */     try {
/* 342 */       PreparedStatement psmt = conn.prepareStatement(sql);
/* 343 */       for (int i = 0; i < list.size(); i++) {
/* 344 */         Map dataMap = (Map)list.get(i);
/* 345 */         psmt.setString(1, (String)dataMap.get("IF_NUMBER_"));
/* 346 */         psmt.setString(2, (String)dataMap.get("FILE_NAME_"));
/* 347 */         psmt.setInt(3, Integer.parseInt((String)dataMap.get("DATA_TIME_")));
/* 348 */         psmt.addBatch();
/*     */       }
/* 350 */       psmt.executeBatch();
/* 351 */       conn.commit();
/*     */     } catch (Exception e) {
/* 353 */       conn.rollback();
/* 354 */       throw e;
/*     */     } finally {
/* 356 */       if (conn != null)
/* 357 */         conn.close();  }  } 
/* 364 */   public boolean updateStatus(String insId, String iterfaceNO, String tableName, String fileName, String filePath, int dataTime) { boolean update = false;
/* 365 */     Session session = null;
/* 366 */     Connection conn = null;
/* 367 */     PreparedStatement queryStmt = null;
/* 368 */     Statement stmt = null;
/* 369 */     ResultSet queryRs = null;
/*     */     String errMsg;
/*     */     try { session = HibernateUtils.getSessionFactory().openSession();
/* 372 */       conn = session.connection();
/* 373 */       conn.setAutoCommit(false);
/* 374 */       String querySql = "select FILE_SIZE_, FILE_RECORD_NUM_ from STATUS_ET_FILE where IF_NO_='" + 
/* 375 */         iterfaceNO + 
/* 376 */         "' and FILE_NAME_='" + 
/* 377 */         fileName + 
/* 378 */         "' and DATA_TIME_=" + dataTime;
/* 379 */       queryStmt = conn.prepareStatement(querySql);
/* 380 */       queryRs = queryStmt.executeQuery();
/*     */ 
/* 382 */       String udpateSql = "update STATUS_ET_FILE set TRANS_STATUS_=1, FLOW_INST_ID_='" + 
/* 383 */         insId + 
/* 384 */         "', TRANS_END_TIME_=now() " + 
/* 385 */         "where IF_NO_='" + 
/* 386 */         iterfaceNO + 
/* 387 */         "' and FILE_NAME_='" + 
/* 388 */         fileName + 
/* 389 */         "' and DATA_TIME_=" + dataTime;
/* 390 */       LOGGER.info("Update  ET file status sql ==> " + udpateSql);
/* 391 */       writeLocalLog("Update  ET file status sql ==> " + udpateSql);
/* 392 */       stmt = conn.createStatement();
/* 393 */       stmt.execute(udpateSql);
/* 394 */       while (queryRs.next()) {
/* 395 */         String fileSize = queryRs.getString(1);
/* 396 */         String recordNum = queryRs.getString(2);
/* 397 */         String insertSql = "insert into STATUS_LD_FILE(DATA_TIME_, FLOW_INST_ID_, TABLE_NAME_, FILE_NAME_, FILE_PATH_, FILE_SIZE_, FILE_RECORD_NUM_) values (" + 
/* 399 */           dataTime + 
/* 400 */           ", '" + 
/* 401 */           insId + 
/* 402 */           "', '" + 
/* 403 */           tableName + 
/* 404 */           "', '" + 
/* 405 */           fileName + 
/* 406 */           "', '" + 
/* 407 */           filePath + 
/* 408 */           "', '" + 
/* 409 */           fileSize + 
/* 410 */           "', '" + 
/* 411 */           recordNum + "')";
/* 412 */         stmt.execute(insertSql);
/* 413 */         LOGGER.info("Insert  LD file status sql ==> " + insertSql);
/* 414 */         writeLocalLog("Insert  LD file status sql ==> " + 
/* 415 */           insertSql);
/*     */       }
/* 417 */       conn.commit();
/* 418 */       update = true;
/*     */     } catch (Exception e) {
/* 420 */       if (conn != null)
/*     */         try {
/* 422 */           conn.rollback();
/*     */         } catch (SQLException e1) {
/* 424 */           String errMsgs = "Database connection rollback Exception";
/* 425 */           LOGGER.error(errMsgs, e1);
/* 426 */           writeLocalLog(e, errMsgs);
/*     */         }
/* 428 */       this.result = JobResult.ERROR;
/* 429 */       String errMsge = "SimpleTransformUDF update status Exception";
/* 430 */       LOGGER.error(errMsge, e);
/* 431 */       writeLocalLog(e, errMsge);
///*     */       String errMsg;
/* 432 */       return update;
/*     */     } finally {
/*     */       try {
/* 435 */         if (queryRs != null)
/* 436 */           queryRs.close();
/* 437 */         if (queryStmt != null)
/* 438 */           queryStmt.close();
/* 439 */         if (stmt != null)
/* 440 */           stmt.close();
/* 441 */         if (session != null)
/* 442 */           session.close();
/*     */       } catch (SQLException e) {
/* 444 */         String errMsge = "Close db connection[updateStatus] error.";
/* 445 */         writeLocalLog(e, errMsge);
/* 446 */         LOGGER.error(errMsge, e);
/*     */       }
/*     */     }
/* 449 */     return update; }
/*     */ 
/*     */   private Map<String, Set<String>> copyFile(Configuration conf, FileSystem fs, Map.Entry<String, List<Map<String, String>>> tmpEntry)
/*     */   {
/*     */     try
/*     */     {
/* 455 */       LOGGER.info("开始向目标路径：" + (String)tmpEntry.getKey() + "拷贝文件...");
/* 456 */       writeLocalLog("开始向目标路径：" + (String)tmpEntry.getKey() + "拷贝文件...");
/* 457 */       List srcPathList = new ArrayList();
/* 458 */       Iterator iterator = ((List)tmpEntry.getValue())
/* 459 */         .iterator();
/* 460 */       while (iterator.hasNext()) {
/* 461 */         Map dataMap = (Map)iterator.next();
/* 462 */         String srcPath = (String)dataMap.get("SRC_PATH_");
/* 463 */         srcPathList.add(new Path(srcPath));
/*     */       }
/*     */ 
/* 466 */       Map resultMap = FileDistCpUtil.newInstance()
/* 467 */         .copy(fs, srcPathList, fs, new Path((String)tmpEntry.getKey()), 
/* 468 */         null, true, false, this.numMapTask, null, conf, 
/* 469 */         null);
/*     */ 
/* 471 */       return resultMap;
/*     */     } catch (Exception e) {
/* 473 */       String copyFile = "拷贝文件到目标表：" + (String)tmpEntry.getKey() + "失败.";
/* 474 */       this.result = JobResult.ERROR;
/* 475 */       writeLocalLog(e, copyFile);
/* 476 */       LOGGER.error(copyFile, e);
/*     */     }
/* 478 */     return null;
/*     */   }
/*     */ 
/*     */   public String copyFile(Configuration conf, FileSystem fs, String srcPath, String destPath)
/*     */   {
/* 483 */     String copyFile = null;
/*     */     try {
/* 485 */       LOGGER.info("Copy file from [" + srcPath + "] to [" + destPath + 
/* 486 */         "]");
/* 487 */       writeLocalLog("Copy file from [" + srcPath + "] to [" + 
/* 488 */         destPath + "]");
/* 489 */       List srcPathList = new ArrayList();
/* 490 */       srcPathList.add(new Path(srcPath));
/* 491 */       Map resultMap = FileDistCpUtil.newInstance()
/* 492 */         .copy(fs, srcPathList, fs, new Path(destPath), conf);
/* 493 */       if ((resultMap != null) && 
/* 494 */         (resultMap.containsKey("FAIL"))) {
/* 495 */         Set failSet = (Set)resultMap.get("FAIL");
/* 496 */         if ((failSet != null) && (failSet.size() > 0)) {
/* 497 */           this.result = JobResult.ERROR;
/* 498 */           copyFile = "Copy file from [" + srcPath + "] to [" + 
/* 499 */             destPath + "] error." + "失败文件列表:" + failSet;
/* 500 */           LOGGER.error(copyFile);
/* 501 */           writeLocalLog(copyFile);
/*     */         }
/*     */       }
/*     */     } catch (Exception e) {
/* 505 */       copyFile = "Copy file from [" + srcPath + "] to [" + destPath + 
/* 506 */         "] error.";
/* 507 */       this.result = JobResult.ERROR;
/* 508 */       String errMsg = "Copy file from [" + srcPath + "] to [" + destPath + 
/* 509 */         "] has exception.";
/* 510 */       writeLocalLog(e, errMsg);
/* 511 */       LOGGER.error(errMsg, e);
/*     */     }
/* 513 */     return copyFile;
/*     */   }
/*     */ 
/*     */   public void releaseResource(HisActivity hisAct)
/*     */   {
/*     */   }
/*     */ 
/*     */   public JobResult getState()
/*     */   {
/* 524 */     return this.result;
/*     */   }
/*     */ 
/*     */   public static void main(String[] args) {
/* 528 */     SimpleTransformDistcpUDF udf = new SimpleTransformDistcpUDF();
/* 529 */     Map udfParams = new HashMap();
/* 530 */     udf.getClass(); udfParams.put("DAY_OR_MONTH", "day");
/* 531 */     udf.execute(null, udfParams);
/*     */   }
/*     */ }

/* Location:           C:\Users\chenlianguo\Desktop\云经分\ocetl_local_cmc_sd.jar
 * Qualified Name:     com.asiainfo.local.cmc.sd.SimpleTransformDistcpUDF
 * JD-Core Version:    0.6.0
 */