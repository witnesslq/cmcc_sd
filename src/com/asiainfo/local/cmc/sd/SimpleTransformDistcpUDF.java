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
import com.ailk.udf.node.shandong.util.FileDistCpUtil;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

public class SimpleTransformDistcpUDF implements UDFActivity {
	private static final Log LOGGER = LogFactory
			.getLog(SimpleTransformDistcpUDF.class);

	private final String DAY_OR_MONTH = "DAY_OR_MONTH";
	private final String TRANS_LOAD_DIR_OUT = "/asiainfo/ETL/trans/out/$TABLENAME/";
	private final String ERROR_MESSAGE = "ERROR_MESSAGE";

	private JobResult result = JobResult.RIGHT;

	private boolean singleLog = true;
	private Logger localLogger = null;
	private int numMapTask = -1;
	private String transLoadDirOut = null;

	@SuppressWarnings("unchecked")
	public Map<String, Object> execute(Environment env,
			Map<String, String> udfParams) {
		initLocalLog(env, udfParams);
		writeLocalLog("开始执行节点" + getClass().getName());
		Map executeResult = new HashMap();
		String tmpTransLoadDirOut = (String) udfParams.get("transLoadDirOut");
		if (StringUtils.isNotBlank(tmpTransLoadDirOut))
			this.transLoadDirOut = tmpTransLoadDirOut;
		else {
			this.transLoadDirOut = "/asiainfo/ETL/trans/out/$TABLENAME/";
		}

		String numMapTaskStr = (String) udfParams.get("numMapTask");
		if (StringUtils.isNotBlank(numMapTaskStr)) {
			this.numMapTask = Integer.parseInt(numMapTaskStr);
		}
		String dayOrMonth = (String) udfParams.get("DAY_OR_MONTH");
		int dayMonth = 0;
		if ((dayOrMonth != null) && (dayOrMonth.trim().length() > 0)
				&& (dayOrMonth.trim().toLowerCase().startsWith("d"))) {
			dayMonth = 1;
		} else if ((dayOrMonth != null) && (dayOrMonth.trim().length() > 0)
				&& (dayOrMonth.trim().toLowerCase().startsWith("m"))) {
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
			String selectSql = "select cfg.IF_NO_, cfg.TABLE_NAME_, cfg.IS_PARTITION_, ett.FILE_NAME_, ett.FILE_PATH_, ett.DATA_TIME_ from (select et.IF_NO_, et.FILE_NAME_, et.FILE_PATH_, et.DATA_TIME_,si.CHK_STATUS_ from STATUS_ET_FILE et left join STATUS_INTERFACE si on et.IF_NO_=si.IF_NO_ and et.DATA_TIME_=si.DATA_TIME_ where et.STATUS_=1 and et.FILE_NAME_ not like '%CHK' and et.FILE_NAME_ not like '%VERF' and (et.TRANS_STATUS_=0 or et.TRANS_STATUS_ is null)) ett inner join (select IF_NO_, TABLE_NAME_, IS_PARTITION_,LOAD_ACTION_,IF_NAME_ from CFG_TABLE_INTERFACE where LOAD_STYLE_=1 and DAY_OR_MONTH_="
					+ dayMonth
					+ ") cfg on ett.IF_NO_=cfg.IF_NO_ "
					+ "where (cfg.LOAD_ACTION_=0 and ett.CHK_STATUS_=1) or cfg.LOAD_ACTION_=1 or (cfg.LOAD_ACTION_=0 and cfg.IF_NAME_='DIM')";
			queryStmt = conn.prepareStatement(selectSql);
			LOGGER.info("Query config interface sql ==> " + selectSql);
			writeLocalLog("Query config interface sql ==> " + selectSql);
			queryRs = queryStmt.executeQuery();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			String dfInstanceId = (String) env.get("jobLogId");

			Map tmpMap = new HashMap();
			while (queryRs.next()) {
				String ifNumber = queryRs.getString(1).trim();
				String tableName = queryRs.getString(2).trim();
				int isPartition = queryRs.getInt(3);
				String fileName = queryRs.getString(4).trim();
				String filePath = queryRs.getString(5).trim();
				int dataTime = queryRs.getInt(6);
				splitMap(ifNumber, tableName, isPartition, fileName, filePath,
						dataTime, tmpMap, env);
			}

			Iterator<Object> tableName = tmpMap.entrySet().iterator();

			while (tableName.hasNext()) {
				Map.Entry tmpEntry = (Map.Entry) tableName.next();
				try {
					updateStatusStart(session, (List) tmpEntry.getValue());
				} catch (Exception e) {
					this.result = JobResult.ERROR;
					LOGGER.error("设置数据开始标识出错", e);
					writeLocalLog(e, "设置数据开始标识出错");
					continue;
				}
				Map resultMap = copyFile(conf, fs, tmpEntry);
				executeResult = checkResult(resultMap, executeResult);
				if ((resultMap == null)
						|| ((!resultMap.containsKey("SKIP")) && (!resultMap
								.containsKey("SUCCESS"))))
					continue;
				Iterator iterator = ((List) tmpEntry.getValue()).iterator();
				Set failSet = (Set) resultMap.get("FAIL");
				while (iterator.hasNext()) {
					Map dataMap = (Map) iterator.next();
					String srcPath = (String) dataMap.get("SRC_PATH_");
					String ifNumber = (String) dataMap.get("IF_NUMBER_");
					String tableName_str = (String) dataMap.get("TABLE_NAME_");
					String fileName = (String) dataMap.get("FILE_NAME_");
					int dataTime = Integer.parseInt((String) dataMap
							.get("DATA_TIME_"));
					if ((failSet == null) || (!failSet.contains(srcPath))) {
						boolean update = updateStatus(dfInstanceId, ifNumber,
								tableName_str, fileName,
								(String) tmpEntry.getKey(), dataTime);
						if (!update) {
							String errorMsg = "Update file status ERROR ==> IF_NO_:"
									+ ifNumber
									+ ", FILE_NAME_:"
									+ fileName
									+ ", DATA_TIME_:"
									+ dataTime
									+ ", DataFlowInstanceId:" + dfInstanceId;
							LOGGER.info(errorMsg);
							writeLocalLog(null, errorMsg);
							executeResult = checkResult(errorMsg, executeResult);
						}
					}
				}
			}
		} catch (SQLException e) {
			this.result = JobResult.ERROR;
			String errMsg = "SimpleTransformUDF SQLException";
			LOGGER.error(errMsg, e);
			writeLocalLog(e, errMsg);
			// String errMsg;
			return null;
		} catch (IOException e) {
			this.result = JobResult.ERROR;
			String errMsg = "SimpleTransformUDF IOException";
			LOGGER.error(errMsg, e);
			writeLocalLog(e, errMsg);
			// String errMsg;
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

		writeLocalLog("节点执行完毕.");
		return executeResult;
	}

	private void splitMap(String ifNumber, String tableName, int isPartition,
			String fileName, String filePath, int dataTime,
			Map<String, List<Map<String, String>>> tmpMap, Environment env) {
		String loadOutPath = this.transLoadDirOut.replace("$TABLENAME",
				tableName);
		String dateString = "";
		DataTimeUtil dtu = new DataTimeUtil();
		if (isPartition == 1) {
			dateString = "PT_TIME_="
					+ dtu.dealDate(env, "", tableName,
							fileName.substring(6, 14), "yyyy-MM-dd") + "/";
		} else if (isPartition != 0) {
			this.result = JobResult.ERROR;
			LOGGER.error("Unsupported isPartition value ::" + isPartition);
			writeLocalLog(null, "Unsupported isPartition value ::"
					+ isPartition);
		}
		loadOutPath = loadOutPath + dateString;
		String srcPath = FileTransUtil.getAbsolutePath(filePath, fileName);
		Map dataMap = new HashMap();
		dataMap.put("IF_NUMBER_", ifNumber);
		dataMap.put("TABLE_NAME_", tableName);
		dataMap.put("FILE_NAME_", fileName);
		dataMap.put("FILE_PATH_", filePath);
		dataMap.put("DATA_TIME_", dataTime);
		dataMap.put("IS_PARTITION_", isPartition);
		dataMap.put("SRC_PATH_", srcPath);
		if (tmpMap.containsKey(loadOutPath)) {
			List list = (List) tmpMap.get(loadOutPath);
			list.add(dataMap);
		} else {
			List list = new ArrayList();
			list.add(dataMap);
			tmpMap.put(loadOutPath, list);
		}
	}

	private void initLocalLog(Environment env, Map<String, String> udfParams) {
		this.singleLog = TaskNodeLogger.isSingleLog();
		Object logObj = env.get("taskNodeLogger");
		if (logObj == null) {
			LOGGER.warn("the localLogger is not in Environment");
			this.singleLog = false;
		} else {
			this.localLogger = ((Logger) logObj);
		}
	}

	private void writeLocalLog(String info) {
		if (this.singleLog)
			this.localLogger.info(info);
	}

	private void writeLocalLog(Exception e, String errMsg) {
		if (this.singleLog) {
			if (StringUtils.isBlank(errMsg)) {
				errMsg = "execute " + getClass().getName() + " activity error."
						+ AICloudETLExceptionUtil.getErrMsg(e);
			}
			this.localLogger.error(errMsg, e);
		}
	}

	public Map<String, Object> checkResult(String copyResult,
			Map<String, Object> resultMap) {
		if (copyResult != null) {
			this.result = JobResult.ERROR;
			if (resultMap == null) {
				resultMap = new HashMap();
				resultMap.put("ERROR_MESSAGE", copyResult);
			} else {
				StringBuffer buffer = new StringBuffer(
						resultMap.get("ERROR_MESSAGE") + "");
				resultMap.put("ERROR_MESSAGE",
						buffer.append(", \n" + copyResult));
			}
		}
		return resultMap;
	}

	public Map<String, Object> checkResult(Map<String, Set<String>> copyResult,
			Map<String, Object> resultMap) {
		if ((copyResult != null) && (copyResult.containsKey("FAIL"))) {
			this.result = JobResult.ERROR;
			if (resultMap == null) {
				resultMap = new HashMap();
				resultMap.put("ERROR_MESSAGE",
						"失败文件列表:" + copyResult.get("FAIL"));
			} else {
				StringBuffer buffer = new StringBuffer(
						resultMap.get("ERROR_MESSAGE") + "");
				resultMap.put("ERROR_MESSAGE",
						buffer.append(", \n" + copyResult.get("FAIL")));
			}
		}
		return resultMap;
	}

	private void updateStatusStart(Session session,
			List<Map<String, String>> list) throws Exception {
		String sql = "update STATUS_ET_FILE set TRANS_START_TIME_=now() where IF_NO_=? and FILE_NAME_=? and DATA_TIME_=?";

		Connection conn = session.connection();
		conn.setAutoCommit(false);
		try {
			PreparedStatement psmt = conn.prepareStatement(sql);
			for (int i = 0; i < list.size(); i++) {
				Map dataMap = (Map) list.get(i);
				psmt.setString(1, (String) dataMap.get("IF_NUMBER_"));
				psmt.setString(2, (String) dataMap.get("FILE_NAME_"));
				psmt.setInt(3,
						Integer.parseInt((String) dataMap.get("DATA_TIME_")));
				psmt.addBatch();
			}
			psmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			conn.rollback();
			throw e;
		} finally {
			if (conn != null)
				conn.close();
		}
	}

	public boolean updateStatus(String insId, String iterfaceNO,
			String tableName, String fileName, String filePath, int dataTime) {
		boolean update = false;
		Session session = null;
		Connection conn = null;
		PreparedStatement queryStmt = null;
		Statement stmt = null;
		ResultSet queryRs = null;
		String errMsg;
		try {
			session = HibernateUtils.getSessionFactory().openSession();
			conn = session.connection();
			conn.setAutoCommit(false);
			String querySql = "select FILE_SIZE_, FILE_RECORD_NUM_ from STATUS_ET_FILE where IF_NO_='"
					+ iterfaceNO
					+ "' and FILE_NAME_='"
					+ fileName
					+ "' and DATA_TIME_=" + dataTime;
			queryStmt = conn.prepareStatement(querySql);
			queryRs = queryStmt.executeQuery();

			String udpateSql = "update STATUS_ET_FILE set TRANS_STATUS_=1, FLOW_INST_ID_='"
					+ insId
					+ "', TRANS_END_TIME_=now() "
					+ "where IF_NO_='"
					+ iterfaceNO
					+ "' and FILE_NAME_='"
					+ fileName
					+ "' and DATA_TIME_=" + dataTime;
			LOGGER.info("Update  ET file status sql ==> " + udpateSql);
			writeLocalLog("Update  ET file status sql ==> " + udpateSql);
			stmt = conn.createStatement();
			stmt.execute(udpateSql);
			while (queryRs.next()) {
				String fileSize = queryRs.getString(1);
				String recordNum = queryRs.getString(2);
				String insertSql = "insert into STATUS_LD_FILE(DATA_TIME_, FLOW_INST_ID_, TABLE_NAME_, FILE_NAME_, FILE_PATH_, FILE_SIZE_, FILE_RECORD_NUM_) values ("
						+ dataTime
						+ ", '"
						+ insId
						+ "', '"
						+ tableName
						+ "', '"
						+ fileName
						+ "', '"
						+ filePath
						+ "', '"
						+ fileSize + "', '" + recordNum + "')";
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
					String errMsgs = "Database connection rollback Exception";
					LOGGER.error(errMsgs, e1);
					writeLocalLog(e, errMsgs);
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
		return update;
	}

	private Map<String, Set<String>> copyFile(Configuration conf,
			FileSystem fs, Map.Entry<String, List<Map<String, String>>> tmpEntry) {
		try {
			LOGGER.info("开始向目标路径：" + (String) tmpEntry.getKey() + "拷贝文件...");
			writeLocalLog("开始向目标路径：" + (String) tmpEntry.getKey() + "拷贝文件...");
			List srcPathList = new ArrayList();
			Iterator iterator = ((List) tmpEntry.getValue()).iterator();
			while (iterator.hasNext()) {
				Map dataMap = (Map) iterator.next();
				String srcPath = (String) dataMap.get("SRC_PATH_");
				srcPathList.add(new Path(srcPath));
			}

			Map resultMap = FileDistCpUtil.newInstance().copy(fs, srcPathList,
					fs, new Path((String) tmpEntry.getKey()), null, true,
					false, this.numMapTask, null, conf, null);

			return resultMap;
		} catch (Exception e) {
			String copyFile = "拷贝文件到目标表：" + (String) tmpEntry.getKey() + "失败.";
			this.result = JobResult.ERROR;
			writeLocalLog(e, copyFile);
			LOGGER.error(copyFile, e);
		}
		return null;
	}

	public String copyFile(Configuration conf, FileSystem fs, String srcPath,
			String destPath) {
		String copyFile = null;
		try {
			LOGGER.info("Copy file from [" + srcPath + "] to [" + destPath
					+ "]");
			writeLocalLog("Copy file from [" + srcPath + "] to [" + destPath
					+ "]");
			List srcPathList = new ArrayList();
			srcPathList.add(new Path(srcPath));
			Map resultMap = FileDistCpUtil.newInstance().copy(fs, srcPathList,
					fs, new Path(destPath), conf);
			if ((resultMap != null) && (resultMap.containsKey("FAIL"))) {
				Set failSet = (Set) resultMap.get("FAIL");
				if ((failSet != null) && (failSet.size() > 0)) {
					this.result = JobResult.ERROR;
					copyFile = "Copy file from [" + srcPath + "] to ["
							+ destPath + "] error." + "失败文件列表:" + failSet;
					LOGGER.error(copyFile);
					writeLocalLog(copyFile);
				}
			}
		} catch (Exception e) {
			copyFile = "Copy file from [" + srcPath + "] to [" + destPath
					+ "] error.";
			this.result = JobResult.ERROR;
			String errMsg = "Copy file from [" + srcPath + "] to [" + destPath
					+ "] has exception.";
			writeLocalLog(e, errMsg);
			LOGGER.error(errMsg, e);
		}
		return copyFile;
	}

	public void releaseResource(HisActivity hisAct) {
	}

	public JobResult getState() {
		return this.result;
	}

	public static void main(String[] args) {
		SimpleTransformDistcpUDF udf = new SimpleTransformDistcpUDF();
		Map udfParams = new HashMap();
		udf.getClass();
		udfParams.put("DAY_OR_MONTH", "day");
		udf.execute(null, udfParams);
	}
}