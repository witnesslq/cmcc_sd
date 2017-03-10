package com.asiainfo.local.cmc.sd;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
import com.ailk.cloudetl.commons.internal.tools.ReadableMsgUtils;
import com.ailk.cloudetl.dbservice.platform.api.DBPool;
import com.ailk.cloudetl.exception.AICloudETLRuntimeException;
import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
import com.ailk.udf.node.shandong.util.FileDistCpUtil;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
/**
 * 
 * @author liugang by 2017
 * test
 *  
 */
public class DistcpHDFSDataByDBFilterUDF implements UDFActivity {
	private static final Log log = LogFactory
			.getLog(DistcpHDFSDataByDBFilterUDF.class);

	private final String DB_CONFIGURE_NAME = "DB_CONFIGURE_NAME";
	private final String INTERFACE_NAME = "INTERFACE_NAME";
	private final String V_DATE = "V_DATE";

	private JobResult result = JobResult.RIGHT;
	private String srcFilePath;
	private String fileNameEnd;
	private String destFilePath;
	protected List<String> needFiles = new ArrayList();

	protected Map<String, Object> retMap = new HashMap();
	private static final String NEED_COPY_FILES = "NEED_COPY_FILES";
	private boolean singleLog = true;
	private Logger localLogger = null;

	private int numMapTask = -1;

	private void checkDependencies(FileSystem srcFS, Path src,
			FileSystem dstFS, Path dst) throws IOException {
		if (srcFS == dstFS) {
			String srcq = src.makeQualified(srcFS).toString() + "/";
			String dstq = dst.makeQualified(dstFS).toString() + "/";
			if (dstq.startsWith(srcq)) {
				if (srcq.length() == dstq.length()) {
					throw new IOException("Cannot copy " + src + " to itself.");
				}
				throw new IOException("Cannot copy " + src
						+ " to its subdirectory " + dst);
			}
		}
	}

	public Map<String, Object> execute(Environment env,
			Map<String, String> udfParams) {
		initLocalLog(env, udfParams);
		try {
			if (initParams(udfParams)) {
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				mkdirs(fs, getDestFilePath());
				deleteFiles(fs, getDestFilePath());
				if (getSrcFilePath() != null) {
					List filePathList = Arrays.asList(getSrcFilePath().split(
							","));
					copyFile(conf, fs, filePathList,
							filterPath(getDestFilePath()));
				}
			}
		} catch (Throwable e) {
			this.result = JobResult.ERROR;
			String msg = "复制HDFS文件出错: " + e.getMessage();
			writeLocalLog(e, msg);
			this.retMap.put(ReadableMsgUtils.readableMsg, msg);
			throw new AICloudETLRuntimeException(msg, e);
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
			this.localLogger = ((Logger) logObj);
		}
	}

	private void writeLocalLog(String info) {
		if (this.singleLog)
			this.localLogger.info(info);
	}

	private void writeLocalLog(Throwable e, String errMsg) {
		if (this.singleLog) {
			if (StringUtils.isBlank(errMsg)) {
				errMsg = "execute " + getClass().getName() + " activity error."
						+ AICloudETLExceptionUtil.getErrMsg(e);
			}
			this.localLogger.error(errMsg, e);
		}
	}

	public void copyFile(Configuration conf, FileSystem fs,
			List<String> filePathList, String toPath) throws Exception {
		log.info("copyFile来源路径==" + filePathList);
		log.info("copyFile目的路径==" + toPath);
		Path fileToPath = new Path(toPath);
		List srcPathList = new ArrayList();
		for (String filePath : filePathList) {
			Path fileFromPath = new Path(filePath);
			if (fs.exists(fileFromPath)) {
				checkDependencies(fs, fileFromPath, fs, fileToPath);
				srcPathList.add(fileFromPath);
			} else {
				String msg = "源文件[" + fileFromPath + "]不存在";
				this.retMap.put(ReadableMsgUtils.readableMsg, msg);
				throw new AICloudETLRuntimeException(msg);
			}
		}
		PathFilter filter = null;
		if (this.fileNameEnd != null) {
			filter = new PathFilter() {
				public boolean accept(Path path) {
					return path.toString().endsWith(
							DistcpHDFSDataByDBFilterUDF.this.fileNameEnd);
				}
			};
		}
		Map resultMap = FileDistCpUtil.newInstance().copy(fs, srcPathList, fs,
				fileToPath, filter, true, false, this.numMapTask, null, conf,
				this.needFiles);
		if ((resultMap != null) && (resultMap.containsKey("FAIL"))) {
			String msg = "向目标路径：" + toPath + "复制文件失败";
			this.retMap.put(ReadableMsgUtils.readableMsg, msg);
			throw new AICloudETLRuntimeException(msg);
		}
	}

	public void deleteFiles(FileSystem fs, String path) {
		log.info("deleteFiles toPath==" + path);
		writeLocalLog("deleteFiles toPath==" + path);
		try {
			Path f = new Path(path);
			if (!fs.isFile(f)) {
				FileStatus[] files = fs.listStatus(f);
				if ((files != null) && (files.length > 0)) {
					for (FileStatus fileStatus : files)
						if (!fileStatus.isDir())
							fs.delete(fileStatus.getPath());
				}
			}
		} catch (Throwable e) {
			String msg = "删除路径" + path + "下的文件发生异常";
			writeLocalLog(e, msg);
			this.retMap.put(ReadableMsgUtils.readableMsg, msg);
			throw new AICloudETLRuntimeException(msg, e);
		}
	}

	public void mkdirs(FileSystem fs, String path) {
		try {
			Path f = new Path(path);
			if (!fs.exists(f)) {
				writeLocalLog("创建路径:" + path);
				fs.mkdirs(f);
			}
		} catch (Throwable e) {
			String msg = "创建路径" + path + "发生异常";
			writeLocalLog(e, msg);
			this.retMap.put(ReadableMsgUtils.readableMsg, msg);
			throw new AICloudETLRuntimeException(msg, e);
		}
	}

	public JobResult getState() {
		return this.result;
	}

	public String trimPath(String path) {
		String retPath = StringUtils.trim(path);
		if (StringUtils.isEmpty(retPath)) {
			return null;
		}
		if (retPath.equals("/")) {
			retPath = retPath + ".";
		}
		return retPath;
	}

	private boolean initParams(Map<String, String> udfParams) throws Exception {
		log.info("HDFS文件复制,源路径==" + (String) udfParams.get("srcFilePath"));
		log.info("HDFS文件复制,目的路径==" + (String) udfParams.get("destFilePath"));
		log.info("HDFS文件复制,文件扩展:==" + (String) udfParams.get("fileNameEnd"));
		String numMapTaskStr = (String) udfParams.get("numMapTask");
		if (StringUtils.isNotBlank(numMapTaskStr)) {
			this.numMapTask = Integer.parseInt(numMapTaskStr);
		}
		writeLocalLog("HDFS文件复制,源路径:" + (String) udfParams.get("srcFilePath")
				+ ",目的路径==" + (String) udfParams.get("destFilePath")
				+ ",文件扩展:=" + (String) udfParams.get("fileNameEnd"));
		if (StringUtils.isEmpty((String) udfParams.get("srcFilePath"))) {
			throw new AICloudETLRuntimeException("需要复制的源路径为空");
		}
		if (StringUtils.isEmpty((String) udfParams.get("destFilePath"))) {
			throw new AICloudETLRuntimeException("需要复制的目的路径为空");
		}
		setSrcFilePath(trimPath((String) udfParams.get("srcFilePath")));
		setDestFilePath(trimPath((String) udfParams.get("destFilePath")));
		setFileNameEnd((String) udfParams.get("fileNameEnd"));
		this.needFiles = getCopyFileName(udfParams);
		if (this.needFiles.size() == 0) {
			String msg = "源路径["
					+ getSrcFilePath()
					+ "]下不存在需要拷贝的文件,或文件已经被转换成功.如果需要重新执行转换请修改STATUS_ET_FILE,STATUS_INTERFACE表当前接口号和数据日期的TRANS_STATUS_为空或并清理已经加载的数据";
			this.retMap.put(ReadableMsgUtils.readableMsg, msg);
			throw new AICloudETLRuntimeException(msg);
		}
		log.info("NEED_COPY_FILES:" + this.needFiles.size());
		writeLocalLog("NEED_COPY_FILES:" + this.needFiles.size());
		this.retMap.put("NEED_COPY_FILES", this.needFiles);
		return true;
	}

	public String filterPath(String path) {
		String lastChar = path.substring(path.length() - 1, path.length());
		if (!"/".equals(lastChar)) {
			path = path + "/";
		}
		return path;
	}

	public String getSrcFilePath() {
		return this.srcFilePath;
	}

	public void setSrcFilePath(String srcFilePath) {
		this.srcFilePath = srcFilePath;
	}

	public String getDestFilePath() {
		return this.destFilePath;
	}

	public void setDestFilePath(String destFilePath) {
		this.destFilePath = destFilePath;
	}

	public String getFileNameEnd() {
		return this.fileNameEnd;
	}

	public void setFileNameEnd(String fileNameEnd) {
		this.fileNameEnd = fileNameEnd;
	}

	public void releaseResource(HisActivity arg0) {
	}

	public List<String> getCopyFileName(Map<String, String> udfParams)
			throws Exception {
		List needCopyFileList = new ArrayList();
		Session session = null;
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String dbConfigureName = (String) udfParams
					.get("DB_CONFIGURE_NAME");
			String interfaceNO = (String) udfParams.get("INTERFACE_NAME");
			String dataTime = (String) udfParams.get("V_DATE");
			log.info("接口参数：" + interfaceNO);
			log.info("目录日期:" + dataTime);
			writeLocalLog("接口参数:" + interfaceNO + "目录日期:" + dataTime);
			try {
				new SimpleDateFormat("yyyyMMdd").parse(dataTime);
			} catch (Exception e) {
				throw new AICloudETLRuntimeException(
						"目录日期为空或格式错误,应该为[yyyyMMdd]");
			}
			if ((interfaceNO == null) || ("".equals(interfaceNO))) {
				throw new AICloudETLRuntimeException("接口号不允许为空!");
			}
			if ((dbConfigureName != null)
					&& (!"".equals(dbConfigureName.toString()))) {
				con = DBPool.INSTANCE.getDBConnection(dbConfigureName
						.toString());
			} else {
				session = HibernateUtils.getSessionFactory().openSession();
				con = session.connection();
			}
			StringBuffer sql = new StringBuffer();
			sql.append("select FILE_NAME_ from STATUS_ET_FILE t where t.IF_NO_=? and t.DATA_TIME_=? and t.STATUS_ = 1 and (TRANS_STATUS_ = 0 or TRANS_STATUS_ is null)");
			ps = con.prepareStatement(sql.toString());
			ps.setString(1, interfaceNO.toString());
			ps.setInt(2, Integer.valueOf(dataTime).intValue());
			rs = ps.executeQuery();
			while (rs.next()) {
				String fileName = rs.getString(1);
				if ((fileName != null) && (!"".equals(fileName)))
					if ((this.fileNameEnd != null)
							&& (!"".equals(this.fileNameEnd))) {
						if (rs.getString(1).endsWith(this.fileNameEnd))
							needCopyFileList.add(rs.getString(1));
					} else
						needCopyFileList.add(rs.getString(1));
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
			if (session != null) {
				session.close();
			}
		}
		log.info("需要拷贝的文件:" + needCopyFileList.size());
		writeLocalLog("需要拷贝的文件:" + needCopyFileList.size());
		return needCopyFileList;
	}
}