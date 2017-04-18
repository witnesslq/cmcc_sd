package com.asiainfo.local.cmc.sd;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.Session;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
import com.ailk.cloudetl.dbservice.commons.HiveJdbcClient;
import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;

@SuppressWarnings({"unchecked","deprecation"})
public class HiveLoadUDF2 implements UDFActivity {

    class LoadJob {

        public String TABLE_NAME_;//目标表名
        public Integer DATA_TIME_;//数据日期
        public String FILE_NAME_;//文件名
        public String FILE_PATH_;//文件HDFS路径
        public String LOAD_ACTION_;//是否追加.追加=1,覆盖=0
        public String IS_PARTITION_;//是否分区.分区=1,不分区=0
        public String TIME_WINDOW_;//入库开始时间(定义几点此接口开始入库,如09:00:00)
        public String DAY_FLAG_;//日期标识(月接口在哪天入库,如2,3等.)
        public String VALIDATE_STATUS_;//校验状态:成功=1,失败=0
        public String NEED_CHECK_RECORD_NUM_;//是否需要校验记录条数:需要=1,不需要=0

        public LoadJob(Object[] objs) {
            TABLE_NAME_ = objs[0] == null ? null : objs[0].toString();//目标表名
            DATA_TIME_ = objs[1] == null ? null : Integer.valueOf(objs[1].toString());//数据日期
            FILE_NAME_ = objs[2] == null ? null : objs[2].toString();//文件名
            FILE_PATH_ = objs[3] == null ? null : objs[3].toString();//文件HDFS路径
            LOAD_ACTION_ = objs[4] == null ? null : objs[4].toString();//是否追加.追加=1,覆盖=0
            IS_PARTITION_ = objs[5] == null ? null : objs[5].toString();//是否分区.分区=1,不分区=0
            TIME_WINDOW_ = objs[6] == null ? null : objs[6].toString();//入库开始时间(定义几点此接口开始入库,如09:00:00)
            DAY_FLAG_ = objs[7] == null ? null : objs[7].toString();//日期标识(月接口在哪天入库,如2,3等.)
            VALIDATE_STATUS_ = objs[8] == null ? null : objs[8].toString();//校验状态:成功=1,失败=0
            NEED_CHECK_RECORD_NUM_ = objs[9] == null ? null : objs[9].toString();//是否需要校验记录条数:需要=1,不需要=0
            LOG.info("配置信息: 目标表[" + TABLE_NAME_ + "]"
                    + "数据日期[" + DATA_TIME_ + "]"
                    + "路径[" + FILE_PATH_ + "]"
                    + "文件名[" + FILE_NAME_ + "]"
                    + "是否追加[" + LOAD_ACTION_ + "]"
                    + "是否分区[" + IS_PARTITION_ + "]"
                    + "需要加载时间[" + TIME_WINDOW_ + "]"
                    + "月接口加载时间[" + DAY_FLAG_ + "]");
        }
    }

    class LoadTableThread implements Runnable {

        private String tableName;
        private List<LoadJob> jobList;
        private FileSystem hdfs;
        private Configuration conf;
        private boolean isDone = false;

        LoadTableThread(String tableName, List<LoadJob> jobList, FileSystem hdfs, Configuration conf) {
            this.tableName = tableName;
            this.jobList = jobList;
            this.hdfs = hdfs;
            this.conf = conf;
        }

		@Override
        public void run() {
            try {
                Date d = new Date();
                Date compare = new Date();
                String TIME_WINDOW_ = jobList.get(0).TIME_WINDOW_;
                String DAY_FLAG_ = jobList.get(0).DAY_FLAG_;
                if (TIME_WINDOW_ != null && DAY_FLAG_ != null) {
                    //入库日期和入库时间都存在
                    String[] ss = TIME_WINDOW_.toString().split(":");
                    compare.setHours(Integer.valueOf(ss[0]).intValue());
                    compare.setMinutes(Integer.valueOf(ss[1]).intValue());
                    compare.setSeconds(Integer.valueOf(ss[2]).intValue());
                    LOG.info("入库时间和入库日期都有配置TIME_WINDOW_[" + TIME_WINDOW_ + "]DAY_FLAG_[" + DAY_FLAG_ + "]");
                    writeLocalLog("入库时间和入库日期都有配置TIME_WINDOW_[" + TIME_WINDOW_ + "]DAY_FLAG_[" + DAY_FLAG_ + "]");
                    if (d.after(compare) && Integer.valueOf(DAY_FLAG_.toString()) <= d.getDate()) {
                        LOG.info("入库时间和日期符合要求,开始加载[" + tableName + "] 共计文件[" + jobList.size() + "]个");
                        writeLocalLog("入库时间和日期符合要求,开始加载[" + tableName + "] 共计文件[" + jobList.size() + "]个");
                        long start = System.currentTimeMillis();
                        doLoad(tableName, jobList, conf, hdfs);
                        long stop = System.currentTimeMillis();
                        LOG.info("完成加载[" + tableName + "] 共计文件[" + jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                        writeLocalLog("完成加载[" + tableName + "] 共计文件[" + jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                        for (LoadJob job : jobList) {
                            loadedFiles.add(job.FILE_PATH_ + File.separator + job.FILE_NAME_);
                        }
                    }
                } else if (TIME_WINDOW_ != null) {
                    //只存在入库时间
                    String[] ss = TIME_WINDOW_.toString().split(":");
                    compare.setHours(Integer.valueOf(ss[0]).intValue());
                    compare.setMinutes(Integer.valueOf(ss[1]).intValue());
                    compare.setSeconds(Integer.valueOf(ss[2]).intValue());
                    LOG.info("入库时间有配置TIME_WINDOW_[" + TIME_WINDOW_ + "]");
                    writeLocalLog("入库时间有配置TIME_WINDOW_[" + TIME_WINDOW_ + "]");
                    if (d.after(compare)) {
                        LOG.info("入库时间符合要求,开始加载[" + tableName + "] 共计文件[" + jobList.size() + "]个");
                        writeLocalLog("入库时间符合要求,开始加载[" + tableName + "] 共计文件[" + jobList.size() + "]个");
                        long start = System.currentTimeMillis();
                        doLoad(tableName, jobList, conf, hdfs);
                        long stop = System.currentTimeMillis();
                        LOG.info("完成加载[" + tableName + "] 共计文件[" + jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                        writeLocalLog("完成加载[" + tableName + "] 共计文件[" + jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                        for (LoadJob job : jobList) {
                            loadedFiles.add(job.FILE_PATH_ + File.separator + job.FILE_NAME_);
                        }
                    }
                } else if (DAY_FLAG_ != null) {
                    LOG.info("入库日期有配置DAY_FLAG_[" + DAY_FLAG_ + "]");
                    writeLocalLog("入库日期有配置DAY_FLAG_[" + DAY_FLAG_ + "]");
                    //只存在入库日期
                    if (Integer.valueOf(DAY_FLAG_.toString()) <= d.getDate()) {
                        LOG.info("入库日期符合要求,开始加载[" + tableName + "] 共计文件[" + jobList.size() + "]个");
                        writeLocalLog("入库日期符合要求,开始加载[" + tableName + "] 共计文件[" + jobList.size() + "]个");
                        long start = System.currentTimeMillis();
                        doLoad(tableName, jobList, conf, hdfs);
                        long stop = System.currentTimeMillis();
                        LOG.info("完成加载[" + tableName + "] 共计文件[" + jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                        writeLocalLog("完成加载[" + tableName + "] 共计文件[" + jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                        for (LoadJob job : jobList) {
                            loadedFiles.add(job.FILE_PATH_ + File.separator + job.FILE_NAME_);
                        }
                    }
                } else {
                    LOG.info("入库日期和入库时间都没有配置");
                    writeLocalLog("入库日期和入库时间都没有配置");
                    //入库日期和入库时间都不存在
                    LOG.info("开始加载[" + tableName + "] 共计文件[" + jobList.size() + "]个");
                    writeLocalLog("开始加载[" + tableName + "] 共计文件[" + jobList.size() + "]个");
                    long start = System.currentTimeMillis();
                    doLoad(tableName, jobList, conf, hdfs);
                    long stop = System.currentTimeMillis();
                    LOG.info("完成加载[" + tableName + "] 共计文件[" + jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                    writeLocalLog("完成加载[" + tableName + "] 共计文件[" + jobList.size() + "]个, 耗时[" + (stop - start) + "]毫秒");
                    for (LoadJob job : jobList) {
                        loadedFiles.add(job.FILE_PATH_ + File.separator + job.FILE_NAME_);
                    }
                }
            } catch (Exception e) {
                errorsb.append(tableName + ",");
            }
            isDone = true;
        }

    }

    class HDFSCopyThread implements Runnable {

        private String fileName;
        private String filePath;
        private String destPath;
        private FileSystem hdfs;
        public String message;
        public boolean isSuccess;
        private boolean isDone = false;

        HDFSCopyThread(String fileName, String filePath, String destPath, FileSystem hdfs) {
            this.fileName = fileName;
            this.filePath = filePath;
            this.destPath = destPath;
            this.hdfs = hdfs;
        }

        @Override
        public void run() {
            try {
                long start = System.currentTimeMillis();
                isSuccess = FileUtil.copy(hdfs, new Path(filePath +File.separator+ fileName), hdfs, new Path(destPath), false, true, hdfs.getConf());
                long stop = System.currentTimeMillis();
                LOG.info("copy file["+filePath +File.separator+ fileName+"] to ["+destPath+"] done , use time["+(stop-start)+"]ms");
                writeLocalLog("copy file["+filePath +File.separator+ fileName+"] to ["+destPath+"] done , use time["+(stop-start)+"]ms");
            } catch (Exception ex) {
            	isSuccess = false;
                message = "copy file[" + filePath +File.separator+ fileName + "] to [" + destPath+ "] error: " + ex.getMessage();
            }
            isDone = true;
        }
    }
    
    private static final Log LOG = LogFactory.getLog(HiveLoadUDF2.class);
    private JobResult result = JobResult.RIGHT;
    private static final String LOAD_TMP_DST = "/asiainfo/ETL/load/temp/";//load临时目录路径
    private static final String LOAD_SRC_PATH = "/asiainfo/ETL/trans/out";//load临时目录路径
    SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Map<String, Object> returnMap = new HashMap<String, Object>();
    //写节点日志需要用到的变量
    private boolean singleLog = true;
    private Logger localLogger = null;

    private StringBuffer errorsb = new StringBuffer();

    private String database;
    private String connStr;
    private ThreadPoolExecutor copyThreadPool;
    private ThreadPoolExecutor loadThreadPool;
    List<String> loadedFiles = new ArrayList<String>();

	private List<LoadJob> getLoadList(String tableName, String dataTime) {
        List<LoadJob> result = new ArrayList<LoadJob>();
        Session session = HibernateUtils.getSessionFactory().openSession();
        StringBuilder sb = new StringBuilder(
                "select distinct a.TABLE_NAME_, "
                + "a.DATA_TIME_, "
                + "a.FILE_NAME_, "
                + "a.FILE_PATH_, "
                + "b.LOAD_ACTION_, "
                + "b.IS_PARTITION_, "
                + "b.TIME_WINDOW_, "
                + "b.DAY_FLAG_, "
                + "a.VALIDATE_STATUS_,"
                + "b.NEED_CHECK_RECORD_NUM_ "
                + " from STATUS_LD_FILE a, " 
                + "(select distinct IF_NO_,	" 
	            +    "TABLE_NAME_," 
	            +    "LOAD_STYLE_," 
	            +    "HIVE_STATUS_," 
	            +    "NEED_CHECK_RECORD_NUM_," 
	            +    "DAY_FLAG_," 
	            +    "TIME_WINDOW_," 
	            +    "IS_PARTITION_," 
	            +    "LOAD_ACTION_, " 
	            +    "PRI_ "
	            +    " from CFG_TABLE_INTERFACE) b "
                + "where upper(a.TABLE_NAME_)=upper(b.TABLE_NAME_) "
                + "and (a.HIVE_LOAD_STATUS_=0 or a.HIVE_LOAD_STATUS_ is null) " //未加载的
                + "and ((b.NEED_CHECK_RECORD_NUM_= 1 and a.VALIDATE_STATUS_ = 1) or (b.NEED_CHECK_RECORD_NUM_ is null)) " //过滤未通过行数校验的文件                      
                + "and b.HIVE_STATUS_=1 " //要在hive中加载的
                + "and b.LOAD_STYLE_!=3 " //需要统一加载的
                + "and exists (select 1 from STATUS_ET_FILE where FLOW_INST_ID_ is not null and a.FLOW_INST_ID_=FLOW_INST_ID_) "//与ET_FILE表匹配的待加载文件
        		+ "and not exists (select 1 from STATUS_INTERFACE where b.LOAD_ACTION_=0 and upper(a.TABLE_NAME_)=upper(b.TABLE_NAME_) and b.IF_NO_=IF_NO_ and DATA_TIME_=a.DATA_TIME_ and (TRANS_STATUS_=0 or TRANS_STATUS_ is null))");//trans over
        if (!StringUtils.isEmpty(tableName)) {
            sb.append(" and a.TABLE_NAME_=?");
        }
        if (!StringUtils.isEmpty(dataTime)) {
            sb.append(" and a.DATA_TIME_=?");
        }
        sb.append(" order by b.PRI_ desc");

        SQLQuery querySql = session.createSQLQuery(sb.toString());

        boolean flag = false;

        if (!StringUtils.isEmpty(tableName)) {
            querySql.setString(0, tableName);
            flag = true;
        }

        if (!StringUtils.isEmpty(dataTime)) {
            if (flag) {
                querySql.setInteger(1, Integer.valueOf(dataTime));
            } else {
                querySql.setInteger(0, Integer.valueOf(dataTime));
            }
        }
        LOG.info("Hive加载查询待加载文件SQL:[" + querySql.getQueryString() + "]");
        writeLocalLog("Hive加载查询待加载文件SQL:[" + querySql.getQueryString() + "]");
        List<Object> temp = querySql.list();
        for (Object obj : temp) {
            result.add(new LoadJob((Object[]) obj));
        }
        LOG.info("Hive加载查询待加载文件SQL结果是否为空:" + result.isEmpty() + ",待加载文件个数:" +result.size());
        writeLocalLog("Hive加载查询待加载文件SQL结果是否为空:" + result.isEmpty() + ",待加载文件个数:" +result.size());
        return result;
    }

    @Override
    public Map<String, Object> execute(Environment env,
            Map<String, String> udfParams) throws Exception {
        //日志
        initLocalLog(env, udfParams);
        this.writeLocalLog("开始执行hiveload节点."+this.getClass().getName());
        try {
        	connStr = (String) EnvironmentImpl.getCurrent().get("com.ailk.cloudetl.dbservice.hive.connStr");
        	log.info("HiveJdbc Connection String:"+connStr);
        	this.writeLocalLog("HiveJdbc Connection String:"+connStr);
        	database = udfParams.get("database") == null ? "" : udfParams.get("database").toString();
            //从DB中获取待加载的文件列表
            String tableName = udfParams.get("TABLE_NAME") == null ? null : udfParams.get("TABLE_NAME").toString();
            String dataTime = udfParams.get("DATA_TIME_") == null ? null : udfParams.get("DATA_TIME_").toString();
            List<LoadJob> queryList = getLoadList(tableName, dataTime);

            //按照目标表顺序执行加载动作
            Map<String, List<LoadJob>> loadTableList = new HashMap<String, List<LoadJob>>();
            for (LoadJob job : queryList) {
                if (!loadTableList.containsKey(job.TABLE_NAME_)) {
                    loadTableList.put(job.TABLE_NAME_, new ArrayList<LoadJob>());
                }
                loadTableList.get(job.TABLE_NAME_).add(job);
            }
            if(loadTableList.size() > 0){
	            //初始化线程池
	            int threadNum = udfParams.get("threadNum") == null ? 10 : Integer.valueOf(udfParams.get("threadNum").toString());
	            copyThreadPool = new ThreadPoolExecutor(threadNum, Integer.MAX_VALUE, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
	            loadThreadPool = new ThreadPoolExecutor(threadNum, Integer.MAX_VALUE, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
	            Configuration conf = new Configuration();
	            FileSystem hdfs = FileSystem.get(conf);
	            List<LoadTableThread> loadThreads = new ArrayList<LoadTableThread>();
	            this.writeLocalLog("待处理任务数为："+loadTableList.size()+",开始利用多线程执行任务...");
	            for (String loadTableName : loadTableList.keySet()) {
	                LoadTableThread thread = new LoadTableThread(loadTableName, loadTableList.get(loadTableName), hdfs, conf);
	                loadThreadPool.execute(thread);
	                loadThreads.add(thread);
	            }
	            while(true) {
	            	boolean allDone = true;
	                for (LoadTableThread thread : loadThreads) {
	                    if (!thread.isDone) {
	                    	allDone = false;
	                    	break;
	                    }
	                }
	                if(allDone){
	                	break;
	                }else{
	                	Thread.sleep(1000);
	                }
	            }
            }
        } catch (Exception e) {
            result = JobResult.ERROR;
            LOG.error(e.getMessage(), e);
            writeLocalLog(e, null);
        } finally {
        	if(copyThreadPool != null && !copyThreadPool.isShutdown()){
        		copyThreadPool.shutdown();
        	}
        	if(loadThreadPool != null && !loadThreadPool.isShutdown()){
        		loadThreadPool.shutdown();
        	}
        }
        return returnMap;
    }

    private void initLocalLog(Environment env, Map<String, String> udfParams) {
        this.singleLog = TaskNodeLogger.isSingleLog();
        Object logObj = env.get(TaskNodeLogger.TASK_NODE_LOGGER);
        if (logObj == null) {
            LOG.warn("the localLogger is not in Environment");
            this.singleLog = false;
        } else {
            localLogger = (Logger) logObj;
        }
    }

    private void writeLocalLog(Exception e, String errMsg) {
        if (singleLog) {
            if (StringUtils.isBlank(errMsg)) {
                errMsg = "execute " + this.getClass().getName() + " activity error." + AICloudETLExceptionUtil.getErrMsg(e);
            }
            localLogger.error(errMsg, e);
        }
    }

    private void writeLocalLog(String message) {
        if (singleLog) {
            localLogger.info(message);
        }
    }

    private void copyMultiJobFile(List<LoadJob> partitionJobList, String tempdst, FileSystem hdfs) throws Exception {
        List<HDFSCopyThread> copyThreads = new ArrayList<HDFSCopyThread>();
        //HDFSCopyThread
        if (!hdfs.exists(new Path(tempdst))) {
            hdfs.mkdirs(new Path(tempdst));
        }
        for (LoadJob job : partitionJobList) {
            HDFSCopyThread thread = new HDFSCopyThread(job.FILE_NAME_, job.FILE_PATH_, tempdst, hdfs);
            copyThreadPool.execute(thread);
            copyThreads.add(thread);
        }
        while (true) {
        	boolean allDone = true;
            for (HDFSCopyThread thread : copyThreads) {
                if (!thread.isDone) {
                	allDone = false;
                	break;
                }
            }
            if(allDone){
            	break;
            }else{
            	Thread.sleep(1000);
            }
        }
        for (HDFSCopyThread thread : copyThreads) {
            if (!thread.isSuccess) {
                updateTable(thread.fileName, thread.filePath, "error", false);
                throw new RuntimeException("copy file fail." + thread.message);
            }
        }
    }

    private HashSet<String> getBasePartitionKeys(List<LoadJob> joblist) {
        HashSet<String> result = new HashSet<String>();
        for (LoadJob job : joblist) {
            String path = job.FILE_PATH_;
            if (File.separator.equals(String.valueOf(job.FILE_PATH_.charAt(job.FILE_PATH_.length() - 1)))) {
                path = job.FILE_PATH_.substring(0, job.FILE_PATH_.length() - 1);
            }
            String[] splits = path.split(File.separator);
            String partitionKey = new String();
            for (String split : splits) {
                if (split.indexOf("=") >= 0) {
                    String[] partition = split.split("=");
                    partitionKey = partition[0] + "='" + partition[1]+"'";
                    //LOG.info("基础分区字段［"+partitionKey+"］");
                    break;
                }
            }
            result.add(partitionKey);
        }
        return result;
    }

    private Map<String, List<LoadJob>> getJobListByPartition(List<LoadJob> joblist) {
        Map<String, List<LoadJob>> partitionList = new HashMap<String, List<LoadJob>>();
        for (LoadJob job : joblist) {
            String path = job.FILE_PATH_;
            if (File.separator.equals(String.valueOf(job.FILE_PATH_.charAt(job.FILE_PATH_.length() - 1)))) {
                path = job.FILE_PATH_.substring(0, job.FILE_PATH_.length() - 1);
            }
            String[] splits = path.split(File.separator);
            StringBuilder partitionsb = new StringBuilder();
            for (String split : splits) {
                if (split.indexOf("=") >= 0) {
                    String[] partition = split.split("=");
                    partitionsb.append(partition[0]);
                    partitionsb.append("='");
                    partitionsb.append(partition[1]);
                    partitionsb.append("',");
                }
            }
            String partitionStr = partitionsb.substring(0, partitionsb.length() - 1);
            if (!partitionList.containsKey(partitionStr)) {
                partitionList.put(partitionStr, new ArrayList<LoadJob>());
            }
            partitionList.get(partitionStr).add(job);
        }
        return partitionList;
    }

    private Map<Integer, List<LoadJob>> getJobListByDataTime(List<LoadJob> joblist) {
        Map<Integer, List<LoadJob>> partitionList = new HashMap<Integer, List<LoadJob>>();
        for (LoadJob job : joblist) {
            Integer dataTime = job.DATA_TIME_;
            if (!partitionList.containsKey(dataTime)) {
                partitionList.put(dataTime, new ArrayList<LoadJob>());
                LOG.info("DataTime［"+dataTime+"］");
            }
            partitionList.get(dataTime).add(job);
        }
        return partitionList;
    }

    private void doLoad(String tableName, List<LoadJob> joblist, Configuration conf, FileSystem hdfs) {
        //获取表加载配置
        boolean isPartition = joblist.get(0).IS_PARTITION_.equals("1");
        boolean isOverWrite = joblist.get(0).LOAD_ACTION_.equals("0");
        LOG.info("加载[" + tableName + "] 是否分区[" + isPartition + "] 是否追加［" + isOverWrite + "］");
        if (isPartition) {
            if (isOverWrite) {
                
                LOG.info("DROP对应的Partition");
                HashSet<String> basePartitionList = getBasePartitionKeys(joblist);
                try {
                    for (String partitionKey : basePartitionList) {
                        excuteHiveSql("use " + this.database + ";ALTER TABLE " + tableName + " DROP PARTITION (" + partitionKey + ")");
                    }
                } catch (Exception e) {
                	updateTableBatch(joblist,"error");
                    throw new RuntimeException("DROP PARTITION FAILED. ", e);
                }
            }
            //按分区字段分组加载
            LOG.info("按分区字段分组加载");
            Map<String, List<LoadJob>> partitionList = getJobListByPartition(joblist);

            for (String partitionStr : partitionList.keySet()) {
                
                LOG.info("加载分区["+partitionStr+"]");
                List<LoadJob> partitionJobList = partitionList.get(partitionStr);
                updateTableBatch(partitionJobList,"do");
                String tempdst = LOAD_TMP_DST + partitionJobList.get(0).FILE_PATH_.substring(LOAD_SRC_PATH.length());
                try {
                    if (hdfs.exists(new Path(tempdst))) {
                        hdfs.delete(new Path(tempdst), true);
                    }
                    copyMultiJobFile(partitionJobList, tempdst, hdfs);
                    excuteHiveSql("use " + this.database + ";LOAD DATA INPATH '" + tempdst + "' INTO TABLE " + tableName + " PARTITION (" + partitionStr + ")");
                    updateTableBatch(partitionJobList,"done");
                } catch (Exception e) {
                	updateTableBatch(partitionJobList,"error");
                    throw new RuntimeException("load files in [" + tempdst + "] into hive [" + tableName + "] fail. ", e);
                }
            }

        } else {
            if (isOverWrite) {
                
                LOG.info("按dataTime字段加载最新数据");
                //按dataTime字段分组，先truncate，后加载
                Map<Integer, List<LoadJob>> dataTimeList = getJobListByDataTime(joblist);
                //获取最大的dataTime
                Integer maxDataTime = Integer.MIN_VALUE;
                for (Integer dataTime : dataTimeList.keySet()) {
                    if (dataTime > maxDataTime) {
                        maxDataTime = dataTime;
                    }
                }
                LOG.info("maxDataTime［"+maxDataTime+"］");
                //开始遍历所有的Job，仅加载最大的dataTime
                for (Integer curDataTime : dataTimeList.keySet()) {
                    LOG.info("加载［"+curDataTime+"］开始");
                    List<LoadJob> curJobList = dataTimeList.get(curDataTime);
                    if (curDataTime.equals(maxDataTime)) {
                        String tempdst = LOAD_TMP_DST + curJobList.get(0).FILE_PATH_.substring(LOAD_SRC_PATH.length());
                        LOG.info("临时目标目录［"+tempdst+"］");
                        try {
                            if (hdfs.exists(new Path(tempdst))) {
                                LOG.info("临时目标目录［"+tempdst+"］已存在，删除");
                                hdfs.delete(new Path(tempdst), true);
                            }
                            copyMultiJobFile(curJobList, tempdst, hdfs);
                            excuteHiveSql("use " + this.database + ";LOAD DATA INPATH '" + tempdst + "' OVERWRITE INTO TABLE " + tableName);
                            updateTableBatch(curJobList,"done");
                        } catch (Exception e) {
                        	updateTableBatch(curJobList,"error");
                            throw new RuntimeException("load files in [" + tempdst + "] into hive [" + tableName + "] fail. ", e);
                        }
                    } else {//非最大的dataTime，不加载，仅更新状态
                    	updateTableBatch(curJobList,"done");
                    }
                    LOG.info("加载［"+curDataTime+"］完成");
                }
            } else {
                String tempdst = LOAD_TMP_DST + joblist.get(0).FILE_PATH_.substring(LOAD_SRC_PATH.length());
                try {
                    if (hdfs.exists(new Path(tempdst))) {
                        hdfs.delete(new Path(tempdst), true);
                    }
                    copyMultiJobFile(joblist, tempdst, hdfs);
                    excuteHiveSql("use " + this.database + ";LOAD DATA INPATH '" + tempdst + "' INTO TABLE " + tableName);
                    updateTableBatch(joblist,"done");
                } catch (Exception e) {
                	updateTableBatch(joblist,"error");
                    throw new RuntimeException("load files in [" + tempdst + "] into hive [" + tableName + "] fail. ", e);
                }
            }
        }
    }

    private void excuteHiveSql(String sql) throws Exception {
        LOG.info("执行HQL:["+sql+"]");
        this.executeSql(sql);
    }
    
    public String executeSql(String sql) throws Exception {
		HiveJdbcClient hiveClient = new HiveJdbcClient(connStr);
		String returnValue = null;
		try{
			String[] sqls = StringUtils.splitByWholeSeparator(sql, ";");
			for (String s : sqls) {
				hiveClient.execSql(s);
			}
		}finally{
			if(hiveClient != null){
				hiveClient.destroy();	
			}
		}
		return returnValue;
	}
    private void updateTableBatch( List<LoadJob> jobList,String flag){
    	Session session = null;
    	PreparedStatement ups = null;
    	Connection con = null;
    	try {
            session = HibernateUtils.getSessionFactory().openSession();
            con = session.connection();
            con.setAutoCommit(false);
            String sql = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=0,HIVE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
            if ("do".equals(flag)) {
            	sql = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=2,HIVE_LOAD_START_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
            } else if ("done".equals(flag)) {
            	sql = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=1,HIVE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
            }
            ups = con.prepareStatement(sql);
			for(LoadJob job : jobList){
	            String time = dateformatter.format(new Date());
	            ups.setString(1, time);
	            ups.setString(2, job.FILE_PATH_);
	            ups.setString(3, job.FILE_NAME_);
	            ups.execute();
	            con.commit();
	            //ups.addBatch();
				ups.clearParameters();
			}
			//ups.executeBatch();
			//con.commit();
        } catch (Exception e) {
            LOG.error("updateTableBatch error", e);
        } finally {
        	try{
				if (ups != null) {
					ups.close();
				}
	            if (session != null) {
	                session.close();
	            }
        	}catch(Exception e){
        		e.printStackTrace();
        	}
        }
    }
    private void updateTable(String FILE_NAME_, String FILE_PATH_, String flag, boolean overwrite) {
        //跟据load结果更新STATUS_LD_FILE表中记录状态
        Session session = null;

        try {
            session = HibernateUtils.getSessionFactory().openSession();
            session.beginTransaction();

            String sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=0,HIVE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
            if (overwrite) {
                sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=0,HIVE_LOAD_END_TIME_=? where FILE_PATH_=?";
                if ("do".equals(flag)) {
                    sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=2,HIVE_LOAD_START_TIME_=? where FILE_PATH_=?";
                } else if ("done".equals(flag)) {
                    sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=1,HIVE_LOAD_END_TIME_=? where FILE_PATH_=?";
                }
            } else {
                if ("do".equals(flag)) {
                    sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=2,HIVE_LOAD_START_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
                } else if ("done".equals(flag)) {
                    sql4sqlSTATUS_LD_FILE = "update STATUS_LD_FILE set HIVE_LOAD_STATUS_=1,HIVE_LOAD_END_TIME_=? where FILE_PATH_=? and FILE_NAME_=?";
                }
            }
            
            SQLQuery STATUS_LD_FILE = session.createSQLQuery(sql4sqlSTATUS_LD_FILE);
            String time = dateformatter.format(new Date());
            STATUS_LD_FILE.setString(0, time);
            STATUS_LD_FILE.setString(1, FILE_PATH_);
            if (!overwrite) {
                STATUS_LD_FILE.setString(2, FILE_NAME_);
                LOG.info("excute sql ["+sql4sqlSTATUS_LD_FILE+"] ["+time+"] ["+FILE_PATH_+"] ["+FILE_NAME_+"]");
            } else {
                LOG.info("excute sql ["+sql4sqlSTATUS_LD_FILE+"] ["+time+"] ["+FILE_PATH_+"]");
            }
            STATUS_LD_FILE.executeUpdate();

            session.getTransaction().commit();
        } catch (Exception e) {
            LOG.error("updateTable error", e);
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    public void releaseResource(HisActivity hisAct) {
        // TODO Auto-generated method stub

    }

    @Override
    public JobResult getState() {
        return result;
    }

    public static void main(String[] args) {
//		try {
//			com.ailk.cloudetl.commons.api.Configuration.init("AICloudETLConf.xml");
//			HiveLoadUDF u = new HiveLoadUDF();
//			Map <String, String> m = new HashMap <String, String> ();
//			u.execute(null, m);
//		} catch(Exception e) {
//			e.printStackTrace();
//		}
        String FILE_PATH_ = "/asiainfo/ETL/trans/out/ods_userscore_accu_ds/PT_TIME_=20131012/";
        String path = FILE_PATH_;
        if ("/".equals(String.valueOf(FILE_PATH_.charAt(FILE_PATH_.length() - 1)))) {
            path = FILE_PATH_.substring(0, FILE_PATH_.length() - 1);
        }
        System.out.println(FILE_PATH_.charAt(FILE_PATH_.length() - 1));
        System.out.println(path);
        String partition = path.substring(path.lastIndexOf("/") + 1);
        System.out.println(partition);
        String partitionKey = partition.split("=")[0];
        String partitionValue = partition.split("=")[1];

        System.out.println(partitionKey + "|" + partitionValue);
        System.out.println(FILE_PATH_.substring(LOAD_SRC_PATH.length()));

        StringBuffer s = new StringBuffer("123");
        System.out.println(s.substring(0, s.length()));
    }
}