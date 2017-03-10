package com.asiainfo.local.cmc.sd;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.Session;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
import com.ailk.cloudetl.commons.internal.tools.HibernateUtils;
import com.ailk.cloudetl.exception.utils.AICloudETLExceptionUtil;
import com.ailk.cloudetl.ndataflow.intarnel.log.TaskNodeLogger;
 
 public class DataTimeUtil
 {
   private boolean singleLog = true;
   private Logger localLogger = null;
   private static final Log LOG = LogFactory.getLog(DataTimeUtil.class);
 
   public String dealDate(Environment env, String interfaceNo, String tableName, String inDate, String outputFormat) {
     String returnValue = inDate;
     if ((StringUtils.trimToNull(interfaceNo) != null) || (StringUtils.trimToNull(tableName) != null)) {
       try {
         String sql = "select distinct DATE_OP_TYPE,DATE_OP_VALUE,DAY_OR_MONTH_ from CFG_TABLE_INTERFACE where 1=1 ";
         if (StringUtils.trimToNull(tableName) != null) {
           sql = sql + " and lower(TABLE_NAME_)=lower('" + tableName + "')";
         }
         List list = excuteQuerySQL(sql);
         if ((list != null) && (list.size() > 0)) {
           String opT = (String)((Object[])list.get(0))[0];
           String opV = (String)((Object[])list.get(0))[1];
           Object dayOrMonth = ((Object[])list.get(0))[2];
           if ((StringUtils.trimToNull(opT) != null) && (StringUtils.trimToNull(opV) != null)) {
             returnValue = dealDate(env, inDate, opT, opV, "yyyyMMdd", outputFormat);
             if ("2".equals(String.valueOf(dayOrMonth)))
               returnValue = returnValue.substring(0, returnValue.length() - 2) + "01";
           }
         }
       }
       catch (Exception e) {
         e.printStackTrace();
       }
     }
     return returnValue;
   }
 
   public String dealDate(Environment env, String inDate, String opType, String opValue, String inputFormat, String outputFormat)
   {
     initLocalLog(env);
     String returnValue = inDate;
     try {
       Calendar c = Calendar.getInstance();
       SimpleDateFormat dateInputFormatter = new SimpleDateFormat(
         inputFormat);
       SimpleDateFormat dateOutputFormatter = new SimpleDateFormat(
         outputFormat);
       c.setTime(dateInputFormatter.parse(inDate));
       int operateValue = Integer.valueOf(opValue).intValue();
       if ("day".equals(opType))
         c.add(5, operateValue);
       else if ("month".equals(opType))
         c.add(2, operateValue);
       else if ("year".equals(opType))
         c.add(1, operateValue);
       else if ("hour".equals(opType))
         c.add(11, operateValue);
       else if ("minute".equals(opType)) {
         c.add(12, operateValue);
       }
       returnValue = dateOutputFormatter.format(c.getTime());
     } catch (Exception e) {
       e.printStackTrace();
       writeLocalLog(e, "");
     }
     return returnValue;
   }
 
   private List<Object[]> excuteQuerySQL(String sql) throws Exception
   {
     Session session = null;
     try {
       session = HibernateUtils.getSessionFactory().openSession();
       SQLQuery q = session.createSQLQuery(sql);
       List list = q.list();
       List localList1 = list;
       return localList1;
     } catch (Exception e) {
       e.printStackTrace();
       LOG.error("excute SQL[" + sql + "] Failed", e);
       throw e;
     } finally {
       if (session != null)
         session.close();
     }
     //throw localObject;
   }
 
   private void initLocalLog(Environment env) {
     this.singleLog = TaskNodeLogger.isSingleLog();
     Object logObj = env.get("taskNodeLogger");
     if (logObj == null) {
       LOG.warn("the localLogger is not in Environment");
       this.singleLog = false;
     } else {
       this.localLogger = ((Logger)logObj);
     }
   }
 
   private void writeLocalLog(Exception e, String errMsg) {
     if (this.singleLog) {
       if (StringUtils.isBlank(errMsg)) {
         errMsg = "execute " + getClass().getName() + 
           " activity error." + 
           AICloudETLExceptionUtil.getErrMsg(e);
       }
       this.localLogger.error(errMsg, e);
     }
   }
 
   public static void main(String[] args) {
     DataTimeUtil du = new DataTimeUtil();
     System.out.println(du.dealDate(new EnvironmentImpl(), "I07065", "", "20131205", "yyyy-MM-dd"));
     System.out.println(du.dealDate(new EnvironmentImpl(), "", "oDs_charging_event_def_ds", "20131102", "yyyy-MM-dd"));
   }
 }