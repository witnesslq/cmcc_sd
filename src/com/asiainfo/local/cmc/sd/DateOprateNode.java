package com.asiainfo.local.cmc.sd;

import com.ailk.cloudetl.commons.api.Environment;
import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
import com.ailk.cloudetl.ndataflow.api.HisActivity;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.context.DataFlowContext;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DateOprateNode
  implements UDFActivity
{
  private static final Log LOG = LogFactory.getLog(DateOprateNode.class);
  private JobResult result = JobResult.RIGHT;
  Map<String, Object> returnMap = new HashMap();

  public Map<String, Object> execute(Environment env, Map<String, String> udfParams)
  {
    try
    {
      Calendar c = Calendar.getInstance();
      SimpleDateFormat dateformatter = new SimpleDateFormat(((String)udfParams.get("formatter")).toString());
      SimpleDateFormat dateformatterOut = new SimpleDateFormat(((String)udfParams.get("formatterOut")).toString());
      c.setTime(dateformatter.parse(((String)udfParams.get("date")).toString()));
      int operateValue = Integer.valueOf(((String)udfParams.get("operateValue")).toString()).intValue();
      String operateType = ((String)udfParams.get("operateType")).toString();

      if ("day".equals(operateType))
        c.add(5, operateValue);
      else if ("month".equals(operateType))
        c.add(2, operateValue);
      else if ("year".equals(operateType))
        c.add(1, operateValue);
      else if ("hour".equals(operateType))
        c.add(11, operateValue);
      else if ("minute".equals(operateType)) {
        c.add(12, operateValue);
      }
      LOG.info(((String)udfParams.get("returnName")).toString()+"=="+dateformatterOut.format(c.getTime()));
      this.returnMap.put(((String)udfParams.get("returnName")).toString(), dateformatterOut.format(c.getTime()));
      DataFlowContext localDataFlowContext = (DataFlowContext)EnvironmentImpl.getCurrent().get("DFContext");
      //LOG.info("DFContext.getActVar(计算当前日期ADD1,dir_time)==="+DFContext.getActVar("计算当前日期ADD1","dir_time"));
    }
    catch (Exception e) {
      LOG.error("DateOprateNode is Error : ", e);
    }

    return this.returnMap;
  }

  public JobResult getState()
  {
    return this.result;
  }

  public static void main(String[] args) {
    DateOprateNode d = new DateOprateNode();
    Map m = new HashMap();
    m.put("operateValue", "-2");
    m.put("operateType", "month");
    m.put("formatter", "yyyyMM");
    m.put("date", "201301");
    m.put("returnName", "kk");
    System.out.println(d.execute(null, m).get("kk"));
  }

  public void releaseResource(HisActivity arg0)
  {
  }
}