package com.asiainfo.local.cmc.sd;

import com.ailk.cloudetl.action.Execution;
import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
import com.ailk.cloudetl.dataflow.xmlmodel.Property;
import com.ailk.cloudetl.dbservice.DBUtils;
import com.ailk.cloudetl.ndataflow.api.constant.JobResult;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivity;
import com.ailk.cloudetl.ndataflow.api.udf.UDFActivityNew;
import com.ailk.cloudetl.script.ScriptEnv;
import com.ailk.cloudetl.script.ScriptUtil;
import com.ailk.cloudetl.udf.xml.UDF;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Udf extends Execution
{
  private static final Log LOG = LogFactory.getLog(Udf.class);
  private String className = null;
  private Map params = new HashMap();

  protected void init() throws Exception {
    LOG.info("----UDF  shandong init start-----");
    UDF thisNode = (UDF)this.node;

    this.className = thisNode.getClassName();
    LOG.info("----UDF  shandong className=" + this.className);
    ScriptEnv scriptEnv = new ScriptEnv();
    scriptEnv.setVar("DFContext", new DFContext());
    Iterator keys = this.vars.keySet().iterator();
    while (keys.hasNext()) {
      String key = (String)keys.next();
      scriptEnv.setVar(key, this.vars.get(key));
    }

    ScriptUtil stuil = new ScriptUtil();

    if (thisNode.getProperties() != null)
      for (Property prop : thisNode.getProperties())
        this.params.put(prop.getName(), (String)stuil.getScriptResult(scriptEnv, prop.getValue()));
  }

  public void run(Map out)
    throws Exception
  {
    Class clazz = null;

    clazz = Class.forName(this.className);
    UDFActivity UDF = (UDFActivity)clazz.newInstance();
    Map result = null;
    if ((UDF instanceof UDFActivityNew)) {
      UDFActivityNew un = (UDFActivityNew)UDF;
      result = un.execute(EnvironmentImpl.getCurrent(), this.params, out);
    } else {
      result = UDF.execute(EnvironmentImpl.getCurrent(), this.params);
    }
    if (result != null) out.putAll(result);

    if (UDF.getState() == JobResult.ERROR)
      throw new Exception("UDF Error");
  }

  protected void clear()
  {
  }

  protected void releaseResource(String executionId) throws Exception
  {
    List valueList = DBUtils.querySql("select VALUE_ from ocdc_execution_data where EXECUTION_ID_='" + executionId + "' and KEY_='" + "ACT.VAR.UDF_CLASS" + "'");
    if ((valueList != null) && (valueList.size() > 0)) {
      Class clazz = Class.forName(valueList.get(0).toString());
      UDFActivity uDFActivity = (UDFActivity)clazz.newInstance();
      if ((uDFActivity instanceof UDFActivityNew)) {
        ((UDFActivityNew)uDFActivity).releaseResource(executionId);
      } else {
        LOG.info("releaseResource method old, class is " + valueList.get(0).toString());
        uDFActivity.releaseResource(null);
      }
    }
  }
}