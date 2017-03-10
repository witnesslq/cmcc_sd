package com.asiainfo.local.cmc.sd;

import com.ailk.cloudetl.commons.internal.env.EnvironmentImpl;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DFContext
{
  private static final Log LOG = LogFactory.getLog(DFContext.class);
  public static String getAct()
  {
	  LOG.info("-----start shandong DFContext getAct1");
	  LOG.info("-----start shandong DFContext getAct2");
	  return "20170218";
  }
  public static String getActVar(String act, String key) 
  { 
	String value = null;
    try 
    {
      LOG.info("-----start shandong DFContext getActVar:act=" + act + ",key=" + key);
      Map actOuts = (Map)EnvironmentImpl.getCurrent().get("ACTSOUTS");
      if (actOuts == null) return value;
      LOG.info("-----start shandong DFContext getActVar:actOuts=" + actOuts.toString());

      Map actOut = (Map)actOuts.get(act);
      if (actOut == null) return value;
      LOG.info("-----start shandong DFContext getActVar:parameter=" + actOut.toString());
      value = (String)actOut.get(key);
    }
    catch (Exception e) 
    {
      LOG.info("-----start shandong DFContext getActVar------Exception");
    }
    return value;
  }
}