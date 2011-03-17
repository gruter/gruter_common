package com.gruter.common.metrics;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.gruter.common.metrics.MetricsValue.MetricsValueSeperator;
import com.gruter.common.metrics.system.SystemMetricsParser;


/**
 * @author jindolk
 *
 */
public class SystemMetrics implements MetricsUpdater {
  private String metricsName;
  private boolean existsFile = false;
  
  //file name -> SystemMetricsParser
  private Map<String, SystemMetricsParser> systemMetircsFiles = new HashMap<String, SystemMetricsParser>();
  
  private GruterMetricsContext context;
  
  public SystemMetrics(String metricsName) {
    this.metricsName = metricsName;
    context = GruterMetricsFactory.getFactory().getContext(metricsName, "system");
    context.registerMetricsUpdater(this);
    
    GruterMetricsFactory factory = GruterMetricsFactory.getFactory();
    String systemFileNameAttr = factory.getAttribute("system.target.files");
    
    if(systemFileNameAttr == null || systemFileNameAttr.length() == 0) {
      GruterMetricsFactory.LOG.info("No system.target.files property in conf/cloudata-metrics.properties");
      return;
    }
    
    String[] fileNameTokens = systemFileNameAttr.split(",");
    for(String eachFileName: fileNameTokens) {
      String fileName = eachFileName.trim();
      File file = new File(fileName);
      if(!file.exists() || file.isDirectory()) {
        GruterMetricsFactory.LOG.error(fileName + " not exists or not a file");
        continue;
      }
      String parserClass = factory.getAttribute("system." + fileName + ".parser");
      if(parserClass == null || parserClass.trim().length() == 0) {
        GruterMetricsFactory.LOG.error("No [system." + fileName + ".parser] property in conf/cloudata-metrics.properties");
        continue;
      }
      try {
        SystemMetricsParser parser = (SystemMetricsParser)Class.forName(parserClass.trim()).newInstance();
        try {
          parser.init(fileName);
        } catch (IOException e) {
          GruterMetricsFactory.LOG.error("Parser init error:" + fileName + ", " + parser + ":" + e.getMessage());
          continue;
        }
        systemMetircsFiles.put(fileName, parser);
        GruterMetricsFactory.LOG.info("Add SystemMetrics[" + fileName + "," + parser + "]");
      } catch (Exception e) {
        GruterMetricsFactory.LOG.error("Can't make parser instance:" + e.getMessage());
      }
      existsFile = true;
    }
  }
  
  public void doUpdates(GruterMetricsContext context) {
    if(!existsFile) {
      return;
    }

    updateVmstat(context);
    
    for(Map.Entry<String, SystemMetricsParser> entry: systemMetircsFiles.entrySet()) {
      String fileName = entry.getKey();
      
      SystemMetricsParser parser = entry.getValue();

      String metricsPropertyName = metricsName + ".system." + fileName;
      context.clearMetricsData(metricsPropertyName);
      
      Map<String, Object> values = parser.getMetricsValues();
      if(values == null || values.isEmpty()) {
        GruterMetricsFactory.LOG.warn("No metrics data: " + fileName);
        continue;
      }
      
      for(Map.Entry<String, Object> valueEntry: values.entrySet()) {
        context.addMetricsData(metricsPropertyName,  new MetricsValue(valueEntry.getKey(), valueEntry.getValue()));
      }
      
      context.addMetricsData(metricsPropertyName, new MetricsValueSeperator());
    }
  }
  
  public void shutdown() {
    context.stopMonitoring();
  }
  
  private void updateVmstat(GruterMetricsContext context) {
    String vmstatName = metricsName + ".system.vmstat";
    context.clearMetricsData(vmstatName);
    
    Runtime rt = Runtime.getRuntime();
    Process process = null;
    long startTime = System.currentTimeMillis();
    BufferedInputStream in = null;
    try {
      process = rt.exec("vmstat");
      in = new BufferedInputStream(process.getInputStream());
      
      StringBuilder sb = new StringBuilder(1000);
      
      while(true) {
        if(in.available() > 0) {
          byte[] buf = new byte[4096];  //vmstat result smaller than 4096
          int length = in.read(buf);
          if(length > 0) {
            sb.append(new String(buf, 0, length));
          }
        } 
        if(System.currentTimeMillis() - startTime > 100) {
          break;
        }
        Thread.sleep(10);
      }
      if(sb.length() > 0) {
        context.addMetricsData(vmstatName, new MetricsValue("", sb.toString()));
        context.addMetricsData(vmstatName, new MetricsValueSeperator());
      }
    } catch (Exception e) {
    } finally {
      if(in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if(process != null) {
        process.destroy();
      }
    }
  }
}
