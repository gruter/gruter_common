package com.gruter.common.metrics;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.gruter.common.util.StringUtils;


public class FileMetricContext extends GruterMetricsContext {
  private static SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]");
  private String fileName;
  
  private BufferedOutputStream out;
  
  private boolean printContextName = false;
  
  public synchronized void startMonitoring() {
    fileName = GruterMetricsFactory.getFactory().getAttribute(contextName + ".log.fileName");
    
    fileName = fileName.replace("{0}", metricsName);
    
    try {
      File file = new File(fileName);
      if(file.getParent() != null && !file.getParentFile().exists()) {
        FileUtils.forceMkdir(file.getParentFile());
      }
      GruterMetricsFactory.LOG.info("create metrics file:" + fileName);
      out = new BufferedOutputStream(new FileOutputStream(fileName, true));
    } catch (IOException e) {
      GruterMetricsFactory.LOG.error("can't open output file: " + fileName, e);
      return;
    }

    String period = GruterMetricsFactory.getFactory().getAttribute(contextName + ".period");
    if(period != null) {
      try {
        super.setPeriod(Integer.parseInt(period));
      } catch (NumberFormatException e) {
        GruterMetricsFactory.LOG.warn("wrong " + contextName + ".period property [" +  period + "]");
      }
    }

    if("true".equals(GruterMetricsFactory.getFactory().getAttribute(contextName + ".printContextName"))) {
      printContextName = true;
    }
    super.startMonitoring();
  }
  
  @Override
  protected void emitRecords(String name, List<MetricsValue> metricsValues) {
    Calendar cal = Calendar.getInstance();
    String logDate = df.format(cal.getTime());
    
    try {
      String keyLine;
      
      if(printContextName) {
        keyLine = logDate + "\t" + name;
      } else {
        keyLine = logDate;
      }
      
      String valueLine = StringUtils.leftPad("", keyLine.getBytes().length, ' ');
      
      boolean neededPrint = false;
      for(MetricsValue eachValue: metricsValues) {
        if(eachValue.isNewLine()) {
          out.write(keyLine.getBytes());
          out.write("\n".getBytes());
          out.write(valueLine.getBytes());
          out.write("\n".getBytes());
          
          neededPrint = false;
          if(printContextName) {
            keyLine = logDate + "\t" + name;
          } else {
            keyLine = logDate;
          }
          
          valueLine = StringUtils.leftPad("", keyLine.getBytes().length, ' ');          
        } else {
          if(eachValue.getKey().length() > 0) {
            keyLine += "\t" + eachValue.getKey();
            valueLine += "\t" + eachValue.getValueAsString();
          } else {
            valueLine = valueLine.trim() + eachValue.getValueAsString();
          }
          neededPrint = true;
        }
      }
      
      if(neededPrint) {
        out.write(keyLine.getBytes());
        out.write("\n".getBytes());
        out.write(valueLine.getBytes());
        out.write("\n".getBytes());
      }
    } catch (IOException e) {
      GruterMetricsFactory.LOG.error("Can't FileMetricContext.emitRecords:" + e.getMessage());
    }
  }
  
  @Override
  protected boolean isMonitoring() {
    return true;
  }

  @Override
  protected void flush() {
    try {
      out.write("----------------------------------------------------------------------\n".getBytes());
      out.flush();
    } catch (IOException e) {
      GruterMetricsFactory.LOG.error(e);
    }
  }
  
  public String toString() {
    return "FileMetricContext:" + metricsName + "," + contextName;
  }
}
