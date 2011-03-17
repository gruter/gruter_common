package com.gruter.common.metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public abstract class GruterMetricsContext {
  private Timer timer;
  private Set<MetricsUpdater> updaters = new HashSet<MetricsUpdater>(1);
  
  private Map<String, List<MetricsValue>> metricsData = new HashMap<String, List<MetricsValue>>(); 
  
  private boolean isMonitoring = false;

  protected String contextName;
  
  protected String metricsName;
  
  private int period = 10; //default 10 sec
  
  public synchronized void startMonitoring() {
    if (!isMonitoring) {
      startTimer();
      isMonitoring = true;
    }
  }
  
  public void stopMonitoring() {
    timer.cancel(); 
  }
  
  private synchronized void startTimer() {
    //System.out.println(this.toString() + " timer start:" + period);
    if (timer == null) {
      timer = new Timer("Timer thread for monitoring", true);
      TimerTask task = new TimerTask() {
          public void run() {
            try {
              timerEvent();
            }
            catch (IOException ioe) {
              ioe.printStackTrace();
            }
          }
        };
      long millis = period * 1000;
      timer.scheduleAtFixedRate(task, millis, millis);
    }
  }
  
  private void timerEvent() throws IOException {
    Collection<MetricsUpdater> tmpUpdaters;
    synchronized (this) {
      tmpUpdaters = new ArrayList<MetricsUpdater>(updaters);
    }     
    for (MetricsUpdater updater : tmpUpdaters) {
      try {
        updater.doUpdates(this);
      }
      catch (Throwable throwable) {
        throwable.printStackTrace();
      }
    }
    synchronized(metricsData) {
      for(Map.Entry<String, List<MetricsValue>> entry: metricsData.entrySet()) {
        emitRecords(entry.getKey(), entry.getValue());
      }
    }
    flush();
  }
  
  public void clearMetricsData(String metricName) {
    synchronized(metricsData) {
      List<MetricsValue> values = metricsData.get(metricName);
      if(values != null) {
        values.clear();
      }
    }
  }
  
  protected abstract void emitRecords(String name, List<MetricsValue> metricsValues);
  protected abstract boolean isMonitoring();
  protected abstract void flush();
  
  /**
   * @param testMetrics
   */
  public void registerMetricsUpdater(MetricsUpdater updater) {
    updaters.add(updater);
  }

  public void addMetricsData(String metricName,  MetricsValue value) {
    synchronized(metricsData) {
      List<MetricsValue> list = metricsData.get(metricName);
      if(list == null) {
        list = new ArrayList<MetricsValue>();
        metricsData.put(metricName, list);
      }
      
      list.add(value);
    }
  }

  public void setMetricsName(String metricsName, String contextName) {
    this.metricsName = metricsName;
    this.contextName = contextName;
  }
  
  public void setPeriod(int period) {
    this.period = period;
  }
}
