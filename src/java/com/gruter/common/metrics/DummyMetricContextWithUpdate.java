package com.gruter.common.metrics;

import java.util.List;

/**
 * @author jindolk
 *
 */
public class DummyMetricContextWithUpdate extends GruterMetricsContext {

  @Override
  protected void emitRecords(String name, List<MetricsValue> metricsValues) {
//    for(MetricsValue eachMetricsValue: metricsValues) {
//      System.out.println(eachMetricsValue.getKey() + ":" + eachMetricsValue.getValueAsString());
//    }    
  }


  protected boolean isMonitoring() {
    return true;
  }

  @Override
  protected void flush() {
  }
  
  public String toString() {
    return "DummyMetricContextWithUpdate:" + metricsName + "," + contextName;
  }
}
