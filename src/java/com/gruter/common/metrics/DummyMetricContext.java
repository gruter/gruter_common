package com.gruter.common.metrics;

import java.util.List;

public class DummyMetricContext extends GruterMetricsContext {

  @Override
  protected void emitRecords(String name, List<MetricsValue> metricsValues) {
  
  }

  protected boolean isMonitoring() {
    return false;
  }

  @Override
  protected void flush() {
  }
  
  public String toString() {
    return "DummyMetricContext:" + metricsName + "," + contextName;
  }
}
