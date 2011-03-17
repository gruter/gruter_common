package com.gruter.common.metrics;

public interface MetricsUpdater {
  public void doUpdates(GruterMetricsContext context);
  public void shutdown();
}
