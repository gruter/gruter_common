package com.gruter.common.metrics.system;

import java.io.IOException;
import java.util.Map;

/**
 * @author jindolk
 *
 */
public interface SystemMetricsParser {
  public Map<String, Object> getMetricsValues();

  public void init(String fileName) throws IOException;
}
