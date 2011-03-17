package com.gruter.common.metrics;

import java.text.DecimalFormat;

/**
 * @author jindolk
 *
 */
public class MetricsValue {
  private static DecimalFormat df = new DecimalFormat("0.000");
  
  private String key;
  private Object value;
  
  public MetricsValue(String key, Object value) {
    this.key = key;
    this.value = value;
  }

  public void setValue(Object value) {
    this.value = value;
  }
  
  public Object getValue() {
    return value;
  }
  
  public String getKey() {
    return key;
  }
  
  public String toString() {
    return key + ":" + value;
  }

  public String getValueAsString() {
    if(value instanceof Number) {
      return df.format(((Number)value).doubleValue());
    } else {
      return value.toString();
    }
  }
  
  public boolean isNewLine() {
    return false;
  }
  
  public static class MetricsValueSeperator extends MetricsValue {
    public MetricsValueSeperator() {
      super("", "");
    }
    public boolean isNewLine() {
      return true;
    }
  }
}
