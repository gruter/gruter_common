package com.gruter.common.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jindolk
 *
 */
public class MetricsNumber {
  private AtomicLong count = new AtomicLong(0);
  private AtomicLong lastValue = new AtomicLong(0);
  private AtomicLong currentValue = new AtomicLong(0);
  private AtomicLong max = new AtomicLong(0);
  
  public MetricsNumber() {
  }
  
  public void mark() {
    lastValue.getAndSet(currentValue.get());
  }
  
  public long getDiff() {
    return currentValue.get() - lastValue.get();
  }

  public void add(long delta) {
    if(delta > max.get()) {
      max.getAndSet(delta);
    }
    
    count.incrementAndGet();
    currentValue.addAndGet(delta);
  }
  
  public long getValue() {
    return currentValue.get();
  }
  
  public long getMax() {
    return max.get();
  }
  
  public long getCount() {
    return count.get();
  }
  
  public float getAvg() {
    if(count.get() == 0) {
      return 0;
    }
    return (float)(currentValue.floatValue() / count.floatValue());
  }
}
