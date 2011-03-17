package com.gruter.common.metrics;

/**
 * @author jindolk
 *
 */
public interface RPCServerMetrics {
  public void incrementRunningCall();
  public void decrementRunningCall();

  public void incrementCallQueue();
  public void decrementCallQueue();
  public void setQueueWaitTime(long waitTime);
}
