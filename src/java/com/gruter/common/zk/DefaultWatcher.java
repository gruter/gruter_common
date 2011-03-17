package com.gruter.common.zk;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class DefaultWatcher implements Watcher {
  private CountDownLatch latch;
  
  public DefaultWatcher() {
    this(new CountDownLatch(1));
  }
  
  public DefaultWatcher(CountDownLatch latch) {
    this.latch = latch;
  }
  
  @Override
  public void process(WatchedEvent event) {
    if(event.getType() == Event.EventType.None) {
      switch (event.getState()) {
      case SyncConnected:
        latch.countDown();
        break;
      }
    }
  }
}
