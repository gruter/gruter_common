package com.gruter.common.server.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.gruter.common.conf.GruterConf;
import com.gruter.common.server.service.AbstractServiceLocator;

public class ThriftConnectionPoolFactory implements Watcher {
  static final Log LOG = LogFactory.getLog(ThriftConnectionPoolFactory.class);
  
  private static ThriftConnectionPoolFactory instance;
  
  private List<AbstractServiceLocator> serviceLocators = new ArrayList<AbstractServiceLocator>();
  
  private ZooKeeper zk;
  
  private String zkServers;
  
  private int zkTimeout;
  
  private GruterConf conf;
  
  private CountDownLatch latch;
  
  private ThriftConnectionPoolFactory(GruterConf conf) throws IOException {
    this.conf = conf;
    this.zkServers = conf.get("zk.servers");
    
    if(zkServers == null) {
      throw new IOException("No zk.servers proeprty in gruter-common.xml");
    }
    zkTimeout = conf.getInt("zk.session.timeout", 30 * 1000);
    connectZK();
  }
  
  public static synchronized ThriftConnectionPoolFactory getInstance(GruterConf conf) throws IOException {
//    if(conf.getBoolean("gaia.test", false)) {
//      return new ThriftConnectionPoolFactory(conf);
//    }
    if(instance == null) {
      instance = new ThriftConnectionPoolFactory(conf);
    }
    return instance;
  }
  
  public ThriftConnectionPool getPool(AbstractServiceLocator serviceLocator, Class thriftClientClass) throws IOException {
    serviceLocator.setZk(zk);
    serviceLocator.initServiceServers(this);
    ThriftConnectionPool pool = new ThriftConnectionPool(serviceLocator, thriftClientClass);
    synchronized(serviceLocators) {
      serviceLocators.add(serviceLocator);
    }
    return pool;
  }

  private void connectZK() throws IOException {
    latch = new CountDownLatch(1);
    zk = new ZooKeeper(zkServers, zkTimeout, this);
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void process(WatchedEvent event) {
    if(event.getType() == Event.EventType.None) {
      switch(event.getState()) {
      case SyncConnected:
        try {
          latch.countDown();
          for(AbstractServiceLocator eachLocator: serviceLocators) {
            eachLocator.setZk(zk);
            eachLocator.initServiceServers(this);
          }
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
        break;
      case Disconnected:
        break;
      case Expired:
        try {
          connectZK();
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
        break;
      }
    } else {
      synchronized(serviceLocators) {
        for(AbstractServiceLocator eachLocator: serviceLocators) {
          eachLocator.processEvent(event, this);
        }
      }
    }
  }
}
