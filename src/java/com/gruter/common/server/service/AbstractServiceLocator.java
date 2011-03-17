package com.gruter.common.server.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.gruter.common.conf.GruterConf;

public abstract class AbstractServiceLocator {
  protected ZooKeeper zk;
  protected GruterConf conf;
  protected List<String> servers = new  ArrayList<String>(); 
  protected ServiceChangeListener serviceChangeListener;
  protected String serviceName;
  protected int timeout;
  
  public abstract void initServiceServers(Watcher watcher) throws IOException;
  public abstract void processEvent(WatchedEvent event, Watcher watcher);
  public abstract String getServer();
  
  public AbstractServiceLocator(GruterConf conf, String serviceName, int timeout) throws IOException {
    this.conf = conf;
    this.serviceName = serviceName;
    this.timeout = timeout;
  }
  
  public String getSerivceName() {
	  return serviceName;
  }
  
  public void setServiceChangeListener(ServiceChangeListener serviceChangeListener) {
    this.serviceChangeListener = serviceChangeListener;
  }
  
  public List<String> getServers() {
    return servers;
  }

  public void setZk(ZooKeeper zk) {
    this.zk = zk;
  }
  
  public ZooKeeper getZk() {
    return this.zk;
  }
  
  public int getTimeout() {
    return this.timeout;
  }
}
