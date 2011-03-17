package com.gruter.common.server.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs.Ids;

import com.gruter.common.conf.GruterConf;
import com.gruter.common.zk.ZKUtil;

public class ServiceLocator extends AbstractServiceLocator {
  private static final Logger LOG = Logger.getLogger(ServiceLocator.class);

  protected String lookupPath;
  
  private Random rand = new Random(System.currentTimeMillis());

  public ServiceLocator(GruterConf conf, String serviceName, int timeout) throws IOException {
    super(conf, serviceName, timeout);
  }
  
  @Override
  public void initServiceServers(Watcher watcher) throws IOException {
    this.lookupPath = ZKUtil.getServiceLiveServerPath(conf, serviceName);
    try {
      ZKUtil.createNode(zk, lookupPath, new byte[]{}, Ids.OPEN_ACL_UNSAFE, 
          CreateMode.PERSISTENT, true);
    } catch (KeeperException.NodeExistsException e) {
    } catch (KeeperException e) {
      LOG.error(e.toString(), e);
    } catch (InterruptedException e) {
    }
    
    try {
      this.servers = zk.getChildren(lookupPath, watcher);
    } catch (Exception e) {
      LOG.error(e.toString(), e);
      throw new IOException(e.getMessage(), e);
    }
  }
  
  @Override
  public void processEvent(WatchedEvent event, Watcher watcher) {
    if (event.getType() == Event.EventType.NodeCreated) {
      if(event.getPath().equals(lookupPath)) {
        try {
          zk.exists(lookupPath, watcher);
        } catch (Exception e) {
          LOG.error(event.toString(), e);
        }
      }
    } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
      synchronized(servers) {
        try {
          if(zk == null) {
            return;
          }
          List<String> oldServers = this.servers;
          this.servers = zk.getChildren(lookupPath, watcher);
          fireServiceChanged(oldServers, servers);
        } catch (Exception e) {
          LOG.error(event.toString(), e);
        }
      }
    }
  }
  
  private void fireServiceChanged(List<String> oldServers, List<String> newServers) {
    Set<String> oldServerSet = new HashSet<String>(oldServers);
    
    List<String> removedServers = new ArrayList<String>();
    List<String> addedServers = new ArrayList<String>();
    
    for(String eachServer: newServers) {
      if(!oldServerSet.remove(eachServer)) {
        addedServers.add(eachServer);
      }
    }
    
    removedServers.addAll(oldServerSet);
    serviceChangeListener.serverChanged(serviceName, newServers, addedServers, removedServers);
  }

  @Override
  public String getServer() {
    if(servers == null) {
      return null;
    }
    
    synchronized(servers) {
      if(servers.size() == 0) {
        return null;
      }
      
      return servers.get(rand.nextInt(servers.size()));
    }
  }
}
