package com.gruter.common.server.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.gruter.common.conf.GruterConf;
import com.gruter.common.zk.ZKUtil;

public class ServiceListener implements Watcher {
  static final Log LOG = LogFactory.getLog(ServiceListener.class);
  private GruterConf conf;
  private ZooKeeper zk;
  private String lookupPath;
  private List<String> servers = new ArrayList<String>();
  private String serviceName;
  private ServiceChangeListener serviceChangeListener;

  public ServiceListener(GruterConf conf, ZooKeeper zk, String serviceName, ServiceChangeListener serviceChangeListener)
      throws IOException {
    this.conf = conf;
    this.zk = zk;
    this.lookupPath = ZKUtil.getServiceLiveServerPath(conf, serviceName);
    this.serviceName = serviceName;
    this.serviceChangeListener = serviceChangeListener;
    setWatcher();
  }

  /**
   * Session Expire등으로 인해 ZooKeeper 연결 객체가 변경된 경우 호출한다.
   * 
   * @param zk
   * @throws IOException
   */
  public void setZk(ZooKeeper zk) throws IOException {
    if(this.zk != null && !this.zk.equals(zk)) {
      this.zk = zk;
      setWatcher();
    }
  }

  private void setWatcher() throws IOException {
    try {
      ZKUtil.createNode(zk, lookupPath, new byte[] {}, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, true);
    } catch (KeeperException.NodeExistsException e) {
    } catch (KeeperException e) {
      LOG.error(e.toString(), e);
    } catch (InterruptedException e) {
    }

    try {
      LOG.info("Add Watcher:" + lookupPath);
      synchronized (servers) {
        List<String> oldServers = this.servers;
        this.servers = zk.getChildren(lookupPath, this);
        fireServiceChanged(oldServers, servers);
      }
    } catch (Exception e) {
      LOG.error(e.toString(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  private void fireServiceChanged(List<String> oldServers, List<String> newServers) {
    Set<String> oldServerSet = new HashSet<String>(oldServers);

    List<String> removedServers = new ArrayList<String>();
    List<String> addedServers = new ArrayList<String>();

    for (String eachServer : newServers) {
      if (!oldServerSet.remove(eachServer)) {
        addedServers.add(eachServer);
      }
    }

    removedServers.addAll(oldServerSet);
    serviceChangeListener.serverChanged(serviceName, newServers, addedServers, removedServers);
  }

  public List<String> getServers() {
    return servers;
  }

  private void serverChanged(WatchedEvent event) {
    if (event.getType() == Event.EventType.NodeCreated) {
      if(event.getPath().equals(lookupPath)) {
        try {
          zk.exists(lookupPath, this);
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
          this.servers = zk.getChildren(lookupPath, this);
          fireServiceChanged(oldServers, servers);
        } catch (Exception e) {
          LOG.error(event.toString(), e);
        }
      }
    }    
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() != Event.EventType.None) {
      serverChanged(event);
    }
  }
}
