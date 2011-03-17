package com.gruter.common.server.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;

import com.gruter.common.conf.GruterConf;
import com.gruter.common.zk.ZKUtil;

public class HAServerLocator extends AbstractServiceLocator {
  private static final Logger LOG = Logger.getLogger(HAServerLocator.class);

  protected String lookupPath;
  
  public HAServerLocator(GruterConf conf, String serviceName) throws IOException {
    super(conf, serviceName, 10 * 1000);
  }
  
  @Override
  public void initServiceServers(Watcher watcher) throws IOException {
    this.lookupPath = ZKUtil.getServiceMasterDir(conf, serviceName);
    try {
      this.servers.clear();
      byte[] masterServer = zk.getData(lookupPath, watcher, new Stat());
      if (masterServer != null) {
        servers.add(new String(masterServer));
      }
    } catch (KeeperException.NoNodeException e) {
    } catch (Exception e) {
      LOG.error(e.toString(), e);
      throw new IOException(e.getMessage(), e);
    }
  }
  
  @Override
  public void processEvent(WatchedEvent event, Watcher watcher) {
    if (event.getType() == Event.EventType.NodeCreated ||
        event.getType() == Event.EventType.NodeDataChanged) {
      if(event.getPath().equals(lookupPath)) {
        try {
          List<String> oldServers = new ArrayList<String>(this.servers);
          
          servers.clear();
          byte[] masterServer = zk.getData(lookupPath, watcher, new Stat());
          if (masterServer != null) {
            servers.add(new String(masterServer));
          }
          serviceChangeListener.serverChanged(serviceName, new ArrayList<String>(servers), new ArrayList<String>(servers), oldServers);
        } catch (Exception e) {
          LOG.error(e.toString(), e);
        }
      }
    } 
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
      
      return servers.get(0);
    }
  }
}
