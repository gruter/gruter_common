package com.gruter.common.server;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import com.gruter.common.conf.GruterConf;
import com.gruter.common.zk.ZKUtil;
import com.gruter.common.zk.ZKUtil.LockStatus;

public abstract class MasterStandbyServer extends ZKManagedServer {
  static final Log LOG = LogFactory.getLog(MasterStandbyServer.class);

  protected Object monitor = new Object();
  protected LockStatus lockStatus;
  
  protected DisconnectedTimeoutHandler timeoutHandler;
  
  /**
   * 마스터 락을 가져온 서버는 masterInit()을 수행한 다음 마스터 역할을 수행한다.
   * 주로 소켓 서버 구성 및 필요 정보 로딩 등의 작업을 수행한다.
   * @throws IOException
   */
  abstract protected void masterInit() throws IOException;

  /**
   * ZK Disconnect 상태가 Session Time보다 큰 경우에 대한 처리 로직
   * ZK Server에서는 Session이 expire 되었기 때문에 Standby 서버가 마스터 역할을 수행하고 있다.
   * 따라서 기존 서버는 수행하던 작업을 중지하는 등의 처리를 해야 한다. 
   * @throws IOException
   */
  abstract protected void doZKSessionTimeout() throws IOException;
  
  public MasterStandbyServer(GruterConf conf) throws IOException {
    super(conf);
    timeoutHandler = new DisconnectedTimeoutHandler(zkTimeout);

    synchronized(timeoutHandler) {
      try {
        if(timeoutHandler.isDisconnedted()) {
          timeoutHandler.wait();
        }
      } catch (InterruptedException e) {
      }
    }
    
    if(timeoutHandler.isDisconnedted()) {
      System.out.println("Can't connect zk server[" + zkServers + "]. Server shutdowned");
      System.exit(0);
    }
    
    timeoutHandler.start();
  }

  protected void getMasterLock() throws IOException {
    String lockDir = ZKUtil.getServiceMasterDir(conf, getServiceName());
    try {
      ZKUtil.createNode(zk, lockDir, (hostIp + ":" + getServerPort()).getBytes(), 
          ZKUtil.ZK_ACL, CreateMode.PERSISTENT, true);
    } catch (KeeperException.NodeExistsException e) {
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
    synchronized(monitor) {
      try {
        lockStatus = ZKUtil.acquireLock(zk, lockDir, lockStatus);
        if(lockStatus.isAcquired()) {
          LOG.info(hostIp + ":" + getServerPort() + " get Master lock");
          masterInit();
          updateMasterServer();
        } else {
          LOG.info(hostIp + ":" + getServerPort() + " no Master lock");
          zk.getChildren(lockDir, this);
        }
      } catch (Exception e) {
        LOG.error(e);
        throw new IOException(e);
      }
    }
  }
  
  protected void updateMasterServer() throws IOException {
    String lockDir = ZKUtil.getServiceMasterDir(conf, getServiceName());
    try {
      zk.setData(lockDir, (hostIp + ":" + getServerPort()).getBytes(), -1);
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }
  
  @Override
  protected void zkDisconnected() throws IOException {
    timeoutHandler.setDisconnected(true);
  }

  @Override
  protected void zkSessionExpired() throws IOException {
  }

  @Override
  protected void zkConnected() throws IOException {
    timeoutHandler.setDisconnected(false);
  }
  
  @Override
  protected void processNodeChangedEvent(WatchedEvent event) {
    if(event.getType() == Event.EventType.NodeChildrenChanged) {
      try {
        getMasterLock();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }      
  }
  
  class DisconnectedTimeoutHandler extends Thread {
    private boolean disconnected = false;
    private long lastDisconnectedTime;
    private long timeout;
    
    public DisconnectedTimeoutHandler(long timeout) {
      this.timeout = timeout;
    }

    public synchronized void setDisconnected(boolean disconnected) {
      if(!this.disconnected && disconnected) {
        this.lastDisconnectedTime = System.currentTimeMillis();;
      }
      this.disconnected = disconnected;
      synchronized(this) {
        this.notify();
      }
    }
    
    public boolean isDisconnedted() {
      return disconnected;
    }

    public void run() {
      while(true) {
        synchronized(this) {
          try {
            this.wait();
          } catch (InterruptedException e) {
          }
        }

        while(disconnected) {
          if(System.currentTimeMillis() - lastDisconnectedTime >= timeout) {
            //watcher.process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null));
            try {
              doZKSessionTimeout();
            } catch (IOException e) {
              LOG.error(e.getMessage(), e);
            }
            disconnected = false;
            lastDisconnectedTime = 0;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    }
  }
}
