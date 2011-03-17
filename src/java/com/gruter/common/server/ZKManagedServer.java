package com.gruter.common.server;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.gruter.common.conf.GruterConf;
import com.gruter.common.util.StringUtils;
import com.gruter.common.zk.ZKUtil;

public abstract class ZKManagedServer implements Watcher {
  static final Log LOG = LogFactory.getLog(ZKManagedServer.class);
  
  static final int ZK_DEFAULT_SESSION_TIMEOUT = 30 * 1000;
  
  protected GruterConf conf;
  protected ZooKeeper zk;
  protected String zkServers;
  protected int zkTimeout; 
  protected String hostIp;
  protected String hostName;

  private CountDownLatch latch = new CountDownLatch(1);
  /**
   * 서버 초기화 설정을 한다.(ZooKeeper 연결 등은 생성자에서 이미 진행됨
   * @throws IOException
   */
  abstract protected void setupServer() throws IOException;
  
  /**
   * 서버에서 제공하는 서비스의 명을 반환한다. 전체 시스템에서 유일해야 하며 ZooKeeper에서 디렉토리를
   * 생성하는 기본 정보가 된다.
   * @return
   */
  abstract public String getServiceName();
  
  /**
   * 서비스할 서버의 포트를 반환한다.
   * @return
   */
  abstract public int getServerPort();
  
  /**
   * ZooKeeper와의 세션이 끊어졌을 때 호출된다.
   */
  abstract protected void zkSessionExpired() throws IOException;

  /**
   * ZooKeeper 서버와 접속이 끊어졌을 때 호출된다. 
   */
  abstract protected void zkDisconnected() throws IOException;

  /**
   * ZooKeeper 서버와 접속이 되었을 때 호출된다.
   */
  abstract protected void zkConnected() throws IOException;

  abstract protected void stoppingServer(Object ... options) throws IOException;
  
  public ZKManagedServer(GruterConf conf) throws IOException {
    StringUtils.setExceptionHandler(LOG);
    this.conf = conf;
    this.zkServers = conf.get("zk.servers");
    
    if(zkServers == null) {
      LOG.fatal("Server shutdowned cause no zk.servers proeprty in conf/default.xml or site.xml");
      System.exit(0);
    }
    InetAddress localAddress = InetAddress.getLocalHost();
    this.hostIp = localAddress.getHostAddress();
    this.hostName = localAddress.getHostName();
    
    zkTimeout = conf.getInt("zk.session.timeout", ZK_DEFAULT_SESSION_TIMEOUT);
    connectZK();
  }

  public void stopServer(Object ... options) throws IOException {
    try {
      stoppingServer(options);
      zk.close();
      LOG.info("=============" + getServiceName() + "[" + hostIp + ":" + getServerPort() + "] STOP" + "=============");    
    } catch (InterruptedException e) {
    }
  }
  
  public void startServer() throws IOException {
    setupServer();
    LOG.info(getServiceName() + " server started on " + hostIp + ":" + getServerPort());    
  }
  
  protected void processNodeChangedEvent(WatchedEvent event) {
  }
  
  protected void connectZK() throws IOException {
    String zkUserId = conf.get("zk.service." + getServiceName() + ".userId");
    String zkPasswd = conf.get("zk.service." + getServiceName() + ".password");

    zk = new ZooKeeper(zkServers, zkTimeout, this);
    zk.addAuthInfo("digest", (zkUserId + ":" + zkPasswd).getBytes());
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  protected String getMembershipServerDir() {
    return ZKUtil.getServiceLiveServerPath(conf, getServiceName()) + "/" + hostIp + ":" + getServerPort();
  }
  
  protected void registerServer() throws IOException {
    String serverDir = getMembershipServerDir();
    try {
      LOG.info("create zknode " + serverDir + " for membership");
      ZKUtil.createNode(zk, serverDir, (hostName + ":" + getServerPort()).getBytes(), 
          ZKUtil.ZK_ACL, CreateMode.EPHEMERAL, true);
    } catch (Exception e) {
      throw new IOException("Can't create server dir:[" + serverDir + "]", e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if(event.getType() == Event.EventType.None) {
      switch (event.getState()) {
      case SyncConnected:
        try {
          registerServer();
          latch.countDown();
          zkConnected();
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
        break;
      case Disconnected:
        try {
          zkDisconnected();
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
        break;
      case Expired: 
        try {
          connectZK();
          zkSessionExpired();
        } catch (IOException e) {
          LOG.fatal(e.getMessage(), e);
          System.exit(0);
        }
        break;
      }
    } else {
      processNodeChangedEvent(event);
    }
  }
  
  public String getHostIp() {
    return hostIp;
  }

  public ZooKeeper getZk() {
    return zk;
  }

  public String getHostName() {
    return hostName;
  }

  public String getHostAddress() {
    return hostIp;
  }

  public GruterConf getConf() {
    return conf;
  }
}
