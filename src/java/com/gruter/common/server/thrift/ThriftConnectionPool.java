package com.gruter.common.server.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPoolFactory;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.ZooKeeper;

import com.gruter.common.server.service.AbstractServiceLocator;
import com.gruter.common.server.service.ServiceChangeListener;

/**
 * Thrift로 만든 특정 서비스에 대한 Connection Pool
 * @author babokim
 *
 */
public class ThriftConnectionPool implements ServiceChangeListener {
  static final Logger LOG = Logger.getLogger(ThriftConnectionPool.class);

  public enum ExhaustedAction {
    WHEN_EXHAUSTED_FAIL, WHEN_EXHAUSTED_GROW, WHEN_EXHAUSTED_BLOCK
  }

  public static final int DEFAULT_MAX_ACTIVE = 50;

  public static final int DEFAULT_MAX_IDLE = 10;

  public static final ExhaustedAction DEFAULT_EXHAUSTED_ACTION = ExhaustedAction.WHEN_EXHAUSTED_BLOCK;

  /**
   * the default max wait time when exhausted happened, default value is 1
   * minute
   */
  public static final long DEFAULT_MAX_WAITTIME_WHEN_EXHAUSTED = 10 * 1000;

  private Map<String, GenericObjectPool> pools = new HashMap<String, GenericObjectPool>();
  
  private List<String> servers;
  
  private int maxActive;

  private int maxIdle;
  
  private long maxWait;

  private ExhaustedAction exhaustedAction;

  private long maxWaitWhenBlockByExhausted;

  private Random rand = new Random();
  
  @SuppressWarnings("unchecked")
  private Class thriftClientClass;
  
  private int timeout;
  
  private AbstractServiceLocator serviceLocator;
  
  //hostIpPort -> ThriftConnections
  private Map<String, HashSet<ThriftConnection>> activeConnections = new HashMap<String, HashSet<ThriftConnection>>();
  
  @SuppressWarnings("unchecked")
  protected ThriftConnectionPool(AbstractServiceLocator serviceLocator, Class thriftClientClass)  throws IOException {
    this(serviceLocator, thriftClientClass, DEFAULT_MAX_ACTIVE, DEFAULT_EXHAUSTED_ACTION,
        DEFAULT_MAX_WAITTIME_WHEN_EXHAUSTED, DEFAULT_MAX_IDLE);
  }

  @SuppressWarnings("unchecked")
  protected ThriftConnectionPool(AbstractServiceLocator serviceLocator, Class thriftClientClass, int maxActive,
      ExhaustedAction exhaustedAction, long maxWait, int maxIdle) throws IOException {
    this.serviceLocator = serviceLocator;
    this.maxActive = maxActive;
    this.exhaustedAction = exhaustedAction;
    this.maxWaitWhenBlockByExhausted = maxWait;
    this.maxIdle = maxIdle;
    this.maxWait = maxWait;
    this.thriftClientClass = thriftClientClass;
    this.timeout = serviceLocator.getTimeout();
    
    synchronized(pools) {
      serviceLocator.setServiceChangeListener(this);
      this.servers = serviceLocator.getServers();
      
      if(servers != null) { 
        //LOG.info("server changed event:" + servers.size());
        for(String eachServer: servers) {
          //LOG.info("make pool for each server:" + eachServer);
          GenericObjectPoolFactory poolfactory = new GenericObjectPoolFactory(
              new PoolableClientFactory(eachServer), maxActive,
              getObjectPoolExhaustedAction(exhaustedAction), maxWait, maxIdle);
          pools.put(eachServer, (GenericObjectPool) poolfactory.createPool());
        }
      } else {
        LOG.info("No Live server");
      }
    }
  }

  public ZooKeeper getZooKeeper() {
    return serviceLocator.getZk();
  }

  @Override
  public void serverChanged(String serviceName, List<String> allServers, 
      List<String> addedServers, List<String> removedServers) {
    synchronized(pools) {
      if(servers == null) {
        servers = new ArrayList<String>(allServers);
        for(String eachServer: servers) {
          GenericObjectPoolFactory poolfactory = new GenericObjectPoolFactory(
              new PoolableClientFactory(eachServer), maxActive,
              getObjectPoolExhaustedAction(exhaustedAction), maxWait, maxIdle);
          pools.put(eachServer, (GenericObjectPool) poolfactory.createPool());
        }
        return;
      }
      
      servers.clear();
      servers.addAll(allServers);
      
      for(String eachServer: addedServers) {
        synchronized(pools) {
          if(!pools.containsKey(eachServer)) {
            GenericObjectPoolFactory poolfactory = new GenericObjectPoolFactory(
                new PoolableClientFactory(eachServer), maxActive,
                getObjectPoolExhaustedAction(exhaustedAction), maxWait, maxIdle);
            pools.put(eachServer, (GenericObjectPool) poolfactory.createPool());
            //LOG.info("server " + eachServer + " added to pool");
          }
        }
      }
      
      for(String eachServer: removedServers) {
        //LOG.info("server " + eachServer + " removed from pool");
        //현재 접속되어 처리 중인 접속도 종료 시킨다.
        synchronized(activeConnections) {
          HashSet<ThriftConnection> activeConns = activeConnections.remove(eachServer);
          if(activeConns != null) {
            for(ThriftConnection eachConn: activeConns) {
              try {
                eachConn.close();
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
            }
            activeConns.clear();
          }
        }
        //Pool내에 관리하고 있는 Connection 종료
        synchronized(pools) {
          GenericObjectPool pool = pools.remove(eachServer);
          if(pool != null) {
            try {
              pool.close();
            } catch (Exception e) {
              LOG.error("Pool close error: " + eachServer, e);
            }
          }
        }
      }
    }
  }
  
//  public void removeFromPool(ThriftConnection connection) {
//    try {
//      if (connection == null) {
//        return;
//      }
//      connection.close();
//      synchronized(pools) {
//        servers.remove(connection.getServerIp());
//        GenericObjectPool pool = pools.remove(connection.getServerIp());
//        if (pool != null) {
//          pool.close();
//        }
//      }
//    } catch (Exception e) {
//      LOG.error("removeFromPool error", e);
//    }
//  }
  
  public void close() {
    LOG.info("Pool Closed");
    try {
      synchronized(activeConnections) {
        for(HashSet<ThriftConnection> activeConns: activeConnections.values()) {
          for(ThriftConnection eachConn: activeConns) {
            try {
              eachConn.close();
            } catch (IOException e) {
              LOG.error(e.getMessage(), e);
            }
          }
          activeConns.clear();
        }
        activeConnections.clear();
      }
      
      synchronized(pools) {
        for(GenericObjectPool eachPool: pools.values()) {
          eachPool.close();
        }
        pools.clear();
      }
    } catch (Exception e) {
      LOG.error("close client pool error", e);
    }
  }

  public int getAvailableNum() {
    int numIdle = 0;
    for(GenericObjectPool eachPool: pools.values()) {
      numIdle += eachPool.getNumIdle();
    }
    return numIdle;
  }

  private GenericObjectPool getRandomPool() throws Exception {
    synchronized(pools) {
      if(pools.isEmpty()) {
        throw new NoServerException(serviceLocator.getSerivceName(), "No Live server(pool is empty)");
      }
      if(servers.isEmpty()) {
        throw new NoServerException(serviceLocator.getSerivceName(), "No Live server(ServerList is empty)");
      }
      
      //TODO Server의 Connection 갯수를 보고 판단, 현재는 랜덤
      return pools.get(servers.get(rand.nextInt(servers.size())));
    }
  }
  
  /**
   * 해당 서비스내에 임의의 서버에 대한 연결 정보를 풀에서 가져온다.
   * 서버 선택은 랜덤하게 가져온다.
   * @return
   * @throws IOException
   */
  public ThriftConnection getConnection() throws IOException {
    try {
      ThriftConnection conn = (ThriftConnection)getRandomPool().borrowObject();
      addToActiveConnections(conn.getServerIpPort(), conn);
      return conn;
    } catch (NoSuchElementException e) {
      throw new NoServerException(serviceLocator.getSerivceName(), "No Live server");
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * 특정 서버에 대한 연결 정보를 풀에서 가져온다.
   * @param hostIpPort
   * @return
   * @throws IOException
   */
  public ThriftConnection getConnection(String hostIpPort) throws IOException {
    try {
      GenericObjectPool pool = null;
      synchronized(pools) {
        pool = pools.get(hostIpPort);
        if(pool == null) {
          throw new NoServerException(serviceLocator.getSerivceName(), "No Live server(pool is empty):" + hostIpPort);
        }
      }
      ThriftConnection conn =  (ThriftConnection)pool.borrowObject();
      addToActiveConnections(hostIpPort, conn);
      //LOG.info("getConnection:" + hostIpPort + ":" + conn.connId);
      return conn;
    } catch (NoSuchElementException e) {
      throw new NoServerException(serviceLocator.getSerivceName(), "No Live server(pool is empty):" + hostIpPort);
    } catch(IOException e) {
      throw e; 
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * 사용되고 있는 Connection 정보를 관리하기 위한 목적
   * 서버 장애 상황을 감지할 경우 현재 사용되고 있는 연결도 close 시키기 위함 
   * @param hostIpPort
   * @param conn
   */
  private void addToActiveConnections(String hostIpPort, ThriftConnection conn) {
    if(conn != null) {
      synchronized(activeConnections) {
        HashSet<ThriftConnection> activeConns = activeConnections.get(hostIpPort);
        if(activeConns == null) {
          activeConns = new HashSet<ThriftConnection>();
          activeConnections.put(hostIpPort, activeConns);
        }
        activeConns.add(conn);
      }
    }
  }
  
  public int getActiveNum() {
    int numActive = 0;
    for(GenericObjectPool eachPool: pools.values()) {
      numActive += eachPool.getNumIdle();
    }
    return numActive;
  }

  /**
   * 사용한 Connection을 pool로 반환한다.
   * 연결은 close되지 않고 pool로 반환되어 재사용 된다. 
   * @param connection
   */
  public void releaseConnection(ThriftConnection connection) {
    synchronized(pools) {
      GenericObjectPool pool = pools.get(connection.getServerIpPort());
      //LOG.info("releaseConnection:" + connection.getServerIpPort() + ":" + connection.connId);
      if(pool != null) {
        try {
          pool.returnObject(connection);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          //throw new IOException(e.getMessage(), e);
        }
      }
      synchronized(activeConnections) {
        HashSet<ThriftConnection> activeConns = activeConnections.get(connection.getServerIpPort());
        if(activeConns != null) {
          activeConns.remove(connection);
        }
      }
    }
  }

  public class PoolableClientFactory extends BasePoolableObjectFactory
      implements PoolableObjectFactory {
    private String server;
    
    public PoolableClientFactory(String server) {
      this.server = server;
    }
    @Override
    public void destroyObject(Object obj) throws Exception {
      if(obj == null) {
        return;
      }
      ThriftConnection conn = (ThriftConnection)obj;
//      LOG.info("destory connection:" + conn.getServerIpPort() + "," + conn.connId);
      conn.close();
    }

    @Override
    public Object makeObject() throws Exception {
      try {
        return createConnection(server);
      } catch (TTransportException e) {
        LOG.error("create client error:", e);
        throw e;
      } catch (TException e) {
        LOG.error("create client error:", e);
        throw e;
      }
    }

    @Override
    public boolean validateObject(Object obj) {
      //TODO Check ZK Server Dir
      return true;
    }
  }

  public int getMaxActive() {
    return maxActive;
  }

  public void setMaxActive(int maxActive) {
    this.maxActive = maxActive;
    for(GenericObjectPool eachPool: pools.values()) {
      eachPool.setMaxActive(maxActive);
    }
  }

  public int getMaxIdle() {
    return maxIdle;
  }

  public void setMaxIdle(int maxIdle) {
    this.maxIdle = maxIdle;
    for(GenericObjectPool eachPool: pools.values()) {
      eachPool.setMaxIdle(maxIdle);
    }
  }

  public long getMaxWaitWhenBlockByExhausted() {
    return maxWaitWhenBlockByExhausted;
  }

  public void setMaxWaitWhenBlockByExhausted(long maxWaitWhenBlockByExhausted) {
    this.maxWaitWhenBlockByExhausted = maxWaitWhenBlockByExhausted;
    for(GenericObjectPool eachPool: pools.values()) {
      eachPool.setMaxWait(maxWaitWhenBlockByExhausted);
    }
  }

  private ThriftConnection createConnection(String serverIpPort) throws Exception {
    if(serverIpPort.indexOf(":") <= 0) {
      throw new Exception("Invalid host [" + serverIpPort + "]");
    }
    
    String connId = "" + (System.currentTimeMillis() + rand.nextInt());
//    LOG.info("Created connection:" + serverIpPort + ":connId=" + connId);
    return new ThriftConnection(connId, serverIpPort, thriftClientClass, timeout);
  }

  public void closeConnection(ThriftConnection conn) throws Exception {
    if(conn == null) {
      return;
    }
    synchronized(pools) {
      GenericObjectPool pool = pools.get(conn.getServerIpPort());
      if(pool != null) {
        pool.invalidateObject(conn);
      }
    }
  }
  
  public static byte getObjectPoolExhaustedAction(
      ExhaustedAction exhaustedAction) {
    switch (exhaustedAction) {
    case WHEN_EXHAUSTED_FAIL:
      return GenericObjectPool.WHEN_EXHAUSTED_FAIL;
    case WHEN_EXHAUSTED_BLOCK:
      //LOG.info("Pool.WHEN_EXHAUSTED_BLOCK");
      return GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    case WHEN_EXHAUSTED_GROW:
      return GenericObjectPool.WHEN_EXHAUSTED_GROW;
    default:
      return GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    }
  }
}
