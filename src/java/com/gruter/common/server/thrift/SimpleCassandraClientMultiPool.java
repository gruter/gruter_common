package com.gruter.common.server.thrift;

import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.gruter.common.server.thrift.ThriftConnectionPool.ExhaustedAction;

public class SimpleCassandraClientMultiPool {

  Logger logger = Logger.getLogger(this.getClass());

  /**
   * default max active number is 50, because Cassandra have multiple servers so
   * 50 should be a good value.
   */
  public static final int DEFAULT_MAX_ACTIVE = 50;

  /**
   * default max idle number is 5, so when the client keep idle, the total
   * connection number will release to 5
   */
  public static final int DEFAULT_MAX_IDLE = 5;

  /**
   * default exhausted action is block for 1 minute, because in many time,
   * cassandra was being used in a web backed, so too long time block will cause
   * very bad user experience. for some other non-synchronize application, this
   * value can configure to a more long time.
   */
  public static final ExhaustedAction DEFAULT_EXHAUSTED_ACTION = ExhaustedAction.WHEN_EXHAUSTED_BLOCK;

  /**
   * the default max wait time when exhausted happened, default value is 1
   * minute
   */
  public static final long DEFAULT_MAX_WAITTIME_WHEN_EXHAUSTED = 1 * 60 * 1000;

  /**
   * the object pool, all client instance managed by this pool
   */
  GenericKeyedObjectPoolFactory _poolfactory = null;
  GenericKeyedObjectPool _pool = null;
  KeyedPoolableObjectFactory _clientfactory = null;

  /**
   * a simple wrap class for cassandra cluster
   * 
   * @author sanli
   */
  public static class CassandraCluster {
    public CassandraCluster(String serviceURL, int port) {
      this.serviceURL = serviceURL;
      this.port = port;
    }

    String serviceURL;
    int port;
  }

  /**
   * Constructor for a multi pool, init parameter is a map from each key to
   * serviceURL and port. as bellow: { pool1 :
   * 
   * @param serviceURL
   * @param port
   */
  public SimpleCassandraClientMultiPool(Map<String, CassandraCluster> clusters) {
    this(clusters, null, DEFAULT_MAX_ACTIVE, DEFAULT_EXHAUSTED_ACTION, DEFAULT_MAX_WAITTIME_WHEN_EXHAUSTED,
        DEFAULT_MAX_IDLE);
  }

  public void close() {
    try {
      _pool.close();
    } catch (Exception e) {
      logger.error("close client pool error", e);
    }
  }

  public int getAvailableNum() {
    return _pool.getNumIdle();
  }

  public ThriftConnection getClient(String key) throws Exception, NoSuchElementException, IllegalStateException {
    ThriftConnection cc = (ThriftConnection) _pool.borrowObject(key);
    return cc;
  }

  public int getUsingNum() {
    return 0;
  }

  public int getActiveNum() {
    return _pool.getNumActive();
  }

  public void releaseClient(String key, ThriftConnection client) throws Exception {
    _pool.returnObject(key, client);
  }

  /**
   * inner class for create and destory Cassandra.Client
   * 
   * @author dayong
   */
  public class DefaultKeyedPoolableObjectFactory extends BaseKeyedPoolableObjectFactory implements
      KeyedPoolableObjectFactory {

    @Override
    public void destroyObject(Object key, Object obj) throws Exception {
      if (!clusters.containsKey(key)) {
        throw new IllegalArgumentException("invalide cluster name:" + key);
      }

      ThriftConnection client = (ThriftConnection) obj;
      if (logger.isDebugEnabled())
        logger.debug("close client " + client.toString());

      closeClient(client);

    }

    /**
     * do a simple Cassandra request
     */
    @Override
    public boolean validateObject(Object key, Object obj) {
      return validateClient((ThriftConnection) obj);
    }

    @Override
    public Object makeObject(Object key) throws Exception {
      if (!clusters.containsKey(key)) {
        throw new IllegalArgumentException("invalide cluster name:" + key);
      }
      CassandraCluster cc = clusters.get(key);

      if (logger.isDebugEnabled())
        logger.debug("create cassandra client [" + cc.serviceURL + ":" + cc.port + "]");

      try {
        return createClient(cc.serviceURL, cc.port);
      } catch (TTransportException e) {
        logger.error("create client error:", e);
        throw e;
      } catch (TException e) {
        logger.error("create client error:", e);
        throw e;
      }
    }

  }

  public int getMaxActive() {
    return maxActive;
  }

  public void setMaxActive(int maxActive) {
    this.maxActive = maxActive;
    _pool.setMaxActive(maxActive);
  }

  public int getMaxIdle() {
    return maxIdle;
  }

  public void setMaxIdle(int maxIdle) {
    this.maxIdle = maxIdle;
    _pool.setMaxIdle(maxIdle);
  }

  public long getMaxWaitWhenBlockByExhausted() {
    return maxWaitWhenBlockByExhausted;
  }

  public void setMaxWaitWhenBlockByExhausted(long maxWaitWhenBlockByExhausted) {
    this.maxWaitWhenBlockByExhausted = maxWaitWhenBlockByExhausted;
    _pool.setMaxWait(maxWaitWhenBlockByExhausted);
  }

  // ************************************ protected method
  // ***********************************

  /**
   * this construct method wamaxWaitWhenBlockExhausteds for unit test, for
   * regulary usage, should not given the client factory
   */
  protected SimpleCassandraClientMultiPool(Map<String, CassandraCluster> clusters,
      KeyedPoolableObjectFactory clientfactory, int maxActive, ExhaustedAction exhaustedAction, long maxWait,
      int maxIdle) {
    this.maxActive = maxActive;
    this.exhaustedAction = exhaustedAction;
    this.maxWaitWhenBlockByExhausted = maxWait;
    this.maxIdle = maxIdle;
    this.clusters = clusters;

    // if not give a client factory, will create the default
    // PoolableClientFactory
    if (clientfactory == null) {
      this._clientfactory = new DefaultKeyedPoolableObjectFactory();
      _poolfactory = new GenericKeyedObjectPoolFactory(_clientfactory, maxActive, getObjectPoolExhaustedAction(exhaustedAction), maxWait, maxIdle);
    } else {
      _poolfactory = new GenericKeyedObjectPoolFactory(clientfactory, maxActive, getObjectPoolExhaustedAction(exhaustedAction), maxWait, maxIdle);
    }

    _pool = (GenericKeyedObjectPool) _poolfactory.createPool();
  }

  // ********************************** private method
  // *********************************

  private int maxActive;

  private int maxIdle;

  private ExhaustedAction exhaustedAction;

  private long maxWaitWhenBlockByExhausted;

  private Map<String, CassandraCluster> clusters;

  public Map<String, CassandraCluster> getClusters() {
    return clusters;
  }
  
  
    public static ThriftConnection createClient(String  serviceURL , int port ) throws TTransportException , TException {

      TTransport tr = new TSocket( serviceURL, port );
      TProtocol proto = new TBinaryProtocol(tr);
//      Cassandra.Client client = new Cassandra.Client(proto);
//      tr.open();
//      
//      CassandraClientImpl cclient = new CassandraClientImpl(client) ;
//      cclient.init() ;
      return null ;
    
  }
  
  
  public static void closeClient(ThriftConnection cclient) {
    try {
      cclient.close();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  
  public static boolean validateClient(ThriftConnection client){
    //TODO send simple reqesut to cassandra, this request 
    // should very quickly and light
    return true ;
  }
  
  public static byte getObjectPoolExhaustedAction(ExhaustedAction exhaustedAction){
    switch(exhaustedAction){
      case WHEN_EXHAUSTED_FAIL :
        return GenericObjectPool.WHEN_EXHAUSTED_FAIL ;
      case WHEN_EXHAUSTED_BLOCK :
        return GenericObjectPool.WHEN_EXHAUSTED_BLOCK ;
      case WHEN_EXHAUSTED_GROW :
        return GenericObjectPool.WHEN_EXHAUSTED_GROW ;
      default:
        return GenericObjectPool.WHEN_EXHAUSTED_BLOCK ;
    }
  }
}
