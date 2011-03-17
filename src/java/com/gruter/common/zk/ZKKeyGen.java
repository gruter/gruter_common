package com.gruter.common.zk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZKKeyGen {

  static final Log LOG = LogFactory.getLog(ZKUtil.class);

  public static final byte[] DEFAULT_DATA = ByteBuffer.allocate(8).putLong(-1).array();
  public static final List<ACL> DEFAULT_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

  private final ZooKeeper myZooKeeper;

  private final long retryDelay = 100l;
  private final int retryCount = 10;

  private static ZKKeyGen instance;
  
  private ZKKeyGen(ZooKeeper zk) {
    this.myZooKeeper = zk;
  }

  public synchronized static ZKKeyGen getInstance(ZooKeeper zk) {
    if(instance == null) {
      instance = new ZKKeyGen(zk);
    }
    
    return instance;
  }
  
  /**
   * 유일한 키 값을 반환한다.
   * ZooKeeper의 setData를 사용하는데 setData의 성능이 좋지 않아 10 ~ 15ms 정도 소요됨
   * @param key
   * @return
   * @throws IOException
   */
  public long getNextSequence(String key) throws IOException {
    if(!key.startsWith("/")) {
      key = "/" + key;
    }
    KeeperException mostRecentException = null;

    for (int i = 0; i < retryCount; i++) {
      try {
        return getAndIncrement(key, DEFAULT_DATA);
      } catch (KeeperException.SessionExpiredException e) {
        LOG.warn("Session expired for: " + myZooKeeper + " so reconnecting due to: " + e, e);
        throw new IOException("Session expired", e);
      } catch (KeeperException.ConnectionLossException e) {
        mostRecentException = e;
        LOG.debug("Attempt " + i + " failed with connection loss so " + "attempting to reconnect: " + e, e);
        retryWithDelay(i);
      } catch (KeeperException e) {
        LOG.error(String.format("Caught an unexpected error when " + "incrementing sequence for key %s", key), e);
        mostRecentException = e;
      }
    }
    throw new IOException(String.format("Failed to obtain next sequence for key %s", key), mostRecentException);
  }

  private long getAndIncrement(String key, byte[] initialValue) throws KeeperException {
    LOG.debug(String.format("Incrementing sequence for key %s", key));
    Stat stat = new Stat();
    boolean committed = false;
    long id = 0;
    while (!committed) {
      try {
        byte[] data = myZooKeeper.getData(key, false, stat);
        ByteBuffer buf = ByteBuffer.wrap(data);
        id = buf.getLong();
        buf.rewind();
        buf.putLong(++id);
        myZooKeeper.setData(key, buf.array(), stat.getVersion());
        committed = true;
      } catch (KeeperException.NoNodeException e) {
        createKey(key, DEFAULT_DATA);
        committed = false;
      } catch (KeeperException.BadVersionException e) {
        LOG.debug(String.format("Another client updated key %s, retrying", key));
        committed = false;
      } catch (InterruptedException e) {
        // at this point, we don't know that our update happened.
        // we will err on the side of caution and assume that it
        // didn't. In the worst case, we'll end up with a wasted
        // sequence number that we'll have to apply compensating
        // measures to deal with
        LOG.error(String.format("Unable to determine status counter increment for key %s. "
            + "This may result in unused sequences", key), e);
        committed = false;
      }
    }
    LOG.debug(String.format("Key:Seq => %s, %s", key, id));
    return id;
  }

  public void createKey(String key, byte[] initialValue) throws KeeperException {
    LOG.debug(String.format("Creating new node for key %s", key));
    try {
      ZKUtil.createNode(myZooKeeper, key, initialValue, DEFAULT_ACL, CreateMode.PERSISTENT, true);
    } catch (InterruptedException e) {
      LOG.error(String.format("Caught InterruptedException when creating key %s", key), e);
    } catch (KeeperException e) {
      if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
        LOG.info(String.format("Tried to create %s, but it already exists. " + "Probably a (harmless) race condition",
            key));
      } else {
        throw e;
      }
    }
  }

  public long setInitialValue(String key, long value) throws IOException {
    LOG.debug(String.format("Incrementing sequence for key %s", key));
    Stat stat = new Stat();
    boolean committed = false;
    long id = 0;
    while (!committed) {
      try {
        myZooKeeper.getData(key, false, stat);
        myZooKeeper.setData(key, ByteBuffer.allocate(8).putLong(value - 1).array(), stat.getVersion());
        committed = true;
      } catch (KeeperException.NoNodeException e) {
        try {
          createKey(key, ByteBuffer.allocate(8).putLong(value - 1).array());
          committed = true;
        } catch (KeeperException e1) {
          LOG.error(e1.getMessage());
          committed = false;
        }
      } catch (KeeperException.BadVersionException e) {
        LOG.debug(String.format("Another client updated key %s, retrying", key));
        committed = false;
      } catch(KeeperException e) {
        throw new IOException(e.getMessage(), e);
      }  catch (InterruptedException e) {
        // at this point, we don't know that our update happened.
        // we will err on the side of caution and assume that it
        // didn't. In the worst case, we'll end up with a wasted
        // sequence number that we'll have to apply compensating
        // measures to deal with
        LOG.error(String.format("Unable to determine status counter increment for key %s. "
            + "This may result in unused sequences", key), e);
        committed = false;
      }
    }
    LOG.debug(String.format("Key:Seq => %s, %s", key, id));
    return id;
  }
  
  private void retryWithDelay(int attemptCount) {
    if (attemptCount > 0) {
      try {
        Thread.sleep(attemptCount * retryDelay);
      } catch (InterruptedException e) {
        LOG.debug("Failed to sleep: " + e, e);
      }
    }
  }
}
