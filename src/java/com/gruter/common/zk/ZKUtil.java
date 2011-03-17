package com.gruter.common.zk;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import com.gruter.common.conf.GruterConf;

public class ZKUtil {
  static final Log LOG = LogFactory.getLog(ZKUtil.class);

  public static final String ZK_SERVICES_PATH = "/services";
  public static final String ZK_LIVE_SERVERS_PATH = "/live_servers";
  public static final String ZK_MASTER_PATH = "/master";

  public static ArrayList<ACL> ZK_ACL = new ArrayList<ACL>();
  static {
    // ZK_ACL.add(new ACL(Perms.WRITE | Perms.CREATE | Perms.DELETE |
    // Perms.ADMIN | Perms.ADMIN, Ids.AUTH_IDS));
    // ZK_ACL.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE));
    ZK_ACL.addAll(Ids.OPEN_ACL_UNSAFE);
  }

  public static ZooKeeper connectZK(GruterConf conf, String serviceName, Watcher watcher) throws IOException {
    String zkServers = conf.get("zk.servers");
    
    if(zkServers == null) {
      throw new IOException("No zk.servers proeprty in gruter-common-default.xml or gruter-common-site.xml");
    }
    InetAddress localAddress = InetAddress.getLocalHost();
    String hostIp = localAddress.getHostAddress();
    String hostName = localAddress.getHostName();
    
    String zkUserId = conf.get("zk.service." + serviceName + ".userId", "client");
    String zkPasswd = conf.get("zk.service." + serviceName + ".password", "client");
    
    int zkTimeout = conf.getInt("zk.session.timeout", 10 * 1000);
    ZooKeeper zk = new ZooKeeper(zkServers, zkTimeout, watcher == null ? new DefaultWatcher() : watcher);
    zk.addAuthInfo("digest", (zkUserId + ":" + zkPasswd).getBytes());
    
    return zk;
  }
  /**
   * 첫번째와 두번째 파라미터의 Collection을 비교하여 첫번째에서 삭제된 객체, 두번째에 추가된 객체를 찾는다. 
   * @param origin
   * @param current
   * @param added
   * @param removed
   */
  public static void compareCollection(List<String> origin, List<String> current,
      List<String> added, List<String> removed) {
    if (origin == null) {
      if (current != null) {
        added.addAll(current);
      }
      return;
    }
    
    if (current == null) {
      if (origin != null) {
        removed.addAll(origin);
      }
      return;
    }
    
    Set<String> originSet = new HashSet<String>(origin);
    
    for(String eachValue: current) {
      if(!originSet.remove(eachValue)) {
        added.add(eachValue);
      }
    }
    
    removed.addAll(originSet);
  }
  
  public static String getPathName(String path) {
    int index = path.lastIndexOf("/");
    if (index <= 0) {
      return path;
    }
    
    return path.substring(index + 1);
  }
  
  public static String getParentPath(String path) {
    int index = path.lastIndexOf("/");
    if (index <= 0) {
      return path;
    }
    
    return path.substring(0, index);
  }
  
  public static String getServiceMasterDir(GruterConf conf, String serviceName) {
    return getServicePath(conf, serviceName) + ZK_MASTER_PATH;
  }

  public static String getServiceLiveServerPath(GruterConf conf, String serviceName) {
    return getServicePath(conf, serviceName) + ZK_LIVE_SERVERS_PATH;
  }
  
  public static String getServicePath(GruterConf conf, String serviceName) {
    return getServiceRootPath(conf) + "/" + serviceName;
  }

  public static String getServiceRootPath(GruterConf conf) {
    return conf.get("zk.service.root", "/default") + ZK_SERVICES_PATH;
  }

  public static String createNode(ZooKeeper zk, String path, byte[] data, List<ACL> acl, CreateMode createMode,
      boolean recursive) throws KeeperException, InterruptedException {
    LOG.debug("Create zk dir:" + path);

    if (!recursive) {
      return zk.create(path, data, acl, createMode);
    }

    String[] tokens = path.split("/");

    String currentPath = "/";
    for (int i = 0; i < tokens.length - 1; i++) {
      currentPath += tokens[i];
      try {
        zk.create(currentPath, null, acl, CreateMode.PERSISTENT);
      } catch (NodeExistsException e) {
      }
      if (tokens[i].length() > 0) {
        currentPath += "/";
      }
    }

    try {
      return zk.create(path, data, acl, createMode);
    } catch (KeeperException.NodeExistsException e) {
      return path;
    }
  }

  public static void delete(ZooKeeper zk, String path, boolean recursive) throws IOException {
    try {
      if (zk.exists(path, false) == null) {
        return;
      }
      
      List<String> children = null;
      try {
        children = zk.getChildren(path, false);
      } catch(NoNodeException e) {
      }
      
      if (recursive && children != null && children.size() > 0) {
        for (String child: children) {
          delete(zk, path + "/" + child, recursive);
        }
      }
      delete(zk, path);
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }
  
  public static void delete(ZooKeeper zk, String path) throws IOException {
    try {
      zk.delete(path, -1);
    } catch(NoNodeException e) {
      return;
    } catch (Exception e) {
      LOG.error("Delete error:" + path + "," + e.getMessage());
      throw new IOException(e);
    }
  }

  public static LockStatus acquireLock(ZooKeeper zk, String lockPath, LockStatus previousLockStatus) throws Exception {
    String lockKey = null;
    if (previousLockStatus == null) {
      String createdPath = zk.create(lockPath + "/Lock-", null, ZK_ACL, CreateMode.EPHEMERAL_SEQUENTIAL);
      String[] tokens = createdPath.split("/");

      lockKey = tokens[tokens.length - 1];
    } else {
      lockKey = previousLockStatus.getLockKey();
    }

    List<String> nodes = zk.getChildren(lockPath, false);
    if (nodes == null || nodes.size() == 0) {
      throw new IOException("No child node:" + lockPath);
    }

    Collections.sort(nodes);

    boolean acquired = false;
    if (nodes.get(0).equals(lockKey)) {
      acquired = true;
    } else {
      acquired = false;
    }

    return new LockStatus(acquired, lockKey);
  }

  public static void releaseLock(ZooKeeper zk, String lockPath, LockStatus lockStatus) throws Exception {
    try {
      zk.delete(lockPath + "/" + lockStatus.getLockKey(), -1);
    } catch (KeeperException.NoNodeException e) {
    }
  }

  public static class LockStatus {
    boolean acquired;
    String lockKey;

    public LockStatus(boolean acquired, String lockKey) {
      this.acquired = acquired;
      this.lockKey = lockKey;
    }

    public boolean isAcquired() {
      return acquired;
    }

    public String getLockKey() {
      return lockKey;
    }
  }
}
