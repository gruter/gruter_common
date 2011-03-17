package com.gruter.common.util;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Locker {
  public static final Log LOG = LogFactory.getLog(Locker.class.getName());
  
  private Integer mutex;
  
  private AtomicInteger lockers;

  private Set<LockInfo> lockOwners = new HashSet<LockInfo>();
  
  public Locker() {
    this.mutex = new Integer(0);
    this.lockers = new AtomicInteger(0);
  }

  public void obtainReadLock() {
    synchronized(mutex) {
      while(lockers.get() < 0) {
        try {
          mutex.wait();
        } catch(InterruptedException ie) {
        }
      }
      lockers.incrementAndGet();
      mutex.notifyAll();
    }
  }

  public void obtainReadLock(String owner) {
    synchronized(mutex) {
      while(lockers.get() < 0) {
        try {
          mutex.wait();
        } catch(InterruptedException ie) {
        }
      }
      lockers.incrementAndGet();
      lockOwners.add(new LockInfo(owner, true));
      mutex.notifyAll();
    }
  }

  public void releaseReadLock(String owner) {
    synchronized(mutex) {
      if(lockers.decrementAndGet() < 0) {
        throw new IllegalStateException("lockers: " + lockers);
      }
      lockOwners.remove(new LockInfo(owner, true));
      mutex.notifyAll();
    }
  }
  
  public void releaseReadLock() {
    synchronized(mutex) {
      if(lockers.decrementAndGet() < 0) {
        throw new IllegalStateException("lockers: " + lockers);
      }
      mutex.notifyAll();
    }
  }

  public void obtainWriteLock() {
    synchronized(mutex) {
      while(!lockers.compareAndSet(0, -1)) {
        try {
          mutex.wait();
        } catch (InterruptedException ie) {
        }
      }
      mutex.notifyAll();
    }
  }

  public void obtainWriteLock(String owner) {
    synchronized(mutex) {
      while(!lockers.compareAndSet(0, -1)) {
        try {
          mutex.wait();
        } catch (InterruptedException ie) {
        }
      }
      lockOwners.add(new LockInfo(owner, false));
      mutex.notifyAll();
    }
  }
  
  public void releaseWriteLock() {
    synchronized(mutex) {
      if(!lockers.compareAndSet(-1, 0)) {
        throw new IllegalStateException("lockers: " + lockers);
      }
      mutex.notifyAll();
    }
  }
  
  public void releaseWriteLock(String owner) {
    synchronized(mutex) {
      if(!lockers.compareAndSet(-1, 0)) {
        throw new IllegalStateException("lockers: " + lockers);
      }
      lockOwners.remove(new LockInfo(owner, false));
      mutex.notifyAll();
    }
  }

  public void printLockOwner() {
    synchronized(mutex) {
      for(LockInfo lockOwner: lockOwners) {
        LOG.warn("lock:" + lockOwner.toString());
      }
    }
  }
  
  static class LockInfo {
    String owner;
    boolean read;
    long lockTime;
    
    LockInfo(String owner, boolean read) {
      this.owner = owner;
      this.read = read;
      lockTime = System.currentTimeMillis();
    }
    
    public boolean equals(Object obj) {
      if( !(obj instanceof LockInfo) ) {
        return false;
      }
      
      LockInfo targer = (LockInfo)obj;
      return owner.equals(targer.owner) && read == targer.read;
    }
    
    public String toString() {
      return (new Date(lockTime)) + "," + owner + "," + (read ? "read" : "write");
    }
    
    public int hashCode() {
      return (owner + read).hashCode();
    }
  }
}
