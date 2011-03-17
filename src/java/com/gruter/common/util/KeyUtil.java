package com.gruter.common.util;

import java.io.IOException;
import java.util.UUID;

import org.apache.zookeeper.ZooKeeper;

import com.gruter.common.zk.ZKKeyGen;

public class KeyUtil {
  public static long getSequenceKey(ZooKeeper zk, String serviceName) throws IOException {
    return ZKKeyGen.getInstance(zk).getNextSequence(serviceName);
  }
  
  public static String getUUID() {
    return UUID.randomUUID().toString();   
  }
  
//  public static void main(String[] args) {
//    System.out.println(">>>>" + getUUID());
//  }
}
