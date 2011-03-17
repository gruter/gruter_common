package com.gruter.common.server.service;

import java.util.List;

public interface ServiceChangeListener {
  /**
   * 서버가 추가되거나 변경될 경우 이 메소드가 호출된다.
   * @param serviceName
   * @param allServers
   * @param addedServers
   * @param removedServers
   */
  public void serverChanged(String serviceName,
      List<String> allServers,
      List<String> addedServers, 
      List<String> removedServers); 
}
