package com.gruter.common.server.thrift;

import java.io.IOException;

@SuppressWarnings("serial")
public class NoServerException extends IOException {
  String serviceName;
  public NoServerException(String serviceName, String message) {
    super(message);
    this.serviceName = serviceName;
  }
}
