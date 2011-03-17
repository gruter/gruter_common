package com.gruter.common.server.thrift;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.gruter.common.util.StringUtils;

/**
 * Thrift로 만든 서버에 접속할 Connection getThriftServiceClient()에 반환되는 객체를 원하는 Thrift
 * Client로 타입 캐스팅해서 사용한다.
 * 
 * @author babokim
 * 
 */
@SuppressWarnings("unchecked")
public class ThriftConnection {
  public static final int MAX_RPC_LENGTH = 10 * 1024 * 1024;
  
  static Log LOG = LogFactory.getLog(ThriftConnection.class);
  protected String connId;
  protected TTransport transport;
  protected TProtocol protocol;
  protected String serverIpPort;
  protected Object thriftServiceClient;
  protected Class thriftClientClass;

  protected Boolean closed = new Boolean(false);
  
  public ThriftConnection(String connId, String serverIpPort, 
      Class thriftClientClass, int timeout) throws IOException {
    this.connId = connId;
    this.serverIpPort = serverIpPort;

    this.transport = new TFramedTransport(
        new TSocket(StringUtils.getHostName(serverIpPort), StringUtils.getPort(serverIpPort), timeout), MAX_RPC_LENGTH);
//    this.transport = new TSocket(StringUtils.getHostName(serverIpPort), StringUtils.getPort(serverIpPort), timeout);

    this.protocol = new TBinaryProtocol(transport);

    this.thriftClientClass = thriftClientClass;
    try {
      Constructor constructor = thriftClientClass.getConstructor(TProtocol.class);
      thriftServiceClient = constructor.newInstance(protocol);
      transport.open();
//      LOG.info("transport opened:" + serverIpPort + ":" + connId + ":" + timeout);
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  public Object getThriftServiceClient() {
    synchronized(closed) {
      if(closed) {
        return null;
      }
      return this.thriftServiceClient;
    }
  }

  public synchronized void close() throws IOException {
//    LOG.info("Close:" + connId + "," + serverIpPort);
    synchronized(closed) {
      if(closed) {
        return;
      }
      closed = true;
      transport.close();
    }
  }

  public boolean isClosed() {
    synchronized(closed) {
      return closed;
    }
  }
  
  public TProtocol getProtocol() {
    return this.protocol;
  }

  public String getServerIpPort() {
    return this.serverIpPort;
  }
}
