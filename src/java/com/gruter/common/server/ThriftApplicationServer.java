package com.gruter.common.server;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.gruter.common.conf.GruterConf;

public abstract class ThriftApplicationServer extends ZKManagedServer {
  static final Log LOG = LogFactory.getLog(ThriftApplicationServer.class);
  
  protected TServer server;
  
  protected TNonblockingServerSocket serverSocket;
  
  protected abstract TProcessor getProcessor();
  
  public ThriftApplicationServer(GruterConf conf) throws IOException {
    super(conf);
  }
  
  @Override
  protected void stoppingServer(Object ... options) throws IOException {
    LOG.info("Thrift SocketServer stoped:" + getServerPort());
    server.stop();
  }
  
  @Override
  protected void setupServer() throws IOException {
//    try {
//      serverSocker = new TNonblockingServerSocket(getServerPort());
//    } catch (TTransportException e) {
//      throw new IOException(e.getMessage(), e);
//    }
//    
//    Options options = new Options();
//    options.maxReadBufferBytes = 100 * 1024;
//    options.workerThreads = conf.getInt("appserver.worker.num", 100);
//    options.stopTimeoutVal = 10 * 1000;
//    options.stopTimeoutUnit = TimeUnit.MILLISECONDS;
//  server = new THsHaServer(new TProcessorFactory(getProcessor()), serverSocker,
//  new TFramedTransport.Factory(), new TBinaryProtocol.Factory(), options);

    try {
      serverSocket = new TNonblockingServerSocket(getServerPort());
    } catch (TTransportException e) {
      throw new IOException(e.getMessage(), e);
    }
    Args args = new Args(serverSocket);
    args.workerThreads(conf.getInt("appserver.worker.num", 100));
    args.processor(getProcessor());
    args.inputProtocolFactory(new TBinaryProtocol.Factory());
    args.inputTransportFactory(new TFramedTransport.Factory());
    args.outputProtocolFactory(new TBinaryProtocol.Factory());
    args.outputTransportFactory(new TFramedTransport.Factory());
    
    server = new THsHaServer(args);
        
//    TThreadPoolServer.Options options = new TThreadPoolServer.Options();
//    options.minWorkerThreads = 10;
//    options.maxWorkerThreads = 100;
//    
//    TServerSocket serverSocket = new TServerSocket(new ServerSocket(getServerPort(), 10 * 1000));
//    
//    server = new TThreadPoolServer(getProcessor(), serverSocket, new TTransportFactory(), new TTransportFactory(),
//        new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);
  }
  
  @Override
  public void startServer() throws IOException {
    super.startServer();
    server.serve();
  }
  
  @Override
  protected void zkSessionExpired() {
  }
  
  @Override
  protected void zkDisconnected() {
  }
  
  @Override
  protected void zkConnected() throws IOException {
  }
}