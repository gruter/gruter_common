package com.gruter.common.zk;

import com.gruter.common.http.CommonHttpServer;
import com.gruter.common.http.ZKControllerServlet;


public class ZKManagerWebServer {
  public static void main(String[] args) throws Exception {
//    args = new String[]{"8091"};
    if(args.length < 1) {
      System.out.println("Usage: java ZKManagerWebServer <port>");
      System.exit(0);
    }
    CommonHttpServer webServer = new CommonHttpServer("common_webapps", "0.0.0.0", Integer.parseInt(args[0]), false);
    webServer.addServlet("zkmanager", "/zkmanager", ZKControllerServlet.class);
    webServer.start();
  }
}
