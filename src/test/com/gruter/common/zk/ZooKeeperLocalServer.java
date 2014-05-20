package com.gruter.common.zk;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;

/**
 * zookeeper 는 잘 몰라서 제대로 짠 걸까요. :-)
 * @author Daegeun Kim
 */
public class ZooKeeperLocalServer extends ZooKeeperServerMain {
	private File tempDir = null;
	
	public void run(int clientPort) throws IOException {
		String tempdir = System.getProperty("java.io.tmpdir");
		tempDir = new File(tempdir, UUID.randomUUID().toString());
		tempDir.mkdirs();
		final ServerConfig config = new ServerConfig();
		config.parse(new String[] { "" + clientPort, tempDir.getAbsolutePath() });
		new Thread(new Runnable() {
			public void run() {
				try {
					runFromConfig(config);
				} catch (IOException e) {
				}
			}
		}).start();
	}
	
	@Override
	public void shutdown() {
		super.shutdown();
		tempDir.delete();
	}
}
