package com.gruter.common.zk;

import static org.hamcrest.CoreMatchers.is;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.gruter.common.http.CommonHttpServerTestCase;
import com.gruter.common.http.ZKControllerServlet;

/**
 * @author Daegeun Kim
 */
public class TestZKManagerWebServer extends CommonHttpServerTestCase {
	@Before
	public void before() throws Exception {
		initServer();
	}
	
	@After
	public void after() throws Exception {
		stopServer();
	}

	@Test
	public void testZookeeperServlet() throws Exception {
		ZooKeeperLocalServer zkserver = new ZooKeeperLocalServer();
		zkserver.run(21333);
		
		addServlet("zk-servlet", "/zookeeper", ZKControllerServlet.class);
		Page page = requestToServer("GET", "/zookeeper?action=GetZKNodeDetail&zkservers=127.0.0.1%3A21333&dir=%2F");

		zkserver.shutdown();
		Assert.assertThat(200, is(page.getWebResponse().getStatusCode()));
		//TODO: 임의의 znode 와 data 를 추가하고 넘겨받은 json 결과와 data 비교 검증 필요.
	}
}
