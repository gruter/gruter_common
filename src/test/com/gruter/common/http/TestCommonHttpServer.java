package com.gruter.common.http;

import static org.hamcrest.CoreMatchers.is;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.zookeeper.server.ServerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.gruter.common.zk.ZooKeeperLocalServer;

/**
 * @author Daegeun Kim
 */
public class TestCommonHttpServer extends CommonHttpServerTestCase {
	public TestCommonHttpServer() {
		super("common_webapps");
	}
	
	@Before
	public void before() throws Exception {
		initServer();
	}
	
	@After
	public void after() throws Exception {
		stopServer();
	}

	@Test
	public void test200() throws Exception {
		Page page = requestToServer("GET", "/zktree.html");
		
		Assert.assertThat(200, is(page.getWebResponse().getStatusCode()));
	}
	
	@Test
	public void test404() throws Exception {
		Page page = requestToServer("GET", "/12312310293i81093jfpawejfiwoejfowaiej.html");
		
		Assert.assertThat(404, is(page.getWebResponse().getStatusCode()));
	}
	
	@Test
	public void testServlet() throws Exception {
		addServlet("test-servlet", "/test", TestServlet.class);
		Page page = requestToServer("GET", "/test");
		
		Assert.assertThat(200, is(page.getWebResponse().getStatusCode()));
	}
	
	@Test
	public void testFilter() throws Exception {
		addFilter("test-filter", TestFilter.class.getName(), null);
		Page page = requestToServer("GET", "/zktree.html");

		Assert.assertThat(200, is(page.getWebResponse().getStatusCode()));
		Assert.assertThat(TestFilter.class.getName(), is(getHeader(page, "X-Filter")));
	}
	
	@Test
	public void testGlobalFilter() throws Exception {
		addServlet("test-servlet", "/test", TestServlet.class);
		addGlobalFilter("test-filter", TestFilter.class.getName(), null);
		Page page = requestToServer("GET", "/test");
		
		Assert.assertThat(200, is(page.getWebResponse().getStatusCode()));
		Assert.assertThat(TestFilter.class.getName(), is(getHeader(page, "X-Filter")));
	}

	private String getHeader(Page page, String headerName) {
		return page.getWebResponse().getResponseHeaderValue(headerName);
	}
	
	/**
	 * 테스트용으로 사용할 Filter
	 */
	public static class TestFilter implements Filter {
		@Override
		public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
			((HttpServletResponse) response).addHeader("X-Filter", TestFilter.class.getName());
		}

		@Override
		public void destroy() {
		}

		@Override
		public void init(FilterConfig arg0) throws ServletException {
		}
	}

	/**
	 * 테스트용으로 사용할 Servlet
	 */
	public static class TestServlet extends HttpServlet {
		private static final long serialVersionUID = -1990697247757359833L;
		
		@Override
		protected void service(HttpServletRequest request, HttpServletResponse response)
				throws ServletException, IOException {
			response.setContentType("text/html; charset=UTF-8");
		}
	}
}
