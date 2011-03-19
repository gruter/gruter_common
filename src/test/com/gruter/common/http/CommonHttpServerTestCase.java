package com.gruter.common.http;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServlet;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * @author Daegeun Kim
 */
public abstract class CommonHttpServerTestCase implements FilterContainer {
	private static final String DEFAULT_WEB_CONTEXT_PATH = "default_context_path";
	private static final String DEFAULT_HOSTNAME = "127.0.0.1";
	private static final int DEFAULT_PORT = 8100;
	
	private String webContextPath;
	protected String hostname;
	protected int port;
	protected CommonHttpServer server;
	private boolean running;

	protected CommonHttpServerTestCase() {
		this(DEFAULT_WEB_CONTEXT_PATH, DEFAULT_HOSTNAME, DEFAULT_PORT);
	}
	
	protected CommonHttpServerTestCase(String webContextPath) {
		this(webContextPath, DEFAULT_HOSTNAME, DEFAULT_PORT);
	}
	
	protected CommonHttpServerTestCase(String webContextPath, String hostname, int port) {
		this.webContextPath = webContextPath;
		this.hostname = hostname;
		this.port = port;
	}
	
	protected void initServer() throws IOException {
		if (server == null) {
			server = new CommonHttpServer(getWebContextPath(), getHostname(), getPort(), false);
		}
	}
	
	/**
	 * 서블릿 추가
	 * @param name
	 * @param pathSpec
	 * @param clazz
	 */
	public void addServlet(String name, String pathSpec, Class<? extends HttpServlet> clazz) {
		server.addServlet(name, pathSpec, clazz);
	}
	
	/* (non-Javadoc)
	 * @see com.gruter.common.http.FilterContainer#addFilter(java.lang.String, java.lang.String, java.util.Map)
	 */
	public void addFilter(String name, String classname, Map<String, String> parameters) {
		server.addFilter(name, classname, parameters);
	}
	
	/* (non-Javadoc)
	 * @see com.gruter.common.http.FilterContainer#addGlobalFilter(java.lang.String, java.lang.String, java.util.Map)
	 */
	public void addGlobalFilter(String name, String classname, Map<String, String> parameters) {
		server.addGlobalFilter(name, classname, parameters);
	}
	
	/**
	 * 지정한 hostname 과 port 를 기반으로 서버를 구동한다.
	 * @throws IOException
	 */
	protected void runServer() throws IOException {
		if (running == false && server == null) {
			initServer();
		}
		if (running == false) {
			server.start();
			running = true;
		}
	}

	/**
	 * 구동된 서버를 중지시킨다.
	 * @throws Exception
	 */
	protected void stopServer() throws Exception {
		if (server == null) {
			throw new Exception("server is not running");
		}
		server.stop();
	}
	
	@SuppressWarnings("unchecked")
	protected <P extends Page> P requestToServer(String method, String path) throws Exception {
		if (isRunning() == false) {
			runServer();
		}
		
		DefaultHttpClient client = new DefaultHttpClient();
		HttpUriRequest request = invokeMethod(method, getPageURI(path));
		Page page = new Page(new WebResponse(client.execute(request)));
		return (P) page;
		/*
		// HTMLUnit 으로 작성했다가 필요한 Library 가 워낙 많아서 주석 처리하고 httpcomponent 4.x 으로 변경하고 하위에 inner class 만들었습니다.
		// maven 을 사용하면 test 용 library 는 제외시키기 편한데 ant 는 건드려야 할 것이 많아서 급 수정
		WebClient client = new WebClient();
		WebRequest request = new WebRequest(new URL(getPageURI(path)), HttpMethod.valueOf(method.toUpperCase()));
		try {
			return (P) client.getPage(request);
		} catch (FailingHttpStatusCodeException e) {
			Page page = new HtmlPage(request.getUrl(), e.getResponse(), client.getCurrentWindow());
			return (P) page;
		} catch (MalformedURLException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} finally {
			client.closeAllWindows();
		}
		*/
	}
	
	private HttpUriRequest invokeMethod(String method, String page) {
		Map<String, Class<? extends HttpUriRequest>> methods = new HashMap<String, Class<? extends HttpUriRequest>>();
		methods.put("GET", HttpGet.class);
		methods.put("PUT", HttpPut.class);
		methods.put("POST", HttpPost.class);
		methods.put("OPTIONS", HttpOptions.class);
		methods.put("HEAD", HttpHead.class);
		methods.put("TRACE", HttpTrace.class);
		methods.put("DELETE", HttpDelete.class);
		try {
			return methods.get(method.toUpperCase()).getConstructor(String.class).newInstance(page);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private boolean isRunning() {
		return running;
	}

	/**
	 * 테스트에 사용할 URL 문자열을 만든다. 
	 * @param path
	 * @return
	 */
	protected String getPageURI(String path) {
		return new StringBuilder("http://").append(getAuthority()).append(path).toString();
	}
	
	protected String getAuthority() {
		return getPort() == 80 ? getHostname() : getHostname() + ":" + getPort();
	}
	
	protected String getWebContextPath() {
		return webContextPath;
	}
	
	protected String getHostname() {
		return hostname;
	}
	
	protected int getPort() {
		return port;
	}
	
	
	/**
	 * HtmlUnit 의 Page class 를 코드변경없이 사용하고자 동일한 이름의 클래스로 필요한 것만 작성
	 */
	static class Page {
		WebResponse response;
		
		Page(WebResponse response) {
			this.response = response;
		}
		
		WebResponse getWebResponse() {
			return response;
		}
	}
	
	/**
	 * HtmlUnit 의 WebResponse class 를 코드변경없이 사용하고자 동일한 이름의 클래스로 필요한 것만 작성
	 */
	static class WebResponse {
		HttpResponse response;
		
		WebResponse(HttpResponse response) {
			this.response = response;
		}

		public String getResponseHeaderValue(String name) {
			return response.getFirstHeader(name).getValue();
		}

		public int getStatusCode() {
			return response.getStatusLine().getStatusCode();
		}
	}
}
