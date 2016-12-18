package us.codecraft.webmagic.downloader;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.annotation.ThreadSafe;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.proxy.Proxy;
import us.codecraft.webmagic.selector.PlainText;
import us.codecraft.webmagic.utils.HttpConstant;
import us.codecraft.webmagic.utils.UrlUtils;

/**
 * The http downloader based on HttpClient.
 * 
 * @author code4crafter@gmail.com <br>
 * @since 0.1.0
 */
@ThreadSafe
public class HttpClientDownloader extends AbstractDownloader {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private final ConcurrentLinkedHashMap<String, CloseableHttpClient> httpClients = new ConcurrentLinkedHashMap.Builder<String, CloseableHttpClient>()
			.maximumWeightedCapacity(200).listener(new EvictionListener<String, CloseableHttpClient>() {

				@Override
				public void onEviction(String key, CloseableHttpClient value) {
					logger.info("当前失效的key=" + key + " value=" + value);
				}
			}).build();

	private HttpClientGenerator httpClientGenerator = new HttpClientGenerator();
	
	public CloseableHttpClient getHttpClient(Site site, Proxy proxy) {

		if (site == null) {
			return httpClientGenerator.getClient(null, proxy);
		}
		String uuid = site.getUuid();
		CloseableHttpClient httpClient = null;
		if (uuid == null) {
			uuid = site.getDomain();
			httpClient = httpClients.get(uuid);
		} else {
			httpClient = httpClients.get(uuid);
		}
		if (httpClient == null) {
			synchronized (this) {
				httpClient = httpClients.get(uuid);
				if (httpClient == null) {
					httpClient = httpClientGenerator.getClient(site, proxy);
					httpClients.put(uuid, httpClient);
				}
			}
		}
		return httpClient;
	}

	@Override
	public Page download(Request request, Task task) {
		long startTime = System.currentTimeMillis();
		Site site = null;
		if (task != null) {
			site = task.getSite();
		}
		
		Set<Integer> acceptStatCode;
		String charset = null;
		Map<String, String> headers = null;
		if (site != null) {
			acceptStatCode = site.getAcceptStatCode();
			charset = site.getCharset();
			headers = site.getHeaders();
		} else {
			acceptStatCode = Sets.newHashSet(200);
		}
		logger.info("downloading page {}", request.getUrl());
		CloseableHttpResponse httpResponse = null;
		int statusCode = 0;
		try {
			HttpHost proxyHost = null;
			Proxy proxy = null; // TODO
			if (site.getHttpProxyPool() != null
					&& site.getHttpProxyPool().isEnable()) {
				proxy = site.getHttpProxyFromPool();
				proxyHost = proxy.getHttpHost();
			} else if (site.getHttpProxy() != null) {
				proxyHost = site.getHttpProxy();
			}

			HttpUriRequest httpUriRequest = getHttpUriRequest(request, site,
					headers, proxyHost);// ���������˴���
			Header[] allHeaders = httpUriRequest.getAllHeaders();
			for (Header header : allHeaders) {
				logger.info("请求之前的header--->"+header.getName()+":"+header.getValue());
			}
			CloseableHttpClient httpClient = getHttpClient(site, proxy);
			System.out.println(httpClient.hashCode());
			httpResponse = getHttpClient(site, proxy).execute(httpUriRequest);// getHttpClient�������˴�����，这是作者原来的写法
			statusCode = httpResponse.getStatusLine().getStatusCode();
			
			/*
			 * 执行完请求后加该URL加入site的Referer,这样上次的请求URL会自动作为下次的Referer而携带，
			 * 但是有些Referer还是需要自己手动吊针一些以与网站保持一致，在Process()方法中对于具体的
			 * 请求在调用addTargetRequest()之前再次site.addHeader()覆盖即可
			 */
		
			//site.addHeader("Referer", request.getUrl());
			//将返回的header中的Set-Cookie放入extra并加process方法中加入下次请求的header
			request.putExtra("Set-Cookie", httpResponse.getHeaders("Set-Cookie"));
			request.putExtra("headers", httpResponse.getAllHeaders());
			request.putExtra(Request.STATUS_CODE, statusCode);
			Header[] allHeaders2 = httpResponse.getAllHeaders();
			for (Header header : allHeaders2) {
				logger.info("响应的的header--->"+header.getName()+":"+header.getValue());
			}
			// 如果是正常的200状态码
			if (statusAccept(acceptStatCode, statusCode)) {
				Page page = handleResponse(request, charset, httpResponse, task);
				//将响应的cookie加入
				Header[] headers2 = httpResponse.getHeaders("Set-Cookie");
				for (Header header : headers2) {
					if ("Set-Cookie".equals(header.getName())) {
						logger.info("获取的response中的cookie为："+header.getName()+"="+header.getValue().substring(0, header.getValue().indexOf(";")));
						//site.addHeader("Cookie", header.getValue().substring(0, header.getValue().indexOf(";")));
					}
				}
				onSuccess(request);
				logger.info(request.getMethod() + " statusCode-->" + statusCode
						+ " 获取页面成功  "+request.getUrl());
				return page;
				// 如果是302
			} else if (statusCode == HttpStatus.SC_MOVED_TEMPORARILY) {
				logger.warn(request.getMethod() + " statusCode-->" + statusCode
						+ " " + request.getUrl());
				String locationUrl = httpResponse.getLastHeader("Location")
						.getValue();
				logger.info("获取重定向中的location---->" + locationUrl);
				request = new Request(locationUrl);
				Page page = download(request, task);
				return page;
				// 如果是401
			} else if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
				Page page = handleResponse(request, charset, httpResponse, task);
				logger.warn("未授权，禁止访问：" + statusCode + "\t" + request.getUrl());
				logger.info(page.getHtml().toString());
				return page;
				//如果是403
			} else if (statusCode==HttpStatus.SC_FORBIDDEN) {
				logger.warn("Forbidden,没有权限访问该资源：" + statusCode + "\t" + request.getUrl());
				Page page = handleResponse(request, charset, httpResponse, task);
				return page;
				//如果是404
			} else if (statusCode==HttpStatus.SC_NOT_FOUND) {
				logger.warn("该资源不存在，请检查访问路径：" + statusCode + "\t" + request.getUrl());
				//再次判断是不是关闭了连接
				Header conn = httpResponse.getFirstHeader("Connection");
				Page page = handleResponse(request, charset, httpResponse, task);
				if ("close".equals(conn.getValue())) {
					logger.info("对于请求"+page.getUrl()+"对方服务器关闭了连接,再次将该URL加入队列");
					page.addTargetRequest(page.getRequest().putExtra(Request.CYCLE_TRIED_TIMES, "3"));
				}
				return page;
				//如果是500
			}else if (statusCode==HttpStatus.SC_INTERNAL_SERVER_ERROR) {
				logger.warn("内部服务器错误：" + statusCode + "\t" + request.getUrl());
				Page page = handleResponse(request, charset, httpResponse, task);
				return page;
			}else if(statusCode==HttpStatus.SC_BAD_GATEWAY){
				logger.warn("（错误网关） 服务器作为网关或代理，从上游服务器收到无效响应。" + statusCode + "\t" + request.getUrl());
				Page page = handleResponse(request, charset, httpResponse, task);
				return page;

			}else if(statusCode==HttpStatus.SC_NOT_IMPLEMENTED){
				logger.warn("(Not Implemented/未实现)服务器不支持请求中要求的功能。" + statusCode + "\t" + request.getUrl());
				Page page = handleResponse(request, charset, httpResponse, task);
				return page;
			}else if (statusCode==HttpStatus.SC_SERVICE_UNAVAILABLE) {
				logger.warn("服务无法获得：" + statusCode + "\t" + request.getUrl());
				Page page = handleResponse(request, charset, httpResponse, task);
				return page;
			} else{
				logger.warn("code error " + statusCode + "\t"
						+ request.getUrl());
				return null;
			}
		} catch (IOException e) {
			logger.warn("download page " + request.getUrl() +" statusCode="+statusCode+ " error", e);
			if (site.getCycleRetryTimes() > 0) {
				return addToCycleRetry(request, site);
			}
			onError(request);
			return null;
		} finally {
			request.putExtra(Request.STATUS_CODE, statusCode);
			if (site.getHttpProxyPool() != null
					&& site.getHttpProxyPool().isEnable()) {
				site.returnHttpProxyToPool(
						(HttpHost) request.getExtra(Request.PROXY),
						(Integer) request.getExtra(Request.STATUS_CODE));
			}
			try {
				if (httpResponse != null) {
					// ensure the connection is released back to pool
					EntityUtils.consume(httpResponse.getEntity());
				}
			} catch (IOException e) {
				logger.warn("close response fail", e);
			}
			logger.info("请求"+request.getUrl()+"获得响应耗时:"+(System.currentTimeMillis()-startTime)+"毫秒");
		}
	}

	private HttpClientContext createHttpClientContext() {
		HttpClientContext context = HttpClientContext.create();
		return context;
	}

	@Override
	public void setThread(int thread) {
		httpClientGenerator.setPoolSize(thread);
	}

	protected boolean statusAccept(Set<Integer> acceptStatCode, int statusCode) {
		return acceptStatCode.contains(statusCode);
	}

	public HttpUriRequest getHttpUriRequest(Request request, Site site, Map<String, String> headers, HttpHost proxy) {
		RequestBuilder requestBuilder = selectRequestMethod(request).setUri(request.getUrl());
		if (headers != null) {
			for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
				requestBuilder.addHeader(headerEntry.getKey(), headerEntry.getValue());
			}
		}
		RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
				.setConnectionRequestTimeout(site.getTimeOut()).setSocketTimeout(site.getTimeOut())
				.setConnectTimeout(site.getTimeOut()).setCookieSpec(CookieSpecs.BEST_MATCH);
		if (proxy != null) {
			requestConfigBuilder.setProxy(proxy);
			request.putExtra(Request.PROXY, proxy);
		} else {
			// 设置本地代理，可以抓取程序发送的数据包,这样写报错了：PKIX path building failed。
			// requestConfigBuilder.setProxy(new HttpHost("127.0.0.1", 8888));
			// request.putExtra(Request.PROXY, proxy);
		}
		requestBuilder.setConfig(requestConfigBuilder.build());
		return requestBuilder.build();
	}

	protected RequestBuilder selectRequestMethod(Request request) {
		String method = request.getMethod();
		if (method == null || method.equalsIgnoreCase(HttpConstant.Method.GET)) {
			// default get
			return RequestBuilder.get();
		} else if (method.equalsIgnoreCase(HttpConstant.Method.POST)) {
			RequestBuilder requestBuilder = RequestBuilder.post();
			NameValuePair[] nameValuePair = (NameValuePair[]) request.getExtra("nameValuePair");
			if (nameValuePair != null && nameValuePair.length > 0) {
				requestBuilder.addParameters(nameValuePair);
			}
			return requestBuilder;
		} else if (method.equalsIgnoreCase(HttpConstant.Method.HEAD)) {
			return RequestBuilder.head();
		} else if (method.equalsIgnoreCase(HttpConstant.Method.PUT)) {
			return RequestBuilder.put();
		} else if (method.equalsIgnoreCase(HttpConstant.Method.DELETE)) {
			return RequestBuilder.delete();
		} else if (method.equalsIgnoreCase(HttpConstant.Method.TRACE)) {
			return RequestBuilder.trace();
		}
		throw new IllegalArgumentException("Illegal HTTP Method " + method);
	}

	protected Page handleResponse(Request request, String charset, HttpResponse httpResponse, Task task)
			throws IOException {
		String content = getContent(charset, httpResponse);
		Page page = new Page();
		page.setRawText(content);
		page.setUrl(new PlainText(request.getUrl()));
		page.setRequest(request);
		page.setStatusCode(httpResponse.getStatusLine().getStatusCode());
		return page;
	}

	protected String getContent(String charset, HttpResponse httpResponse) throws IOException {
		if (charset == null) {
			byte[] contentBytes = IOUtils.toByteArray(httpResponse.getEntity().getContent());
			String htmlCharset = getHtmlCharset(httpResponse, contentBytes);
			if (htmlCharset != null) {
				return new String(contentBytes, htmlCharset);
			} else {
				logger.warn("Charset autodetect failed, use {} as charset. Please specify charset in Site.setCharset()",
						Charset.defaultCharset());
				return new String(contentBytes);
			}
		} else {
			return IOUtils.toString(httpResponse.getEntity().getContent(), charset);
		}
	}

	protected String getHtmlCharset(HttpResponse httpResponse, byte[] contentBytes) throws IOException {
		String charset;
		// charset
		// 1、encoding in http header Content-Type
		String value = httpResponse.getEntity().getContentType().getValue();
		charset = UrlUtils.getCharset(value);
		if (StringUtils.isNotBlank(charset)) {
			logger.debug("Auto get charset: {}", charset);
			return charset;
		}
		// use default charset to decode first time
		Charset defaultCharset = Charset.defaultCharset();
		String content = new String(contentBytes, defaultCharset.name());
		// 2、charset in meta
		if (StringUtils.isNotEmpty(content)) {
			Document document = Jsoup.parse(content);
			Elements links = document.select("meta");
			for (Element link : links) {
				// 2.1、html4.01 <meta http-equiv="Content-Type"
				// content="text/html; charset=UTF-8" />
				String metaContent = link.attr("content");
				String metaCharset = link.attr("charset");
				if (metaContent.indexOf("charset") != -1) {
					metaContent = metaContent.substring(metaContent.indexOf("charset"), metaContent.length());
					charset = metaContent.split("=")[1];
					break;
				}
				// 2.2、html5 <meta charset="UTF-8" />
				else if (StringUtils.isNotEmpty(metaCharset)) {
					charset = metaCharset;
					break;
				}
			}
		}
		logger.debug("Auto get charset: {}", charset);
		// 3、todo use tools as cpdetector for content decode
		return charset;
	}
}
