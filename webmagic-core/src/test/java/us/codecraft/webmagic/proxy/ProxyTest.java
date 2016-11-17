package us.codecraft.webmagic.proxy;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.http.HttpHost;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author yxssfxwzy@sina.com May 30, 2014
 * 
 */
public class ProxyTest {

	private static List<String[]> httpProxyList = new ArrayList<String[]>();

	@BeforeClass
	public static void before() {
		// String[] source = { "0.0.0.1:0", "0.0.0.2:0", "0.0.0.3:0",
		// "0.0.0.4:0" };
		Set<String> lines = readFileLines("/tmp/proxy/proxyip.txt");
		String[] source=lines.toArray(new String[lines.size()]);
		for (String str : source) {
			str="::"+str;
		}
		//String[] source = { "::0.0.0.1:0", "::0.0.0.2:0", "::0.0.0.3:0", "::0.0.0.4:0" };
		for (String line : source) {
			System.out.println("line="+line);
			httpProxyList.add(new String[] {line.split(":")[0], line.split(":")[1], line.split(":")[2], line.split(":")[3] });
		}
	}
	
	public static Set<String> readFileLines(String filePath) {
		Set<String> lines =new HashSet<String>();
		BufferedReader br = null;
		try {
			File file = new File(filePath);
			br=new BufferedReader(new FileReader(file));
			String line=null;
			while ((line=br.readLine())!=null) {
				lines.add(line);
			}
		} catch (Exception e) {
		}
		return lines;
	}

	@Test
	public void testProxy() {
		SimpleProxyPool proxyPool = new SimpleProxyPool(httpProxyList);
		proxyPool.setReuseInterval(500);
		assertThat(proxyPool.getIdleNum()).isEqualTo(4);
		assertThat(new File(proxyPool.getProxyFilePath()).exists()).isEqualTo(true);
		for (int i = 0; i < 2; i++) {
			List<Fetch> fetchList = new ArrayList<Fetch>();
			while (proxyPool.getIdleNum() != 0) {
				Proxy proxy = proxyPool.getProxy();
				HttpHost httphost = proxy.getHttpHost();
				// httphostList.add(httphost);
				System.out.println(httphost.getHostName() + ":" + httphost.getPort());
				Fetch tmp = new Fetch(httphost);
				tmp.start();
				fetchList.add(tmp);
			}
			for (Fetch fetch : fetchList) {
				proxyPool.returnProxy(fetch.hp, Proxy.SUCCESS);
			}
			System.out.println(proxyPool.allProxyStatus());

		}
	}

	class Fetch extends Thread {
		HttpHost hp;

		public Fetch(HttpHost hp) {
			this.hp = hp;
		}

		@Override
		public void run() {
			try {
				System.out.println("fetch web page use proxy: " + hp.getHostName() + ":" + hp.getPort());
				sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
