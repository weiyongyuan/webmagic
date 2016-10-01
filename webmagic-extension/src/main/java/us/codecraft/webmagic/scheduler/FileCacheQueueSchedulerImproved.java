package us.codecraft.webmagic.scheduler;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;

import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.scheduler.DuplicateRemovedScheduler;
import us.codecraft.webmagic.scheduler.MonitorableScheduler;
import us.codecraft.webmagic.scheduler.component.DuplicateRemover;

/**
 * Store urls and cursor in files so that a Spider can resume the status when
 * shutdown.<br>
 * 增加去重的校验，对需要重复爬取的网址进行正则过滤，同时对Request类增加了一个是否需要重复抓取的字段
 * request的setNeedCycleCraw(true)用于单独的request时使用起来很方便，二正则则匹配一批request很方便
 * 
 * @author code4crafter@gmail.com&willimas <br>
 * @since 0.2.0
 */
public class FileCacheQueueSchedulerImproved extends DuplicateRemovedScheduler
		implements MonitorableScheduler, Closeable {

	private String filePath = System.getProperty("java.io.tmpdir");

	private String fileUrlAllName = ".urls.txt";

	private Task task;

	private String fileCursor = ".cursor.txt";

	private PrintWriter fileUrlWriter;

	private PrintWriter fileCursorWriter;

	private AtomicInteger cursor = new AtomicInteger();

	private AtomicBoolean inited = new AtomicBoolean(false);

	private BlockingQueue<Request> queue;

	private Set<String> urls;

	private ScheduledExecutorService flushThreadPool;

	private String regx;
	private static List<String> regexs = new ArrayList<String>();

	public FileCacheQueueSchedulerImproved(String filePath) {
		if (!filePath.endsWith("/") && !filePath.endsWith("\\")) {
			filePath += "/";
		}
		this.filePath = filePath;
		initDuplicateRemover();
	}

	private void flush() {
		fileUrlWriter.flush();
		fileCursorWriter.flush();
	}
	
	private void init(Task task) {
		this.task = task;
		File file = new File(filePath);
		if (!file.exists()) {
			file.mkdirs();
		}
		readFile();
		initWriter();
		initFlushThread();
		inited.set(true);
		logger.info("init cache scheduler success");
	}

	private void initDuplicateRemover() {
		setDuplicateRemover(new DuplicateRemover() {
			@Override
			public boolean isDuplicate(Request request, Task task) {
				if (!inited.get()) {
					init(task);
				}
				boolean temp = false;
				String url = request.getUrl();
				temp = urls.contains(url);
				//temp = !urls.add(url);// 原来验证URL是否存在,但是add的话没有的话就会加进去
				// 如果正则匹配存在，先进行正则匹配
				if (regexs.size() > 0) {
					for (String regex : regexs) {
						// 如果该request是需要重复抓取的，或者符合重复抓取的正则，则return false 重复进行抓取
						if (request.isNeedCycleCraw() || url.matches(regex)) {// 二次校验，如果符合我们需要重新爬取的，返回false。可以重新爬取
							temp = false;
						}
						return temp;

					}
				} else if (request.isNeedCycleCraw()) {// 二次校验，如果符合我们需要重新爬取的，返回false。可以重新爬取
					temp = false;
					return temp;
				}

				return temp;
			}

			@Override
			public void resetDuplicateCheck(Task task) {
				urls.clear();
			}

			@Override
			public int getTotalRequestsCount(Task task) {
				return urls.size();
			}
		});
	}

	private void initFlushThread() {
		flushThreadPool = Executors.newScheduledThreadPool(1);
		flushThreadPool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				flush();
			}
		}, 10, 10, TimeUnit.SECONDS);
	}

	private void initWriter() {
		try {
			fileUrlWriter = new PrintWriter(new FileWriter(
					getFileName(fileUrlAllName), true));
			fileCursorWriter = new PrintWriter(new FileWriter(
					getFileName(fileCursor), false));
		} catch (IOException e) {
			throw new RuntimeException("init cache scheduler error", e);
		}
	}

	private void readFile() {
		try {
			queue = new LinkedBlockingQueue<Request>();
			urls = new LinkedHashSet<String>();
			readCursorFile();
			readUrlFile();
			// initDuplicateRemover();
		} catch (FileNotFoundException e) {
			// init
			logger.info("init cache file " + getFileName(fileUrlAllName));
		} catch (IOException e) {
			logger.error("init file error", e);
		}
	}

	/**
	 * 判断if (lineReaded > cursor.get()) {queue.add(new Request(line));}
	 * 有问题，因为urls的保存是一个唯一的URL只保存一次，但是cursor文件好像每次都会重写，这样一来urls的URL的
	 * 个函数肯定会大于cursor，这样已经保存过的URL就会再加入队列。造成的结果就是：比如需要登录的网站，urls中
	 * 登录后才能访问的URL就会加入队列，这样poll出来的request进行访问时机会出问题
	 * @throws IOException
	 *@void
	 *@by weirongzhi
	 *@2016年9月30日上午11:47:48
	 */
	private void readUrlFile() throws IOException {
		String line;
		BufferedReader fileUrlReader = null;
		try {
			fileUrlReader = new BufferedReader(new FileReader(
					getFileName(fileUrlAllName)));
			int lineReaded = 0;
			while ((line = fileUrlReader.readLine()) != null) {
				urls.add(line.trim());
				lineReaded++;
				//如果读取到的行数大于cursor
				if (lineReaded > cursor.get()) {
					queue.add(new Request(line));
				}
			}
		} finally {
			if (fileUrlReader != null) {
				IOUtils.closeQuietly(fileUrlReader);
			}
		}
	}

	private void readCursorFile() throws IOException {
		BufferedReader fileCursorReader = null;
		try {
			fileCursorReader = new BufferedReader(new FileReader(
					getFileName(fileCursor)));
			String line;
			// read the last number
			while ((line = fileCursorReader.readLine()) != null) {
				cursor = new AtomicInteger(NumberUtils.toInt(line));
			}
		} finally {
			if (fileCursorReader != null) {
				IOUtils.closeQuietly(fileCursorReader);
			}
		}
	}
	
	public void close() throws IOException {
		flushThreadPool.shutdown();
		fileUrlWriter.close();
		fileCursorWriter.close();
	}
	/**
	 * 
	 * @param filename
	 * @return
	 *@String
	 *@by weirongzhi
	 *@2016年9月30日上午11:40:20
	 */
	private String getFileName(String filename) {
		return filePath + task.getUUID() + filename;
	}

	/**加入queue
	 * 如果urls中没有该URL则写入urls.txt文件
	 * @param filename
	 * @return
	 *@author weirongzhi
	 *@2016年9月29日下午5:52:18
	 */
	protected void pushWhenNoDuplicate(Request request, Task task) {
		// Returns true if this collection changed as a result of the call. (Returns false if this collection does not permit duplicates and already contains the specified element.)
		queue.add(request);
		if (urls.add(request.getUrl())) {
			fileUrlWriter.println(request.getUrl());
			fileCursorWriter.println(cursor.incrementAndGet());
		}
	}

	/**
	 * 
	 * @return
	 *@Request
	 *@by weirongzhi
	 *@2016年9月30日上午11:45:03
	 */
	@Override
	public synchronized Request poll(Task task) {
		if (!inited.get()) {
			init(task);
		}
		//fileCursorWriter.println(cursor.incrementAndGet());
		return queue.poll();
	}

	@Override
	public int getLeftRequestsCount(Task task) {
		return queue.size();
	}

	@Override
	public int getTotalRequestsCount(Task task) {
		return getDuplicateRemover().getTotalRequestsCount(task);
	}
	
	public String getRegx() {
		return regx;
	}

	/**
	 * 设置保留需要重复爬取url的正则表达式
	 * 
	 * @param regx
	 */
	public void setRegx(String regx) {
		this.regx = regx;
	}

	public List<String> getRegexs() {
		return regexs;
	}

	/**
	 * 需要重复爬取的URL的正则表达式集合
	 * 
	 * @param regex
	 * @author weirongzhi 2016年9月28日上午9:11:09
	 */
	public void setRegexs(String... regex) {
		for (String regexStr : regex) {
			regexs.add(regexStr);
		}
	}

}
