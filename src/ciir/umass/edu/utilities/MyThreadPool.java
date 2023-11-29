/*===============================================================================
 * Copyright (c) 2010-2012 University of Massachusetts.  All Rights Reserved.
 *
 * Use of the RankLib package is subject to the terms of the software license set 
 * forth in the LICENSE file included with this software, and also available at
 * http://people.cs.umass.edu/~vdang/ranklib_license.html
 *===============================================================================
 */

package ciir.umass.edu.utilities;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author vdang
 *
 */
public class MyThreadPool extends ThreadPoolExecutor {

	private final Semaphore semaphore;
	private int size = 0;
	
	private MyThreadPool(int size)
	{
		super(size, size, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		semaphore = new Semaphore(size, true);
		this.size = size;
	}
	
	private static MyThreadPool singleton = null;
	public static MyThreadPool getInstance()
	{
		if(singleton == null)
			init(Runtime.getRuntime().availableProcessors());
		return singleton;
	}
	
	public static void init(int poolSize)
	{
		singleton = new MyThreadPool(poolSize);
	}
	public int size()
	{
		return size;
	}
	private WorkerThread[] workers = null;
	public WorkerThread[] execute(WorkerThread worker, int nTasks)
	{
// StackTraceElement[] _element = Thread.currentThread().getStackTrace();
// System.err.println("<--");
// for (int i = 0 ;i < _element.length; i++){
// 	System.err.println(_element[i]);
// }
// System.err.println("-->");
// System.err.println("entry");

		try {
			this.semaphore.acquire(nTasks);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		MyThreadPool p = MyThreadPool.getInstance();
		int[] partition = p.partition(nTasks);
		// WorkerThread[] workers = new WorkerThread[partition.length-1];
		if (workers == null) {
			workers = new WorkerThread[partition.length-1];
		} else if (workers.length < partition.length - 1) {
			WorkerThread[] tmp = new WorkerThread[partition.length-1];
			System.arraycopy(workers, 0, tmp, 0, workers.length);
			workers = tmp;
		}
		for(int i=0;i<partition.length-1;i++)
		{
			// WorkerThread w = worker.clone();
			WorkerThread w = workers[i];
			if (w == null) {
				w = worker.clone();
				workers[i] = w;
			}
			w.set(partition[i], partition[i+1]-1);
			// w.set(worker);
			workers[i] = w;
			p.execute0(w);
		}
		await0(workers);
		// await();
// System.err.println("leave");
		return workers;
	}

	public void await0(WorkerThread[] threads) {
		for (int i = 0; i < threads.length; ++i) {
			while (!threads[i].finished);
		}
	}
	
	public void await()
	{
		for(int i=0;i<size;i++)
		{
			try {
				semaphore.acquire();				
			}
			catch(Exception ex)
			{
				throw RankLibError.create("Error in MyThreadPool.await(): ", ex);
			}
		}
		for(int i=0;i<size;i++)
			semaphore.release();
	}
	public int[] partition(int listSize)
	{
		int nChunks = Math.min(listSize, size);
		int chunkSize = listSize/nChunks;
		int mod = listSize % nChunks;
		int[] partition = new int[nChunks+1];
		partition[0] = 0;
		for(int i=1;i<=nChunks;i++)
			partition[i] = partition[i-1] + chunkSize + ((i<=mod)?1:0);
		return partition;
	}
	
	public void execute0(Runnable task) 
	{
		try {
			super.execute(task);
		}
		catch(Exception ex)
		{
			throw RankLibError.create("Error in MyThreadPool.execute(): ", ex);
		}
	}
	
	public void execute(Runnable task) 
	{
		try {
			semaphore.acquire();
			super.execute(task);
		}
		catch(Exception ex)
		{
			throw RankLibError.create("Error in MyThreadPool.execute(): ", ex);
		}
	}
	protected void afterExecute(Runnable r, Throwable t)
	{
		super.afterExecute(r, t);
		semaphore.release();
	}
}
