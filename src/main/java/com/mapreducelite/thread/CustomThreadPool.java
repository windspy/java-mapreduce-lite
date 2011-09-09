package com.mapreducelite.thread;
import java.util.LinkedList;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-30
 * Time: 下午6:25
 * To change this template use File | Settings | File Templates.
 */
public class CustomThreadPool extends ThreadGroup {
	private boolean isClosed = false; 
	private LinkedList workQueue;      
	private static int threadPoolID = 1;  
	public CustomThreadPool(int poolSize) {  

		super(threadPoolID + "");    
		setDaemon(true);               
		workQueue = new LinkedList();  
		for(int i = 0; i < poolSize; i++) {
			new WorkThread(i).start();   
		}
	}

	public synchronized void execute(Runnable task) {
		if(isClosed) {
			throw new IllegalStateException();
		}
		if(task != null) {
			workQueue.add(task);
			notify(); 		
		}
	}

	private synchronized Runnable getTask(int threadid) throws InterruptedException {
		while(workQueue.size() == 0) {
			if(isClosed) return null;
			wait();	
		}
		return (Runnable) workQueue.removeFirst(); 
	}

	public synchronized void closePool() {
		if(! isClosed) {
			waitFinish();        
			isClosed = true;
			workQueue.clear();  
			interrupt(); 	
		}
	}
	public void waitFinish() {
		synchronized (this) {
			isClosed = true;
			notifyAll();		
		}
		Thread[] threads = new Thread[activeCount()]; 
		int count = enumerate(threads); 
		for(int i =0; i < count; i++) {
			try {
				threads[i].join();	
			}catch(InterruptedException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	private class WorkThread extends Thread {
		private int id;
		public WorkThread(int id) {
			super(CustomThreadPool.this,id+"");
			this.id =id;
		}
		public void run() {
			while(! isInterrupted()) { 
				Runnable task = null;
				try {
					task = getTask(id);	
				}catch(InterruptedException ex) {
					ex.printStackTrace();
				}
				if(task == null) return;

				try {
					task.run(); 
				}catch(Throwable t) {
					t.printStackTrace();
				}
			}
		}
	}
}
