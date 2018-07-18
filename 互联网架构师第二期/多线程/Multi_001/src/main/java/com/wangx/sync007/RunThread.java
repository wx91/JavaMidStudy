package com.wangx.sync007;

public class RunThread extends Thread{
	/** volatile **/
	private boolean isRunning = true;
	private void setRuning(boolean isRuning) {
		this.isRunning = isRuning;
	}
	public void run() {
		System.out.println("进入run方法");
		while(isRunning == true) {
			
		}
		System.out.println("线程停止");
	}
	
	public static void main(String[] args) throws Exception {
		RunThread rt = new RunThread();
		rt.start();
		Thread.sleep(3000);
		rt.setRuning(false);
		System.out.println("isRuning的值已经被设置为false");
		Thread.sleep(1000);
		System.out.println(rt.isRunning);
	}
}
