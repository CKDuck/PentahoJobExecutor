package com.rdml.pentaho.util;

public class LogDevice implements ILogDevice {
	
	public final static String LOG_INFO = "LOG_INFO";
	public final static String LOG_ERROR = "LOG_ERROR";
	
	public LogDevice() {
	}
	
	@Override
	public void log(String str) {
		System.out.println(str);
	}

}
