package com.batch.util;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

public class LoggerUtil {

	public Logger getLoggerByName(String elogPath,String name){
		Logger logger = Logger.getLogger(name);
		logger.removeAllAppenders();
		logger.setLevel(Level.DEBUG);
		logger.setAdditivity(true);
		FileAppender appender = new RollingFileAppender();
		PatternLayout layout = new PatternLayout();
//		String  conversionPattern = "[%d{yyyy-MM-dd HH:mm:ss}]|%C{1}|%m%n";
		String  conversionPattern = "[%d{HH:mm:ss}]|%C{1}|%m%n";
		layout.setConversionPattern(conversionPattern);
		appender.setLayout(layout);
		//String elogPath = System.getenv("EDWH_ELOG");
		appender.setFile(elogPath+"/"+name+".log");
		appender.setEncoding("utf-8");
		appender.setAppend(true);
		appender.activateOptions();
		logger.addAppender(appender);
		return logger;
	}
}
