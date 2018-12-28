package com.batch.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import com.batch.master.DefaultConf;

public class ClearLog implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	String node = "";
	String cfg = "";
	Map runningMap = null;
	final static String clss = "clr";
	SimpleDateFormat timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	public void initialize(String node,String cfgfile,Map shareM){
		//init conf
		this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	this.node = node;
    	runningMap = (Map) shareM.get(StrVal.MAP_KEY_RUNNING);
	}
	
	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
			if(log==null){
				log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), clss+"_"+node+"_"+yyyymmdd);
			}
			//init conf
			long endtime = System.currentTimeMillis();
			long subval = endtime - starttime;
			long subvalconf = Long.parseLong(conf.get(StrVal.CONF_REF_TIME));
			//init conf
			if(subvalconf<=subval){
				log.info(clss+" refresh config.");
				conf.initialize(new File(cfg));
				DefaultConf.initialize(conf);
				starttime = endtime;
			}
			File logfile = new File(conf.get("sys.log.path"));
			String[] filelst = logfile.list();
			if(conf.get(StrVal.SYS_DEBUG_FLAG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(node+" check log keep period.");
			}
			for(int i=0;i<filelst.length;i++){
				String filename = filelst[i];
				File subfile = new File(conf.get("sys.log.path")+"/"+filename);
				long modfiytime = subfile.lastModified();
				long currenttime = System.currentTimeMillis();
				long subtime = currenttime - modfiytime;
				long keepperiodtime = Math.abs(Long.parseLong(conf.get(StrVal.SYS_LOG_KEEPPERIOD)))*24*3600*1000;
				if(conf.get(StrVal.SYS_DEBUG_FLAG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					log.info(filename+"  will check.");
				}
				if(subtime-keepperiodtime>0){
					log.info(filename+" modfiy timestamp is"+timestamp.format(modfiytime)+
							 ",keep period is "+Long.parseLong(conf.get(StrVal.SYS_LOG_KEEPPERIOD))+
							 ",current time is "+timestamp.format(currenttime)+
							 ",sub time is "+subtime+
							 ",file will delete.");
					if(subfile.isDirectory()){
						delSubDirectory(subfile.getAbsolutePath());
					}else{
						subfile.delete();
					}
				}
			}
			//sleep
			try {
				Thread.sleep(3600000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void delSubDirectory(String path){
		File file = new File(path);
		if(file.isDirectory()){
			String[] filelst = file.list();
			for(int i=0;i<filelst.length;i++){
				String filename = filelst[i];
				File subfile = new File(path+"/"+filename);
				String subpath = subfile.getAbsolutePath();
				delSubDirectory(subpath);
			}
			log.info("Directory "+file.getAbsoluteFile()+"  will delete.");
			file.delete();
		}else{
			log.info("File "+file.getAbsoluteFile()+"  will delete.");
			file.delete();
		}
	}

}
