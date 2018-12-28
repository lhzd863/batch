package com.batch.master;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.batch.util.Configuration;
import com.batch.util.JobInfo;
import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class ResourceCollect implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
	Map serverstatusM = null;
	String cfg = "";
	final static String clss = "resource";
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	serverstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_NODESTATUS);
    	
    	SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String yyyymmdd = time.format(nowTime);
		if(log==null){
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), clss+"_"+yyyymmdd);
		}
	}

	public void setResource(String node){
		
	}
	
    public String getResource(String node){
		return null;
	}
    
    public boolean isActice(String node){
    	return false;
    }

	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), clss+"_"+yyyymmdd);
			//refresh config
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
			
			//save server status
			String slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
			String slvListPath = slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE_LIST);
			if(!new File(slvListPath).exists()){
				log.info("path not exists "+slvListPath+".");
				try {
					Thread.sleep(Long.parseLong(conf.get(StrVal.SYS_HEARTBEADT_TIME)));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check directory "+slvListPath+".");
			}
			String[] slvlist = new File(slvListPath).list();
			for(int i=0;i<slvlist.length;i++){
				String serverctl = slvlist[i];
				Map map = JobInfo.readCTLFile(conf,slvListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+serverctl);
				Map tmpMap = new ConcurrentHashMap();
				Map runningCtlMap = new ConcurrentHashMap();
				String serverinfo1 = "";
				Iterator itertmp = map.entrySet().iterator();
				while(itertmp.hasNext()){
					Entry entry = (Entry) itertmp.next();
					String keytmp = (String) entry.getKey();
					String valtmp = (String)  entry.getValue();
					tmpMap.put(keytmp, valtmp);
					serverinfo1 += "<"+keytmp+","+valtmp+">,";
				}
				if(serverinfo1.length()>1&&conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
					serverinfo1 = serverinfo1.substring(0,serverinfo1.length()-1);
					String serverinfo = "Server "+serverctl+" info{";
					serverinfo +=serverinfo1+"}";
					log.info(serverinfo);
				}
				serverstatusM.put(serverctl, tmpMap);
			}
			//log.info("*********************** Sleep ["+conf.get(StrVal.SYS_HEARTBEADT_TIME)+"] ***********************.");
			try {
				Thread.sleep(Long.parseLong(conf.get(StrVal.SYS_HEARTBEADT_TIME)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
    
}
