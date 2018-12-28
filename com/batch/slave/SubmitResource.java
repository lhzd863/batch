package com.batch.slave;

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
import com.batch.util.Configuration;
import com.batch.util.JobInfo;
import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class SubmitResource implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	String node = "";
	String cfg = "";
	Map runningMap = null;
	final static String clss = "slv";
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
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), node+"_"+yyyymmdd);
			//init conf
			long endtime = System.currentTimeMillis();
			long subval = endtime - starttime;
			long subvalconf = Long.parseLong(conf.get(StrVal.CONF_REF_TIME));
			//init conf
			if(subvalconf<=subval){
				log.info(node+" refresh config.");
				conf.initialize(new File(cfg));
				DefaultConf.initialize(conf);
				starttime = endtime;
			}
			
			//statistics resource
			log.info(node+" Submit slave resource.");
			String slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
			String slvNodeListPath = slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE_LIST);
			//statistics cpu
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info(node+" node exe cmd statistics "+conf.get(StrVal.SYSTEM_PARAMETER_CPUPNT)+"["+conf.get(StrVal.RESOURCE_CMD_CPU)+"].");
			}
			String cpu = exeSystemCmd(conf.get(StrVal.RESOURCE_CMD_CPU));
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info(node+" node exe cmd statistics "+conf.get(StrVal.SYSTEM_PARAMETER_MEMPNT)+"["+conf.get(StrVal.RESOURCE_CMD_MEM)+"].");
			}
			//statistics mem
			String mem = exeSystemCmd(conf.get(StrVal.RESOURCE_CMD_MEM));
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info(node+" delete node resource file .");
			}
			//write info
			JobInfo.writeBlankFile(slvNodeListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node);
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info(node+" node resource "+conf.get(StrVal.SYSTEM_PARAMETER_CPUPNT)+" ["+cpu+"].");
			}
			JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_CPUPNT)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+cpu+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvNodeListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node);
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info(node+" node resource "+conf.get(StrVal.SYSTEM_PARAMETER_MEMPNT)+" ["+mem+"].");
			}
			JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_MEMPNT)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+mem+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvNodeListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node);
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info(node+" node resource "+conf.get(StrVal.SYSTEM_PARAMETER_PRCCNT)+" ["+runningMap.size()+"].");
			}
			JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_PRCCNT)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+runningMap.size()+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvNodeListPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node);
			
			//sleep
			try {
				Thread.sleep(Long.parseLong(conf.get(StrVal.SYS_HEARTBEADT_TIME)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public String exeSystemCmd(String cmd){
		Process p = null;
		InputStream is = null;
		BufferedReader reader = null;
		String line = null;
		String res = "";
		try {
			p = Runtime.getRuntime().exec(cmd);
			is = p.getInputStream();
			reader = new BufferedReader(new InputStreamReader(is));
			while((line=reader.readLine())!=null){
				res = line;
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				if(p.waitFor()!=0){
					
				}
				is.close();
				reader.close();
				p.destroy();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return res;
	}

}
