package com.batch.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.batch.util.Configuration;
import com.batch.util.JobInfo;
import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class SlaveExecuter implements Runnable{
	
	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	String ctl = "";
	String node = "";
	String yyyymmdd = null;
	Map runningMap = null;
	
	String slvPath =  "";
	String mstPath =  "";
	public void initialize(String node,Configuration conf,Map shareM){
		this.conf = conf;
    	this.shareM = shareM;
    	this.node = node;
    	runningMap = (Map) shareM.get(StrVal.MAP_KEY_RUNNING);
    	
    	SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String yyyymmdd = time.format(nowTime);
		if(log==null){
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), node+"_"+yyyymmdd);
		}
		
	}

	public boolean execmd(String cmd){
		//log
		SimpleDateFormat time = new SimpleDateFormat("yyyyMMddHHmmss");
		SimpleDateFormat timedate = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String timedir = time.format(nowTime);
		long  starttime = System.currentTimeMillis();
		File logdir = new File(conf.get("sys.log.path")+"/"+timedate.format(nowTime));
		if(!logdir.exists()){
			log.info("mkdir "+logdir.getAbsolutePath()+".");
			logdir.mkdirs();
		}
		if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
			log.info(node+" node exe cmd ["+cmd+"].");
		}
		Logger joblog = new LoggerUtil().getLoggerByName(logdir.getAbsolutePath(), ctl+"_"+timedir);
		//parameter
		boolean res = false;
		ArrayList cmdarr = new ArrayList<String>();
		String[] args = cmd.split(" ");
		for (int k=0; k<args.length;k++){
			cmdarr.add(args[k]);
		}
		ProcessBuilder pb = new ProcessBuilder(cmdarr);
		Process processWork;
		try {
			//execute script
			processWork = pb.start();
			BufferedReader jobOut = new BufferedReader(new InputStreamReader(processWork.getInputStream()));
			String line;
			String ret = "";
			while ( (line = jobOut.readLine()) != null ) {
				ret +=line+"\n";
				joblog.info(line);
			}
			jobOut.close();
			try {
				processWork.waitFor();
			} catch (InterruptedException e) {
				
			}
			int returncode = processWork.exitValue();
			if(returncode==0){
				res = true;
			}
		} catch (IOException e) {
		}
		return res;
	}

	public String replaceVarValue(String str,String var,String val){
		String ret = str.replaceAll("\\$\\{"+var+"\\}", val);
		return ret;
	}

	@Override
	public void run() {
		slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
		mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstParaPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_MASTER_DIR_PARAMETER);
		String mstLockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
		
		String lastjobstatus="";
		String[] arr = ctl.split(conf.get(StrVal.SYSTEM_CONTROL_DELIMITER));
		String batchnumber = arr[0];

		Map systemmap = JobInfo.readCTLFile(conf,mstParaPath+conf.get(StrVal.SYS_DIR_DELIMITER)+batchnumber);
		if(new File(mstParaPath+conf.get(StrVal.SYS_DIR_DELIMITER)+batchnumber).exists()){
			systemmap = JobInfo.readCTLFile(conf,mstParaPath+conf.get(StrVal.SYS_DIR_DELIMITER)+batchnumber);
		}else{
			systemmap = new HashMap();
		}
		Map jobmap = null;
		if(new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()){
			jobmap = JobInfo.readCTLFile(conf,mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		}else{
			lastjobstatus = conf.get(StrVal.SYSTEM_STATUS_FAIL);
			endjob(ctl,lastjobstatus);
			return;
		}
		String cmd = "";
		
		if(systemmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_CMD))){
			cmd = (String) systemmap.get(conf.get(StrVal.SYSTEM_PARAMETER_CMD));
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info(ctl+" system cmd ["+cmd+"].");
			}
		}else{
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info(ctl+" not exists System cmd.");
			}
		}
		if(jobmap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_CMD))){
			cmd = (String) jobmap.get(conf.get(StrVal.SYSTEM_PARAMETER_CMD));
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Job cmd ["+cmd+"].");
			}
		}else{
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info(ctl+" not exists job cmd.");
			}
		}
		if(cmd.length()<1){
			String apppath =  conf.get(StrVal.SYS_DIR_SCRIPT);
			String[] arrS = ctl.split(conf.get(StrVal.SYSTEM_CONTROL_DELIMITER));
			String ctljob = arrS[1];
			String[] arrjob = ctljob.split("_");
			String pools = arrjob[0];
			String sys = arrjob[1];
			String tmpjob = "";
			for(int i=2;i<arrjob.length;i++){
				tmpjob += arrjob[i]+"_";
			}
			String job = tmpjob.substring(0,tmpjob.length()-1);
			String scriptPath = apppath + conf.get(StrVal.SYS_DIR_DELIMITER) + pools + conf.get(StrVal.SYS_DIR_DELIMITER) + sys + conf.get(StrVal.SYS_DIR_DELIMITER) + job +conf.get(StrVal.SYS_DIR_DELIMITER)+"bin";
			File scriptf = new File(scriptPath);
			if(scriptf.exists()){
				String[] scriptlst = scriptf.list();
				if(scriptlst.length>0){
					Arrays.sort(scriptlst);
				}
				for(int k=0;k<scriptlst.length;k++){
					String tmp1file = scriptlst[k];
					if(tmp1file.endsWith(".pl")){
						cmd += "perl "+scriptPath+conf.get(StrVal.SYS_DIR_DELIMITER)+tmp1file+" "+conf.get(StrVal.SYSTEM_PARAMETER_FORMAT)+" "+conf.get(StrVal.SYS_CMD_DELIMITER);
					}else if(tmp1file.endsWith(".sh")){
						cmd += "sh "+scriptPath+conf.get(StrVal.SYS_DIR_DELIMITER)+tmp1file+" "+conf.get(StrVal.SYSTEM_PARAMETER_FORMAT)+" "+conf.get(StrVal.SYS_CMD_DELIMITER);
					}else if(tmp1file.endsWith(conf.get(StrVal.SYS_SCRIPT_ENDWITH))){
						cmd += conf.get(StrVal.SYS_SCRIPT_ENDWITH_CMD)+" "+scriptPath+conf.get(StrVal.SYS_DIR_DELIMITER)+tmp1file+" "+conf.get(StrVal.SYSTEM_PARAMETER_FORMAT)+" "+conf.get(StrVal.SYS_CMD_DELIMITER);
					}
					if(!new File(scriptPath).exists()){
						log.info("Script not exists:"+scriptPath+conf.get(StrVal.SYS_DIR_DELIMITER)+tmp1file+".");
						endjob(ctl,conf.get(StrVal.SYSTEM_STATUS_FAIL));
						return ;
					}
				}
			}else{
				if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
					log.info(scriptPath+" not exists.");
				}
			}
		}
		if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
			log.info(ctl+" execute cmd["+cmd+"].");
		}
		cmd = replaceVarValue(cmd,conf.get(StrVal.SYSTEM_KEYWORK_CTL),ctl);
		String txdate = "";
		//system
		Iterator itersys = systemmap.entrySet().iterator();
		while(itersys.hasNext()){
			Entry entry = (Entry) itersys.next();
			String key = (String) entry.getKey();
			String val = (String) entry.getValue();
			cmd = replaceVarValue(cmd,key,val);
			if(key.equals(conf.get(StrVal.SYSTEM_PARAMETER_TXDATE))){
				txdate = val;
			}
		}
		//job
		Iterator iterjob = jobmap.entrySet().iterator();
		while(iterjob.hasNext()){
			Entry entry = (Entry) iterjob.next();
			String key = (String) entry.getKey();
			String val = (String) entry.getValue();
			cmd = replaceVarValue(cmd,key,val);
			if(key.equals(conf.get(StrVal.SYSTEM_PARAMETER_TXDATE))){
				txdate = val;
			}
		}
		SimpleDateFormat timestamp_start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date nowTimestart = new Date();
//		String starttime = timestamp_start.format(nowTimestart);
		if(txdate.length()<1){
			SimpleDateFormat yyyymmddfmt = new SimpleDateFormat("yyyyMMdd");
			//txdate replace
			cmd = replaceVarValue(cmd,conf.get(StrVal.SYSTEM_PARAMETER_TXDATE),yyyymmddfmt.format(nowTimestart));
			txdate = yyyymmddfmt.format(nowTimestart);
			JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_TXDATE)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+txdate+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		}
		//
		long  starttime = System.currentTimeMillis();
		JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+starttime+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		if(cmd.length()<1){
			log.info(ctl+" cmd not exists.");
			endjob(ctl,conf.get(StrVal.SYSTEM_STATUS_FAIL));
			return ;
		}
		String[] cmdarr = cmd.split(conf.get(StrVal.SYS_CMD_DELIMITER));
		
		boolean res = false;
		for(int j=0;j<cmdarr.length;j++){
			if(cmdarr[j].length()<1){
				continue;
			}
			log.info("System will executer command ["+cmdarr[j]+"].");
			res = execmd(cmdarr[j]);
			if(!res){
				break;
			}
		}
		SimpleDateFormat timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date nowTime = new Date();
		String timestamp_end = timestamp.format(nowTime);
		
		if(res){
			log.info(ctl+" running result "+conf.get(StrVal.SYSTEM_STATUS_SUCCESS)+".");
			JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_SUCCESS)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
			lastjobstatus = conf.get(StrVal.SYSTEM_STATUS_SUCCESS);
		}else{
			log.info(ctl+" running result "+conf.get(StrVal.SYSTEM_STATUS_FAIL)+".");
			JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_FAIL)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
			lastjobstatus = conf.get(StrVal.SYSTEM_STATUS_FAIL);
		}
		endjob(ctl,lastjobstatus);
	}

	public String getCtl() {
		return ctl;
	}

	public void setCtl(String ctl) {
		this.ctl = ctl;
	}
	
	public void endjob(String ctl,String statusval){
		String slvNodePath = slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node;
		String slvStatusPath = slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
		
		JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+statusval+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		long  endtime = System.currentTimeMillis();
		JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_ENDTIME)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+endtime+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
		
		log.info("***"+ctl+" end status:["+statusval+"],exit.");
		runningMap.remove(ctl);
	}

}
