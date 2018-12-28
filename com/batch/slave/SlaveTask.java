package com.batch.slave;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.batch.master.DefaultConf;
import com.batch.master.MasterTask;
import com.batch.util.Configuration;
import com.batch.util.JobInfo;
import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class SlaveTask {

	Map shareM = null;
	Configuration conf = null;
	Logger log = null;
	String node = null;
	ExecutorService fixThreadPool = null;
	String cfg = "";
	Map runningMap = null;
	final String clss = "slv";
    public void initialize(String node,String cfg){
		//
		conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		this.node = node;
		
		shareM = new ConcurrentHashMap();
		runningMap = new ConcurrentHashMap();
		shareM.put(StrVal.MAP_KEY_RUNNING, runningMap);
		fixThreadPool = Executors.newFixedThreadPool(Integer.parseInt(conf.get(StrVal.SYS_LIMIT_SLAVE_PRC)));
		
		//statistics resource submit to master
		SubmitResource sr = new SubmitResource();
		sr.initialize(node,cfg, shareM);
		new Thread(sr).start();
		long starttime = System.currentTimeMillis();
		while(true){
			//
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
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
			log = new LoggerUtil().getLoggerByName(conf.get("sys.log.path"), node+"_"+yyyymmdd);
			//schedule()
			schedule();
			//log.info("*********************** Sleep ["+conf.get(StrVal.SYS_HEARTBEADT_TIME)+"] ***********************.");
			try {
				Thread.sleep(Long.parseLong(conf.get(StrVal.SYS_HEARTBEADT_TIME)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
	}
    public void schedule(){
    	String slvPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_SLAVE);
		String mstPath = conf.get(StrVal.SYS_BATCH_PATH)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_INSTANCE)+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER);
		String mstLockPath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_MASTER_LOCK);
		String slvNodePath = slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node;
		String slvNodeStatusPath = slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
		String mstNoticePath = mstPath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_NOTICE);
		//lock
		if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
			log.info("Check Directory "+mstLockPath);
		}
		Map runningMap = (Map) shareM.get(StrVal.MAP_KEY_RUNNING);
		String[] mstLocklst = new File(mstLockPath).list();
		for(int i=0;i<mstLocklst.length;i++){
			String ctl = mstLocklst[i];
			if(new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).isDirectory()){
				continue;
			}
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check control "+ctl);
			}
			//chose server
			Map mstLockCtlMap = JobInfo.readCTLFile(conf,mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
			String mstseq = (String) mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE));
			if(!mstLockCtlMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_NODE))||
			   !mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE)).equals(node)){
				if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
					log.info(node+" run "+ctl+" on node "+mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_NODE))+" not local node.");
				}
				continue;
			}
			if(new File(slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()){
				Map tmpjobinfoMap = JobInfo.readCTLFile(conf,slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
				if(new File(slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()){
					if(tmpjobinfoMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))){
						String slvseq = (String) tmpjobinfoMap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE));
						if(slvseq.equals(mstseq)){
							if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
								log.info(node+" "+ctl+" running status ["+tmpjobinfoMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))+"].");
							}
							new File(slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).delete();
							log.info("Delete "+ctl+" from "+slvNodePath+".");
							continue;
						}else{
							if(runningMap.containsKey(ctl)){
								runningMap.remove(ctl);
							}
							new File(slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).delete();
							log.info("Delete "+ctl+" from "+slvNodeStatusPath+".");
							
							SlaveExecuter ser = new SlaveExecuter();
							ser.initialize(node,conf, shareM);
							ser.setCtl(ctl);
							long  starttime = System.currentTimeMillis();
							Map tmpMap = new ConcurrentHashMap();
							tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME),starttime);
							JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+starttime+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
							JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_NODE)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+node+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
							JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+mstseq+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
							JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_RUNNING)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
							
							String slvStatusPath = slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
							new File(slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).renameTo(new File(slvStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl));
							log.info( node +" move "+ctl+" from "+slvNodePath+" to "+slvStatusPath+".");
							runningMap.put(ctl, tmpMap);
							fixThreadPool.execute(ser);
							
						}
					}
				}else{
					if(tmpjobinfoMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))){
						String slvseq = (String) tmpjobinfoMap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE));
						if(slvseq.equals(mstseq)){
							if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
								log.info(node+" "+ctl+" running status ["+tmpjobinfoMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))+"].");
							}
							continue;
						}else{
							if(runningMap.containsKey(ctl)){
								runningMap.remove(ctl);
							}
							new File(slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).delete();
							log.info("Delete "+ctl+" from "+slvNodeStatusPath+".");
						}
				    }else{
				    	new File(slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).delete();
						log.info("Delete "+ctl+" from "+slvNodeStatusPath+".");
				    }
				}
			}else{
                if(new File(slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()){
                	if(runningMap.containsKey(ctl)){
						runningMap.remove(ctl);
					}
					
					SlaveExecuter ser = new SlaveExecuter();
					ser.initialize(node,conf, shareM);
					ser.setCtl(ctl);
					long  starttime = System.currentTimeMillis();
					Map tmpMap = new ConcurrentHashMap();
					tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME),starttime);
					JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+starttime+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
					JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_NODE)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+node+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
					JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+mstseq+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
					JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_RUNNING)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvPath+conf.get(StrVal.SYS_DIR_DELIMITER)+node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
					
					String slvStatusPath = slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+conf.get(StrVal.SYSTEM_VAR_STATUS);
					new File(slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).renameTo(new File(slvStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl));
					log.info( node +" Move "+ctl+" from "+slvNodePath+" to "+slvStatusPath+".");
					runningMap.put(ctl, tmpMap);
					fixThreadPool.execute(ser);
				}else{
					if(runningMap.containsKey(ctl)){
						runningMap.remove(ctl);
					}
				}
			}
		}
		//notice
		if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
			log.info("Check Directory "+mstNoticePath+".");
		}
		String[] mstNoticelst = new File(mstNoticePath).list();
		for(int i=0;i<mstNoticelst.length;i++){
			String ctl = mstNoticelst[i];
			if(new File(mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).isDirectory()){
				continue;
			}
			String slvCtlPath = slvPath + conf.get(StrVal.SYS_DIR_DELIMITER) + node+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl;
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check control "+ctl);
			}
			if(isResourceOK()){
				if(!new File(slvCtlPath).exists()){
					try {
						log.info(ctl+" submit ctl to "+slvCtlPath+".");
						new File(slvCtlPath).createNewFile();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}else{
					if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
						log.info(ctl+" has submit.");
					}
					continue;
				}
			}else{
				log.info(node+" not free resource ,ignore "+ctl);
			}
		}
		//node status
		if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
			log.info("Check Directory "+slvNodeStatusPath+".");
		}
		String[] slvStatuslst = new File(slvNodeStatusPath).list();
		for(int i=0;i<slvStatuslst.length;i++){
			String ctl = slvStatuslst[i];
			if(new File(slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).isDirectory()){
				continue;
			}
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check control "+ctl);
			}
			String slv2mstlockCtlPath = mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl;
			String slv2mstnoticeCtlPath = mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl;
			if(!new File(slv2mstlockCtlPath).exists()){
				log.info(ctl+" slave status exists,master lock not ctl,delete ctl.");
				new File(slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).delete();
				continue;
			}
			Map tmpjobinfoMap = JobInfo.readCTLFile(conf,slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
			if(tmpjobinfoMap.containsKey(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))&&
			   tmpjobinfoMap.get(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)).equals(conf.get(StrVal.SYSTEM_STATUS_RUNNING))&&
			   new File(mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).exists()&&
			   !runningMap.containsKey(ctl)){
				Map mstLockCtlMap = JobInfo.readCTLFile(conf,mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
				if(mstLockCtlMap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)).equals(tmpjobinfoMap.get(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)))){
					log.info(ctl+" update status from " +conf.get(StrVal.SYSTEM_STATUS_RUNNING)+" to "+conf.get(StrVal.SYSTEM_STATUS_FAIL)+",memory not exists.");
					JobInfo.writeCTLFile(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)+conf.get(StrVal.SYSTEM_KEYVAL_DELIMITER)+conf.get(StrVal.SYSTEM_STATUS_FAIL)+conf.get(StrVal.SYSTEM_ROW_DELIMITER), slvNodeStatusPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl);
				}
			}
		}
		//node
		if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
			log.info("Check Directory "+slvNodePath+".");
		}
		String[] slvNodelst = new File(slvNodePath).list();
		for(int i=0;i<slvNodelst.length;i++){
			String ctl = slvNodelst[i];
			if(new File(slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).isDirectory()){
				continue;
			}
			if(conf.get(StrVal.SYS_DEBUG_FLAG).equals(conf.get(StrVal.SYSTEM_SUCCESS_FLAG))){
				log.info("Check control "+ctl+".");
			}
			String slv2mstlockCtlPath = mstLockPath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl;
			String slv2mstnoticeCtlPath = mstNoticePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl;
			if((!new File(slv2mstlockCtlPath).exists())&&(!new File(slv2mstnoticeCtlPath).exists())){
				log.info(ctl+" slave node exists,master lock and notice path not ctl,delete ctl.");
				new File(slvNodePath+conf.get(StrVal.SYS_DIR_DELIMITER)+ctl).delete();
			}
		}
    }
    
    public boolean isResourceOK(){
    	int lmtprc = Integer.parseInt((String)conf.get(StrVal.SYS_LIMIT_SLAVE_PRC));
    	if(runningMap.size()>=lmtprc){
    		return false;
    	}
    	return true;
    }
    private static void printUsage(String node,String cfg) {
	    System.out.println("*******************************************************************");
	    System.out.println("*Usage: java -jar *.jar cfg node");
	    System.out.println("* node:"+node);
	    System.out.println("* cfg :"+cfg);
	    System.out.println("*******************************************************************");
	}
	public static void main(String[] args) {
		String cfg = args[0];//"D:/conf/core-batch.cfg";
		String node = args[1];//"slave-3";
		printUsage( node, cfg);
		SlaveTask st = new SlaveTask();
		st.setNode(node);
		st.initialize(node, cfg);
		//
	}
	public String getNode() {
		return node;
	}
	public void setNode(String node) {
		this.node = node;
	}
}
