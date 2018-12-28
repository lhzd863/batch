package com.batch.util;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class DBConnect {

	private String dbType;
	private String ip;
	private String port;
	private String schema;
	private String user;
	private String passwd;
	private String driver;
	private String dbURL;
    
    private String dbUser = user;
    private String dbPassword = passwd;
    
	public DBConnect(String ipcfg,String portcfg,String schemacfg,String usercfg,String passwdcfg,String dbType) {
		this.ip = ipcfg;
		this.port = portcfg;
		this.schema = schemacfg;
		this.user = usercfg;
		this.passwd = passwdcfg;
		this.dbType=dbType;
	    
		try {
			invokeMethod(this,"set"+dbType.trim().toUpperCase()+"Driver",new String[]{});
			invokeMethod(this,"set"+dbType.trim().toUpperCase()+"URL",new String[]{});
		} catch (Exception e) {
			e.printStackTrace();
		}		
	    dbUser = user;
	    dbPassword = passwd;
	}
    
	public String getDatabaseType(){
		return dbType;
	}
	public String getDriver() {
		return driver;
	}

	public String getDbURL() {
		return dbURL;
	}

	public String getDbUser() {
		return dbUser;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

    public Connection  getDBConnection() {		
		Connection conn = null;
		
		 try {
			Class.forName(getDriver());
			conn = DriverManager.getConnection(getDbURL(), getDbUser(),getDbPassword());			
		} catch (ClassNotFoundException e) {
				e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;		
	}
    
    
    /**   
     * 执行某对象方法   
     *   
     * @param owner   
     *            对象   
     * @param methodName   
     *            方法名   
     * @param args   
     *            参数   
     * @return 方法返回值   
     * @throws Exception   
     */    
    public Object invokeMethod(Object owner, String methodName, Object[] args)     
            throws Exception {     
        Class ownerClass = owner.getClass();     
    
        Class[] argsClass = new Class[args.length];     
    
        for (int i = 0, j = args.length; i < j; i++) {     
            argsClass[i] = args[i].getClass();     
        }     
    
        Method method = ownerClass.getMethod(methodName, argsClass);     
    
        return method.invoke(owner, args);     
    }
    
    
	public void setMYSQLDriver() {
		this.driver = "com.mysql.jdbc.Driver";
	}
	public void setORACLEDriver() {
		this.driver = "oracle.jdbc.driver.OracleDriver";
	}
	public void setDB2Driver() {
		this.driver = "";
	}
	public void setTERADATADriver() {
		this.driver = "com.teradata.jdbc.TeraDriver";
	}
	public void setPOSTGRESDriver() {
		this.driver = "org.postgresql.Driver";
	}
	public void setMYSQLURL() {
		this.dbURL = "jdbc:mysql://"+ip+"/"+schema;
	}
	public void setORACLEURL() {
		this.dbURL = "jdbc:oracle:thin:@"+
		        "(DESCRIPTION =  "+
		        "(ADDRESS = (PROTOCOL = TCP)(HOST = "+ip+")(PORT = "+port+"))"+
		        "(CONNECT_DATA ="+
		        "(SERVER = DEDICATED)"+
		        "(SERVICE_NAME = "+schema+")"+
		        ")"+
		        ")";
	}
	public void setDB2URL() {
		this.dbURL = null;
	}
	public void setTERADATAURL() {
		this.dbURL =  "jdbc:teradata://"+ip+"/CLIENT_CHARSET=cp936";
	}
	public void setPOSTGRESURL() {
		this.dbURL = "jdbc:postgresql://"+ip+":"+port+"/"+schema;
	}
}
