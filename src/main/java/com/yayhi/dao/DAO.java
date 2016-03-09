package com.yayhi.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.yayhi.utils.YProperties;

/**
 * -----------------------------------------------------------------------------
 * @version 1.0
 * @author  Mark Gaither
 * @date	Mar 7 2016
 * -----------------------------------------------------------------------------
 */

public class DAO {

	private String driverClass        		= null;
	private String connectionURL  			= null;
	private String userPassword       		= null;
	private String userID             		= null;
    Connection daoConnection				= null;
    private boolean debug					= false;
    private boolean autoCommit				= false;
    private Properties appProps				= null;
    private YProperties iProps				= null;
    
    // Dynamically create the Direct Access Object.
    public DAO() {

   		iProps = new YProperties();

    	try {
    		appProps = iProps.loadProperties();
    	} catch (IllegalArgumentException iae) {
    		System.err.println("Unable to load the appropriate property file: " + iProps.getPropertyFileName() + " " + iae.getMessage());
    		System.exit(1);
    	}
    	
    	// set up private variables from properties files
    	driverClass = appProps.getProperty("driverClass");
    	connectionURL = appProps.getProperty("connectionURL");
    	userPassword = appProps.getProperty("userPassword");
    	userID = appProps.getProperty("userID");
    	debug = Boolean.parseBoolean(appProps.getProperty("debug"));
    	
    	// set debug level for all other classes that extend this class
    	setDebug(debug);
    	
    	if (debug) {
    		System.out.println("   propertyFileName: " + iProps.getPropertyFileName());
        	System.out.println("   appProps driverClass:  " + appProps.getProperty("driverClass"));
        	System.out.println("   appProps connectionURL:  " + appProps.getProperty("connectionURL"));
        	System.out.println("   appProps userPassword:  " + appProps.getProperty("userPassword"));
        	System.out.println("   appProps userID:  " + appProps.getProperty("userID"));
        	System.out.println("   appProps debug:  " + getDebug());
        }
    	
    }
    
    public void open() throws SQLException {
    	
        Connection con = null;

        try {

            Class.forName(driverClass).newInstance();
            con = DriverManager.getConnection(connectionURL, userID, userPassword);
            
            daoConnection = con;
            daoConnection.setAutoCommit(autoCommit);
            

        } catch (ClassNotFoundException e) {

            e.printStackTrace();
       
        } catch (InstantiationException e) {

            e.printStackTrace();

        } catch (IllegalAccessException e) {

            e.printStackTrace();

        } catch (SQLException e) {

            e.printStackTrace();

            if (con != null) {
                try {
                    con.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }

        } 

    }

    public void close() throws SQLException {
        try {
            if (this.daoConnection != null && !this.daoConnection.isClosed())
                this.daoConnection.close();
        }
        catch(SQLException e) { throw e; }
    }
    
    // get the DAO connection
    public Connection getDAOConnection() {
    	return daoConnection;
    }
    // set the DAO connection
    public void setDAOConnection(Connection c) {
    	daoConnection = c;
    }
    
	// set debug
	public void setDebug(boolean d) {
		debug = d;
	}

	// get debug
	public boolean getDebug() {
		return debug;
	}

    
}