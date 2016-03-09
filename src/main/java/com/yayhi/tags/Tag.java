package com.yayhi.tags;

import java.io.IOException;
import java.sql.SQLException;

import com.yayhi.dao.TagDAO;
import com.yayhi.utils.YLogger;

/**
 * -----------------------------------------------------------------------------
 * @version 1.0
 * @author  Mark Gaither
 * @date	Mar 7 2016
 * -----------------------------------------------------------------------------
 */

public class Tag {

	private static boolean debug				= false;
	private static TagDAO tagDAO				= null;
	private static String concern				= null;
	private static int id						= 0;
	private static String tag					= null;
	private static String keyword				= null;
    private static YLogger logger				= null;
	
	//*********************************************************************************************
    //* Tag constructor 
    //*********************************************************************************************
    public Tag() throws SQLException {
    	
    	// create new DAO
    	tagDAO = new TagDAO();       
        
    }
    
    // set Tag DAO
    public void setTagDAO(TagDAO dao) {
    	tagDAO = dao;
    }
    
    // get Tag DAO
    public TagDAO getTagDAO() {
    	return tagDAO;
    }

    // set concern
    public void setConcern(String c) {
    	concern = c;
    }
    
    // get concern
    public String getConcern() {
    	return concern;
    }
    
    // set keyword
    public void setKeyword(String k) {
    	keyword = k;
    }
    
    // get keyword
    public String getKeyword() {
    	return keyword;
    }
    
    // set tag
    public void setTag(String t) {
    	tag = t;
    }
    
    // get tag
    public String getTag() {
    	return tag;
    }
	
    // set id
    public void setID(int i) {
    	id = i;
    }
    
    // get id
    public int getID() {
    	return id;
    }
	
	// set debug
	public void setDebug(boolean d) {
		debug = d;
	}

	// get debug
	public boolean getDebug() {
		return debug;
	}

	// set logger
	public void setLogger(YLogger l) {
		logger = l;
	}

	// get logger
	public YLogger getLogger() {
		return logger;
	}

	public boolean commit() throws IOException {

        boolean returnVal = false;
        try {
            tagDAO.commit(this);
            returnVal = true;
        } catch (SQLException e) {
                ;
        }

        return returnVal;

	}
	
    public boolean load() {

    	boolean returnVal = false;
    	
    	// read data from the database
    	try {
    		
    		//if (tagDAO.read(this,ownerAlphaCode))
    		if (tagDAO.read(this,id))
    			returnVal = true;
    		else {
    			if (debug) {
    	    		System.out.println("Tag: load failed for: " + id + "\n");
    	    	}
    		}
    		
    	} catch (SQLException e) {
            e.printStackTrace();
        }

    	if (debug) {
    		System.out.println("Tag: id: " + id + "\n");
    	}
    	
    	return returnVal;
    	
    }
     
}
