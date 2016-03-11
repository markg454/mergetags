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
	private static String vocabulary			= null;
	private static int id						= 0;
	private static String tag					= null;
	private static String synonym				= null;
	private static String parentTerm			= null;
	private static String term					= null;
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

    // set vocabulary
    public void setVocabulary(String c) {
    	vocabulary = c;
    }
    
    // get vocabulary
    public String getVocabulary() {
    	return vocabulary;
    }
    
    // set synonym
    public void setSynonym(String k) {
    	synonym = k;
    }
    
    // get synonym
    public String getSynonym() {
    	return synonym;
    }
 
    // set parent term
    public void setParentTerm(String t) {
    	parentTerm = t;
    }
    
    // get parent term
    public String getParentTerm() {
    	return parentTerm;
    }
 
    // set term
    public void setTerm(String t) {
    	term = t;
    }
    
    // get term
    public String getTerm() {
    	return term;
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
     
}
