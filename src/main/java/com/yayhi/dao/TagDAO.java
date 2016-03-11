package com.yayhi.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.yayhi.tags.Tag;

/**
 * -----------------------------------------------------------------------------
 * @version 1.0
 * @author  Mark Gaither
 * @date	Mar 7 2016
 * -----------------------------------------------------------------------------
 */

public class TagDAO {

    static boolean debug			= true;
    DAO dao							= null;
    
    // create the Tag Direct Access Object.
    public TagDAO() {   	
        dao = new DAO();     
    }
    
    // commit a set of inserts
    public void commit() throws SQLException { 
    	dao.getDAOConnection().commit(); 	
    }
    
   // get parent and term for synonym
   public ArrayList<String> getTerms(String k) throws SQLException {
   	
	   ArrayList<String> tagList = new ArrayList<String>();
	   
	   dao.open();
	   Connection con = dao.getDAOConnection();
	   
	   

	   if (debug) {
		   System.out.print("get Terms\n");
		   System.out.print("  keyword: " + k + "\n");
	   }

	   Statement stmt = null;
	   ResultSet rset = null;
	   String query = "SELECT parent_term, term FROM map WHERE synonym = '" + k.trim() + "'";

	   if (debug) {
		   System.out.print("TagDAO: getTerms: query: " + query);
	   }

	   stmt = con.createStatement();

	   rset = stmt.executeQuery(query);

	   // set Tag data
	   while (rset.next()) {

		   if (debug) {
			   System.out.println("  Results...");
			   System.out.println("      parent			 	-> " + rset.getString("parent_term"));
			   System.out.println("      term				-> " + rset.getString("term"));
			   System.out.println("      keword				-> " + k.trim());
		   }

		   tagList.add(rset.getString("parent_term"));
		   tagList.add(rset.getString("term"));
	
	   }

	   rset.close();

	   stmt.close();

	   dao.close();

	   return tagList;

   }
    
}
