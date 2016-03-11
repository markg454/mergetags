package com.yayhi.tags;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.Ostermiller.util.ExcelCSVParser;
import com.yayhi.dao.TagDAO;
import com.yayhi.utils.YLogger;
import com.yayhi.utils.YProperties;

/**
 * -----------------------------------------------------------------------------
 * @version 1.0
 * @author  Mark Gaither
 * @date	Aug 19, 2010
 * -----------------------------------------------------------------------------
 */

public class Merger {

	private static YProperties iProps				= null;
	private static Properties appProps				= null;
    private static String logFilePath				= null;
    private static Boolean logIt					= null;
    private static String inputCSVFilePath			= null;
    private static String outputCSVFilePath			= null;
    private static boolean debug 					= false;
    private static boolean test						= false;
    private static boolean verbose					= false;
    private static YLogger logger					= null;
    private static boolean hasErrors				= false;
    
    // constructor
    Merger() {
    	
    }
    
	// check if string is not empty
	public static boolean isEmpty(String s) {
    	
    	boolean empty = false;
    	
    	Pattern p = Pattern.compile("^[ ]*$");
		
		Matcher m = p.matcher(s);

		if (m.find()) {
			empty = true;
		}
		
		return empty;
		
    }

    /**
     * Sole entry point to the class and application.
     * @param args Array of String arguments.
     * @exception java.lang.InterruptedException
     *            Thrown from the Thread class.
     * @throws SQLException 
     */
    public static void main(String[] args) throws Exception {
    	
    	//*********************************************************************************************
        //* Get Command Line Arguments - overwrites the properties file value, if any
        //*********************************************************************************************
    	
    	String usage = "Usage:\n" + "java -jar merger.jar [-i INPUT_CSV] [-o OUTPUT_CSC] [-t] (TEST and validate the input file but don't database inserts - optional) [-v] (VERBOSE mode - optional) [-d] (DEBUG - optional)\n";
    	String example = "Example:\n" + "java -jar merger.jar -i /tmp/tags.csv -o /tmp/out.csv\n" +
    			"java -jar merger.jar -i /tmp/tags.csv -o /tmp/out.csv -d\n" +
    			"java -jar merger.jar -i /tmp/tags.csv -o /tmp/out.csv -t\n" +
    			"java -jar merger.jar -i /tmp/tags.csv -o /tmp/out.csv -v -d\n";

        // get command line arguments
    	if (args.length >= 4) {
    		
	    	for (int optind = 0; optind < args.length; optind++) {
	    	    
	    		if (args[optind].equals("-i")) {
	    			inputCSVFilePath = args[++optind];
				} else if (args[optind].equals("-o")) {
	    			outputCSVFilePath = args[++optind];
				} else if (args[optind].equals("-d")) {
					debug = true; 
				} else if (args[optind].equals("-v")) {
					verbose = true; 
		    	} else if (args[optind].equals("-t")) {
					test = true;
		    	}
	    		
	    	}
        }
        else {
        	
        	System.err.println(usage);
        	System.err.println(example);
            System.exit(1);
            
        }

    	//*********************************************************************************************
        //* Get Properties File Data
        //*********************************************************************************************
    	iProps = new YProperties();
    	appProps = iProps.loadProperties();
    	
    	// set log file path
    	logFilePath = appProps.getProperty("logFilePath");
    	logIt = Boolean.valueOf(appProps.getProperty("logIt"));
    	
    	// allow command line setting of debug to over rule the properties setting
    	if (!debug)
    		debug = Boolean.parseBoolean(appProps.getProperty("debug"));
    	
    	if (debug) {
    		
			System.out.println("logFilePath: " + logFilePath);
			System.out.println("logIt: " + logIt);
			System.out.println("input CSV file: " + inputCSVFilePath);
			System.out.println("output CSV file: " + outputCSVFilePath);
			
    	}
    	
    	//*********************************************************************************************
        //* Set up logging
        //*********************************************************************************************
    	// create string of todays date and time
    	Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_hhmmss");

        String loggerFilePath = appProps.getProperty("logFilePath") +  "/mergetags_" + sdf.format(cal.getTime()) + ".log";
        // log data, if true
        if (logIt.booleanValue()) {
            	
        	// open log file
    	    try {
    	    	logger = new YLogger(loggerFilePath);
    	    } catch (IOException e) {
    	    	System.out.println("exception: " + e.getMessage());
    	    }
    	    
        }
        
        // output run information to stdout
        if (verbose) {
        	if (test) {
	    		System.out.println("\tRUN MODE =\t\tTest and Validate");
	    	}
	    	else {
	    		System.out.println("\tRUN MODE =\t\tInsert");
	    	}
        }
        
        // log run information   
	    try {
	    	
	    	if (test) {
	    		logger.write("\tRUN MODE =\t\tTest and Validate");
	    	}
	    	else {
	    		logger.write("\tRUN MODE =\t\tInsert");
	    	}
	    	logger.write("\tINPUT CSV =\t\t" + inputCSVFilePath);

			
  	    } catch (IOException e) {
  	    	e.printStackTrace();
  	    }	    
	    
	   	//*********************************************************************************************
        //* Read input CSV
        //*********************************************************************************************
    	
    	// Create the csv input file
		//File input fileFile = new File(inputCSVFilePath);

		String[][] lines = null; // lines of input csv file
		
		// Read contents of the input file file
		BufferedReader br = null;
		String fileContents = "";
		
		try {
 
			String sCurrentLine;
 
			br = new BufferedReader(new FileReader(inputCSVFilePath));
			int lineCount = 0;
			
			while ((sCurrentLine = br.readLine()) != null) {

				if (lineCount == 0 && sCurrentLine.contains("keywords")) { // skip header line if it exists
					if (debug)
						System.out.println("Skipping header ...");
				}
				else {
					
					if (debug) {
						System.out.println("current input file line " + sCurrentLine);
					}

					// build file contents string to give to the parser
					if (lineCount == 0)
						fileContents = sCurrentLine;
					else
						fileContents = fileContents + "\n" + sCurrentLine;
					
				}
				lineCount++;
				
			}
 
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(1);
			}
		}
        
		// Parse the CSV data
		try {
			lines = ExcelCSVParser.parse(fileContents);				
		} 
		catch(Exception e){
			System.err.println(e.getMessage()); 
			e.printStackTrace();
			System.exit(1);
		}
		
		//*********************************************************************************************
        //* Merge incoming CSV with new tag set from database
        //*********************************************************************************************    	
		if (debug) {
			System.out.println("Count of parsed lines: " + lines.length);
		}
		
		// create output CSV file
    	CSVFileWriter fw = new CSVFileWriter(outputCSVFilePath);
    	
		// write the header column data
        fw.write(new String[] { "id","tags","keywords" });
		
		int tCount = 1; //    used to count number of operations
		for (int i = 0; i < lines.length; i++) {
	
			hasErrors = false;
			
			// id
			String idStr = lines[i][0];
			
			// keywords
			String keywordsStr = lines[i][1];
			
			if (debug) {
	    		
				System.out.println("-----------");
				System.out.println("idStr: " + idStr.trim());
				System.out.println("keywordsStr: " + keywordsStr.trim());
		    	
			}
			
			// parse out keywords list of strings
			String[] keywords = null;
			if (keywordsStr.contains(",")) {
				
				keywords = keywordsStr.split(",");
				if (debug) {
					System.out.println("keywords: " + Arrays.toString(keywords));
				}

			   	// create database connection object
				TagDAO  dao = new TagDAO();

				//*********************************************************************************************
	            //* Create list of tags for given keywords for the current title
	            //*********************************************************************************************
				ArrayList <String>tags = new ArrayList<String>();
				for (String keyword : keywords) {
					
					if (debug) {
						System.out.println("keyword: " + keyword.trim());
					}
				
					// get terms for synonym, if any
					ArrayList<String> termList = dao.getTerms(keyword.trim().replace("'", "\\'"));
					
					// add found terms
					if (!termList.isEmpty()) {
						
						String pt =  termList.get(0); // parent term
						String t =  termList.get(1); // term
						
						if (debug) {
							System.out.println("\nkeyword: " + keyword.trim() + " parent found: " + pt + " term found: " + t);
						}
						
						// make sure not include a duplicate parent term
						if (!tags.contains(pt)) {
							tags.add(pt); // add found parent term
						}
						
						// make sure not include a duplicate term
						if (!tags.contains(t)) {
							tags.add(t); // add found term
						}	
					
					} else {
			    		logger.write("\tKEYWORD NOT MAPPED: " + keyword.trim());
					}
					
				}	
				
				if (debug) {
					System.out.println("id: " + idStr + " keywords: " + keywordsStr + " tags: " + tags);
				}
				
				StringBuilder sb = new StringBuilder();
				int sCount = 0;
				for (String s : tags)
				{
  
				    if (sCount == 0) {
				    	sb.append(s);
				    } else {
				    	sb.append(",");
				    	sb.append(s);
				    }
				    sCount++;
				}
				
	        	if (!fw.write(new String[] { idStr, sb.toString(), keywordsStr })) {	        		
	        		// send error message	        		
	        	}
				
			}
			
		}
		// close the output stream
    	fw.close();
    	
    	logger.close();
    	
    }
    
}

