package com.yayhi.tags;

import com.Ostermiller.util.CSVPrinter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;

public class CSVFileWriter {

	// private variables
	private String csvFileWriterFilePath			= null;
	private File csvFileWriterFile					= null;
	private String[] csvFileWriterColumns			= null;
	private String[] csvFileWriterRowData			= null;
	private FileOutputStream fos					= null;
	private CSVPrinter csvp							= null;
	
	public CSVFileWriter(String path) {
		
		// set csv file path
		csvFileWriterFilePath = path;
		
		// create a file
		csvFileWriterFile = new File(csvFileWriterFilePath);
		
		// open the output file stream for writing
	    try {
	    	fos = new FileOutputStream(csvFileWriterFilePath);
	    } catch (IOException e) {
	      System.out.println(e);
	    }
	    
    	// create new CSV Printer
    	csvp = new CSVPrinter(fos);

	}
	
	// get file path
	public String getFilePath() {
		return csvFileWriterFilePath;
	}
	
	// set file path
	public void setFilePath(String path) {
		csvFileWriterFilePath = path;
	}
	
	// get file 
	public File getFile() {
		return csvFileWriterFile;
	}
	
	// set file 
	public void setFile(File f) {
		csvFileWriterFile = f;
	}
	
	// get columns
    public String[] getCSVFileWriterColumns() {
        return csvFileWriterColumns;
    }
    
    // set columns
    public void setCSVFileWriterColumns(String[] columnstrs) {
        csvFileWriterColumns = columnstrs;
    }
    
    // get row data
    public String[] getCSVFileWriterRowData() {
        return csvFileWriterRowData;
    }
    
    // set row data
    public void setCSVFileWriterRowData(String[] datastr) {
        csvFileWriterRowData = datastr;
    }
    
    // close the output stream
    public void close() {

    	try {
    		csvp.close();
    	} catch (IOException e) {
    		System.out.println(e);
    	}
    	
    	try {
    		fos.close();
    	} catch (IOException e) {
    		System.out.println(e);
    	}
    }
    
    // write a line to the CSV file
    public boolean write(String[] data) {
    	
    	boolean success = false;
    	
    	// write out the column headers
	    try {
	    	csvp.writeln(data);
	    	success = true;
	    } catch (IOException e) {
	    	System.out.println(e);
  	    }
    	
	    return success;
	    
    }
}
