/*
 * Copyright 2012-2013 The SCAPE Project Consortium
 * Author: William Palmer (William.Palmer@bl.uk)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package eu.scape_project.tb.chutney.jobs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import eu.scape_project.tb.chutney.FileTracker;
import eu.scape_project.tb.chutney.JMSComms;
import eu.scape_project.tb.chutney.Settings;
import eu.scape_project.tb.chutney.Tools;
import eu.scape_project.tb.chutney.Settings.JobType;
import uk.bl.dpt.qa.JP2Check;
import uk.bl.dpt.qa.JP2Profile;

/**
 * This class generates a report and zip file for previously executed XMLCommandLineJobs
 * @author wpalmer
 *
 */
public class XMLWorkflowReport implements ChutneyJob {

	/**
	 * Local temporary directory
	 */
	private String gTempDir = "";
	/**
	 * FileTracker
	 */
	private FileTracker gFileTracker;
	/**
	 * Was this workflow successful?  Default to true unless reason to be false
	 */
	private boolean gSuccess = true;
	/**
	 * Name of the output file
	 */
	String gOutputFile = "";
	
	/**
	 * Set up an XMLWorkflowReport
	 * @param pFileTracker FileTracker to use
	 */
	public XMLWorkflowReport(FileTracker pFileTracker) {
		gFileTracker = pFileTracker;
		gTempDir = gFileTracker.getLocalTempDir();
		gOutputFile = gTempDir+gFileTracker.getKeyFile()+".zip";
	}
	
	/**
	 * Setup the job
	 */
	public void setup() throws IOException {
		//do nothing
	}

	/**
	 * Run the report
	 */
	public void run() throws IOException {
		
		HashMap<String, String> status = new HashMap<String, String>();
		HashMap<String, String> checksums = new HashMap<String, String>();
		//this is a list so the files are always returned in the same order
		//which is important for zip generation
		List<String> generatedFiles = new ArrayList<String>();
		
		//receive all the data from JMS
		String key = gFileTracker.getHash();
		String message = JMSComms.receiveMessage(key);
		while(message!=null) {
			if(message.startsWith("FILE:")) {
				
				//do nothing with this message at the moment
				//when we try and use this data zipGeneratedFiles complains of 
				//duplicate entries
				@SuppressWarnings("unused")
				String file = message.substring(message.lastIndexOf("/")+1);

				//we could check that the file also exists in the filetracker here
				//but assume consistency for now

			}
			if(message.startsWith("SUCCESS:")) {
				String[] split = message.split(":");
				status.put(split[2], split[1]);
			}
			message = JMSComms.receiveMessage(key);
		} 
		
		//TODO: this currently doesn't work
		//JMSComms.deleteQueue(key);

		//FIXME: make sure to copy all the files to the local temp dir?
		//copy all the generated files in to generatedFiles
		for(String file:gFileTracker.getFileList()) {
			if(!generatedFiles.contains(file)) {
				generatedFiles.add(file);
				checksums.put(file, Tools.generateChecksum(gTempDir+file));
			}
		}
		
		//now we have received all the data, we can produce a report and zip the files
		String reportFile = gFileTracker.getKeyFile()+".report.xml";
		gSuccess = generateShortReport(gTempDir+reportFile, generatedFiles, status);
		checksums.put(reportFile, Tools.generateChecksum(gTempDir+reportFile));
		generatedFiles.add(reportFile);
		
		Tools.zipGeneratedFiles(gSuccess, checksums, generatedFiles, gOutputFile, gTempDir);
		
	}

	/**
	 * Generate a short report from the workflow data
	 * @param pReportFile filename to write report to
	 * @return whether report reports overall success or failure
	 * @throws IOException 
	 */
	private boolean generateShortReport(String pReportFile, List<String> pGeneratedFiles, HashMap<String, String> pStatus) throws IOException {
		boolean success = true;
		
		//HACK: this is a nasty hack (the jp2 bit at least)
		String file = gFileTracker.getKeyFile()+".jp2"+Settings.JPYLYZER_EXT;
		boolean jpylyzer = false;
		boolean generatedIsValid = false;
		boolean generatedMatchesInputProfile = false;
		if(pGeneratedFiles.contains(file)) {
			jpylyzer = true;
			generatedIsValid = JP2Check.jpylyzerSaysValid(gTempDir+file);
			generatedMatchesInputProfile = JP2Check.checkJpylyzerProfile(gTempDir+file, new JP2Profile());
		} 
		boolean matchbox = false;
		boolean ssimMatch = false;
		file = gFileTracker.getKeyFile()+Settings.MATCHBOX_COMP_SIFT_EXT;
		if(pGeneratedFiles.contains(file)) {
			matchbox = true;
			//note the following comparison (>0.9) is one used in Matchbox's MatchboxLib.py 
			ssimMatch = Tools.getSSIMCompareVal(gTempDir+file)>Settings.MATCHBOX_THRESHOLD;
		}
		
		BufferedWriter out = new BufferedWriter(new FileWriter(pReportFile));
		out.write("<?xml version='1.0' encoding='ascii'?>");out.newLine();
		out.write("<migrationReport>");out.newLine();
		if(jpylyzer) { 
			out.write("     <jpylyzerSaysOutputValid>"+generatedIsValid+"</jpylyzerSaysOutputValid>");
			out.newLine();
			out.write("     <outputMatchesInputProfile>"+generatedMatchesInputProfile+"</outputMatchesInputProfile>");
			out.newLine();
		}
		if(matchbox) { 
			out.write("     <ssimImageMatches>"+ssimMatch+"</ssimImageMatches>");
			out.newLine(); 
		}
		
		//go through the key set and look for failures
		for(String xml:pStatus.keySet()) {
			out.write("<xmljob>");
			out.write("<name>"+xml+"</name>");
			out.write("<success>"+pStatus.get(xml)+"</success>");
			out.write("</xmljob>");
			out.newLine();
			
			success &= new Boolean(pStatus.get(xml));
		}
		
		out.write("</migrationReport>");out.newLine();
		out.close();
		
		if(matchbox) {
			success &= ssimMatch;
		}
		if(jpylyzer) {
			success &= (generatedIsValid&generatedMatchesInputProfile);
		}
		return success;
	}
	
	/**
	 * Clean up after this job and the workflow
	 */
	public void cleanup() throws IOException {
		
		//delete the directory here as the workflow is over;
		//Tools.deleteDirectory(new File(tempDir));
		
		gFileTracker.deleteAllFiles();
		
		//delete files from HDFS?
		
	}

	/**
	 * Was this job successful?
	 * @return true or false
	 */
	public boolean wasSuccessful() {

		return gSuccess;
	}

	/**
	 * Get the name of the log file
	 * @return null (i.e. no log file)
	 */
	public String getLogFilename() {

		return null;
	}

	/**
	 * Get the full path of the output file(s)
	 * @return full path of the output file(s)
	 */
	public String[] getOutputFiles() {

		return new String[] { gOutputFile } ;
	}
	
	/**
	 * Get a list of input files
	 * @return null (i.e. no input files)
	 */
	public String[] getInputFiles() {
		return null;
	}

	public static JobType getJobType() {
		return JobType.XMLWorkflowReport;
	}

	public static String getShortJobType() {
		return "XWR";
	}

	
}
