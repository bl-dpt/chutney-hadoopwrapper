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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipFile;

import eu.scape_project.tb.chutney.Settings;
import eu.scape_project.tb.chutney.Tools;
import eu.scape_project.tb.chutney.Settings.JobType;

/**
 * A class to run a Taverna workflow via the command line
 * @author wpalmer
 *
 */
public class TavernaCommandLineJob implements ChutneyJob {

	/**
	 * Name of the workflow to use
	 */
	private String gWorkflow = "";
	/**
	 * Names of input ports and input values
	 */
	private HashMap<String, String> gInputValues;
	/**
	 * Temporary directory to use
	 */
	private String gTempDir = "";
	/**
	 * Directory to store outputs
	 */
	private String gOutputDir = "";

	/**
	 * Constructor for job
	 * @param pList input ports and input values
	 * @param pWorkflow workflow to use
	 * @param pTempDir local temporary directory to use
	 */
	public TavernaCommandLineJob(HashMap<String, String> pList, String pWorkflow, File pTempDir) {
		gInputValues = pList;
		this.gWorkflow = pWorkflow;
		this.gTempDir = pTempDir.toString()+"/";
		gOutputDir = this.gTempDir+"output/";
	}

	/**
	 * Set up the job
	 */
	public void setup() throws IOException {

		//do nothing
		
	}
		
	/**
	 * Query whether the job run was successful
	 * @return true or false
	 */
	public boolean wasSuccessful() {
		//default to true, unless we find a reason for failure
		boolean success = true;
		
		for(String file:getOutputFiles()) {
			//check if the expected output file exists
			if(!(new File(file).exists())) {
				success = false;
				continue;
			}
			//check if the output ports end with an error (i.e. not successful)
			if(file.toLowerCase().endsWith(".error")) {
				success = false;
				continue;
			}
			//if there is a zip file then query the contents
			if(file.toLowerCase().contains("zip")) {
				//assume this is a zip file
				try {
					ZipFile zip = new ZipFile(file);
					if(zip.getEntry("FAILURE") != null) {
						//i.e. this zip file has a FAILURE entry
						success = false;
					}
					zip.close();
				} catch (IOException e) { }
			}
		}
		return success;
	}
	
	/**
	 * Get a list of all the output files
	 * @return full path name to all output files (note these are named the same as the output ports
	 * in the workflow)
	 */
	public String[] getOutputFiles() {
		String[] files = new String[Settings.TAVERNA_WORKFLOW_OUTPUTPORTS.length];
		for(int i=0;i<Settings.TAVERNA_WORKFLOW_OUTPUTPORTS.length;i++) {
			files[i] = gOutputDir+Settings.TAVERNA_WORKFLOW_OUTPUTPORTS[i];
			File out = new File(files[i]);
			if(!out.exists()||out.isDirectory()) {
				//this file does not exist, or it is a directory (indicating failure) so try .error
				files[i] += ".error";
			}
		}
		//System.out.println("Output file: "+files[0]);
		return files;
	}

	/**
	 * Get a list of all the input files
	 * @return null always null
	 */
	public String[] getInputFiles() {
		return null;
	}

	/**
	 * Clean up temporary files 
	 */
	public void cleanup() {
		//delete all the generated files - all being in the same directory makes this easy
		//DO NOT DELETE THE INPUT FILE WHEN RUNNING FROM THIS CLASS' MAIN!
		Tools.deleteDirectory(new File(gTempDir));			
	}

	/**
	 * Run the job
	 */
	public void run() throws IOException {
		
		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(Settings.TAVERNA_SHELL);
		commandLine.add(Settings.TAVERNA_COMMAND);
		commandLine.add(Settings.TAVERNA_OPTIONS);
		
		//now set inputs to workflow ports
		for(String key : gInputValues.keySet()) {
			commandLine.add("-inputvalue");
			commandLine.add(key);
			String value = gInputValues.get(key);
			//if this is the input port - add the full path to the file
			//if(key.equals(Settings.TAVERNA_WORKFLOW_INPUTFILEPORT)) {
				//note we need to add tempdir to the inputs here
			//	value = tempDir+value;
			//}
			commandLine.add(value);
		}
		
		//set the output directory so we can capture the outputs
		commandLine.add("-outputdir");
		//outputDir should not exists at this point, otherwise Taverna doesn't like it
		commandLine.add(gOutputDir);
		commandLine.add(gWorkflow);
		
		for(String s:commandLine) System.out.print(s+",");
		System.out.println();
		
		ProcessBuilder pb = new ProcessBuilder(commandLine);
		//set the working directory to our temporary directory
		pb.directory(new File(gTempDir));
		//make sure that the temporary files are put in to a directory that we will later delete
		pb.environment().put("TMP", gTempDir);
		pb.environment().put("TEMP", gTempDir);
		//this sets the environment variable that will be picked up by Taverna when executed in the shell
		//this is the only way to do it as java.io.tmpdir is hardcoded to /tmp
		pb.environment().put("_JAVA_OPTIONS","-Djava.io.tmpdir="+gTempDir);
		
		//this redirects stderr to stdout
		pb.redirectErrorStream(true);
		//start the executable
		Process proc = pb.start();
		//create a log of the console output
		BufferedReader logBuf = new BufferedReader(new InputStreamReader(proc.getInputStream()));

		try {
			//wait for process to end before continuing
			proc.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		//write a local log file
		//HACK: get the name
		String localLogFile = getLogFilename();
		
		//store the log file of the console output
		BufferedWriter logFile = new BufferedWriter(new FileWriter(localLogFile));
		//write the command line to the file
		logFile.write(commandLine.toString());
		//this adds extra bytes - FIXME (unicode->ascii problem?)
		logFile.write("Exitcode: "+proc.exitValue());
		logFile.write("--------------------------------------");
		//write the log of stdout and stderr to the logfile
		char[] readBuffer = new char[Settings.BUFSIZE];
		int bytesRead = 0;
		while(logBuf.ready()) {
			bytesRead = logBuf.read(readBuffer);
			logFile.write(readBuffer, 0, bytesRead);
		}
		logFile.close();
		
	}

	/**
	 * Gets the full path to the log file
	 * @return full path to the log file
	 */
	public String getLogFilename() {
		return (gTempDir + new File(gInputValues.get(Settings.TAVERNA_WORKFLOW_INPUTFILEPORT)).getName()+".log");
	}
	
	/**
	 * This is a test main() - the class will usually be used in TavernaHadoopWrapper
	 * @param args command line arguments
	 * @throws IOException file access error
	 */
	public static void main(String[] args) throws IOException {
		
	}

	public static JobType getJobType() {
		return JobType.TavernaCommandLine;
	}

	public static String getShortJobType() {
		return "TCL";
	}

}
