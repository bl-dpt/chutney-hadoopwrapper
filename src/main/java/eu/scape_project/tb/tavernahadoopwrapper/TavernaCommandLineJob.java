/*
 * Copyright 2012 The SCAPE Project Consortium
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

package eu.scape_project.tb.tavernahadoopwrapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipFile;

/**
 * A class to run a Taverna workflow via the command line
 * @author wpalmer
 *
 */
public class TavernaCommandLineJob implements HadoopJob {

	/**
	 * Name of the workflow to use
	 */
	private String workflow = "";
	/**
	 * Names of input ports and input values
	 */
	private HashMap<String, String> inputValues;
	/**
	 * Temporary directory to use
	 */
	private String tempDir = "";
	/**
	 * Directory to store outputs
	 */
	private String outputDir = "";

	/**
	 * Constructor for job
	 * @param list input ports and input values
	 * @param workflow workflow to use
	 * @param tempDir local temporary directory to use
	 */
	public TavernaCommandLineJob(HashMap<String, String> list, String workflow, File tempDir) {
		inputValues = list;
		this.workflow = workflow;
		this.tempDir = tempDir.toString()+"/";
		outputDir = this.tempDir+"output/";
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
		String[] files = new String[WrapperSettings.TAVERNA_WORKFLOW_OUTPUTPORTS.length];
		for(int i=0;i<WrapperSettings.TAVERNA_WORKFLOW_OUTPUTPORTS.length;i++) {
			files[i] = outputDir+WrapperSettings.TAVERNA_WORKFLOW_OUTPUTPORTS[i];
			if(!new File(files[i]).exists()) {
				//the files does not exist so try .error
				files[i] += ".error";
			}
		}
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
		Tools.deleteDirectory(new File(tempDir));			
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
		commandLine.add(WrapperSettings.TAVERNA_SHELL);
		commandLine.add(WrapperSettings.TAVERNA_COMMAND);
		commandLine.add(WrapperSettings.TAVERNA_OPTIONS);
		
		//now set inputs to workflow ports
		for(String key : inputValues.keySet()) {
			commandLine.add("-inputvalue");
			commandLine.add(key);
			String value = inputValues.get(key);
			//if this is the input port - add the full path to the file
			if(key.equals(WrapperSettings.TAVERNA_WORKFLOW_INPUTFILEPORT)) {
				//note we need to add tempdir to the inputs here
				value = tempDir+value;
			}
			commandLine.add(value);
		}
		
		//set the output directory so we can capture the outputs
		commandLine.add("-outputdir");
		//outputDir should not exists at this point, otherwise Taverna doesn't like it
		commandLine.add(outputDir);
		
		commandLine.add(workflow);
		
		ProcessBuilder pb = new ProcessBuilder(commandLine);
		//set the working directory to our temporary directory
		pb.directory(new File(tempDir));
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
		char[] readBuffer = new char[WrapperSettings.BUFSIZE];
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
		return (tempDir + new File(inputValues.get(WrapperSettings.TAVERNA_WORKFLOW_OUTPUTFILEPORT)).getName()+".log");
	}
	
	/**
	 * This is a test main() - the class will usually be used in TavernaHadoopWrapper
	 * @param args command line arguments
	 * @throws IOException file access error
	 */
	public static void main(String[] args) throws IOException {
		
		String inputFile = "/tmp/test.tif";
		String niceName = "abcdef1234.tif";
		
		//load input ports/values to a list
		HashMap<String, String> list = new HashMap<String, String>();
		list.put(WrapperSettings.TAVERNA_WORKFLOW_INPUTFILEPORT, new File(inputFile).getName());
		list.put(WrapperSettings.TAVERNA_WORKFLOW_OUTPUTFILEPORT, niceName);
		
		File tempDir = Tools.newTempDir();
		FileInputStream fis = new FileInputStream(inputFile);
		FileOutputStream fos = new FileOutputStream(tempDir.toString()+"/"+new File(inputFile).getName());
		byte[] buffer = new byte[WrapperSettings.BUFSIZE];
		int bytesRead = 0;
		while(fis.available()>0) {
			bytesRead = fis.read(buffer);
			fos.write(buffer, 0, bytesRead);
		}
		fis.close();
		fos.close();

		
		//create new object and execute
		HadoopJob tsb = new TavernaCommandLineJob(list, WrapperSettings.TAVERNA_WORKFLOW, tempDir);
		
		//NOTE: this is the output filename set in the workflow
		//((TavernaCommandLineJob)tsb).setOutputName("ZIPFile");
		
		tsb.run();
		
		for(String f:tsb.getOutputFiles())
			System.out.println("Output file: "+f);
		System.out.println("Log file: "+tsb.getLogFilename());		
		System.out.println("Success: "+tsb.wasSuccessful());
		
		tsb.cleanup();
		
	}
	
}
