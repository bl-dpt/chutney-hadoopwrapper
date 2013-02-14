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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * This class implements a workflow by invoking command line tools
 * that need to be available on all Hadoop nodes.
 * @author wpalmer
 *
 */
public class XMLCommandLineJob implements HadoopJob {

	/**
	 * A class that represents an XML code file
	 * @author wpalmer
	 *
	 */
	private class XMLTool {
		
		/**
		 * List of input files, as defined in the xml
		 */
		private String[] inFiles;
		/**
		 * List of output files, as defined in the xml
		 */
		private String[] outFiles;
		/**
		 * Command line, as defined in the xml
		 */
		private String commandLine = "";
		/**
		 * Library path, as defined in the xml
		 */
		private String libraryPath = "";
		/**
		 * Whether to redirect stdout to an output file, as defined in the xml
		 */
		private boolean redirectSTDOUT = false;
		
		/**
		 * Instantiate the class
		 * @param xmlCode a full path to a local file containing XML code
		 */
		public XMLTool(String xmlCode) {
			loadXML(xmlCode);
		}
		
		/**
		 * Whether to redirect stdout to the output file
		 * @return true or false
		 */
		public boolean redirectSTDOUT() {
			return redirectSTDOUT;
		}
		
		/**
		 * Replaces the input file name already set with @param filename
		 * @param tFilename filename to use as a replacement 
		 */
		//NOTE: this will only work the first time it is called!
		public void setInputFile(String tFilename) {
			//make sure these are local file references only
			String filename = new File(tFilename).getName();
			//System.out.println("setting xml input file: "+filename);
			//replace all instances of %input% in inFiles with filename
			for(int i=0;i<outFiles.length;i++) {
				outFiles[i] = outFiles[i].replaceAll(WrapperSettings.XML_INPUT_REPLACEMENT, filename);
			}
			//replace all instances of %input% in outFiles with filename
			for(int i=0;i<inFiles.length;i++) {
				inFiles[i] = inFiles[i].replaceAll(WrapperSettings.XML_INPUT_REPLACEMENT, filename);
			}
			//replace all instances of %input[i]% in commandline 
			for(int i=0;i<inFiles.length;i++) {
				commandLine = commandLine.replaceAll("%input"+(i+1)+"%", inFiles[i]);
			}
			//replace all instances of %output[i]% in commandline 
			for(int i=0;i<outFiles.length;i++) {
				commandLine = commandLine.replaceAll("%output"+(i+1)+"%", outFiles[i]);
			}
			System.out.println(commandLine);
		}
		
		/**
		 * Get the library path 
		 * @return get the library path defined in the xml
		 */
		public String getLibraryPath() {
			return libraryPath;
		}
		
		/**
		 * Get the list of output files
		 * @return list of output files
		 */
		public String[] getOutputFiles() {
			return outFiles;
		}
		
		/**
		 * Get the list of input files
		 * @return list of input files
		 */
		public String[] getInputFiles() {
			return inFiles;
		}
		
		/**
		 * Get the command line
		 * @return get the command line from the xml
		 */
		public String getCommandLine() {
			return commandLine;
		}

		/**
		 * Load an XML file in to the class
		 * @param xmlCode
		 */
		private void loadXML(String xmlCode) {
			//parse the values returned
			DocumentBuilder docB = null;
			Document doc = null;
			
			try {
				docB = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			} catch(ParserConfigurationException pce) {
			}
			try {
				doc = docB.parse(xmlCode);
			} catch(IOException ioe) {
				ioe.printStackTrace();
			} catch(SAXException se) {
				se.printStackTrace();
			}
			
			Node root = doc.getFirstChild();
			XPath xpath = XPathFactory.newInstance().newXPath();			
			try {
				int num = 0;
				int count = new Integer(xpath.evaluate("count(/tool/input)", root));
				inFiles = new String[count];
				for(int i=0;i<count;i++) {//xpath is 1-based
					num = new Integer(xpath.evaluate("/tool/input["+(i+1)+"]/@val", root));
					inFiles[num-1] = xpath.evaluate("/tool/input["+(i+1)+"]", root);					
				}
				count = new Integer(xpath.evaluate("count(/tool/output)", root));
				outFiles = new String[count];
				for(int i=0;i<count;i++) {//xpath is 1-based
					num = new Integer(xpath.evaluate("/tool/output["+(i+1)+"]/@val", root));
					outFiles[num-1] = xpath.evaluate("/tool/output["+(i+1)+"]", root);
				}
				libraryPath = xpath.evaluate("/tool/librarypath", root);
				commandLine = xpath.evaluate("/tool/command", root);
				redirectSTDOUT = new Boolean(xpath.evaluate("/tool/redirectstdouttooutput", root));
			} catch (XPathExpressionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}
		
	}

	/**
	 * XMLTool for this class
	 */
	private XMLTool xml;
	
	/**
	 * Global stdout buffer
	 */
	private BufferedReader stdout = null;
	/**
	 * Global stderr buffer
	 */
	private BufferedReader stderr = null;
	
	/**
	 * Variable returned by wasSuccessful()
	 * Set in generateShortReport()
	 */
	private boolean success = false;
	
	//populate the input/output files into sensible variable names
	private String tempDir = "";
	private String logFile = "";
	private String xmlName = "";
	
	/**
	 * Construct a CommandLineJob
	 * @param keyFile key file for the tracker 
	 * @param localTempDir local temporary directory
	 * @param xmlCode xml defined job
	 */
	public XMLCommandLineJob(String keyFile, String localTempDir, String xmlCode) {
		tempDir = localTempDir + "/";
		xml = new XMLTool(xmlCode);
		xmlName = new File(xmlCode).getName().replace(".xml","");
		logFile = tempDir + keyFile + "."+xmlName+".log";		
		//Replace all instances of WrapperSettings.XML_INPUT_REPLACEMENT with the input file
		//in the xml code
		xml.setInputFile(keyFile);
	}
	
	/**
	 * Get the name of the logfile
	 */
	public String getLogFilename() {
		return logFile;
	}
	
	/**
	 * Get the name of the xmlcode
	 * @return get the name of the job from the xml filename
	 */
	public String getXMLName() {
		return xmlName;
	}
	
	/**
	 * Set up the job
	 */
	public void setup() {
		
	}

	/**
	 * Get a list of output files
	 * @return list of full path names of output files
	 */
	public String[] getOutputFiles() {
		String[] files = new String[xml.getOutputFiles().length];
		for(int i=0;i<xml.getOutputFiles().length;i++) {
			files[i]=tempDir+xml.getOutputFiles()[i];
		}
		return files; 
	}

	/**
	 * Get a list of input files
	 * @return list of full path names of input files
	 */
	public String[] getInputFiles() {
		if(xml==null) return null;
		String[] xmlFiles = xml.getInputFiles();
		String files[] = new String[xmlFiles.length];
		for(int i=0;i<files.length;i++) {
			files[i] = tempDir+xmlFiles[i];
		}
		return files; 
	}

	/**
	 * Executes a given command line.  Note stdout and stderr will be populated by this method.
	 * @param commandLine command line to run
	 * @return exit code from execution of the command line
	 * @throws IOException
	 */
	private int runCommand(List<String> commandLine, String libraryPath) throws IOException {
		//check there are no command line options that are empty
		while(commandLine.contains("")) {
			commandLine.remove("");
		}
		
		ProcessBuilder pb = new ProcessBuilder(commandLine);
		//don't redirect stderr to stdout as our output XML is in stdout
		pb.redirectErrorStream(false);
		//set the working directory to our temporary directory
		pb.directory(new File(tempDir));
		
		//this is somewhat inelegant
		//HACK: add the library paths to the environment
		//This could be done by wrapping the command line
		//But this will allow us to use shared objects on the cluster
		//As matchbox doesn't seem to want to compile static binaries
		pb.environment().put(libraryPath.split("=")[0], libraryPath.split("=")[1]);
		
		//start the executable
		Process proc = pb.start();
		//create a log of the console output
		stdout = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
		try {
			//wait for process to end before continuing
			proc.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return proc.exitValue();
	}
	
	/**
	 * Runs the command line job/workflow
	 */
	public void run() throws IOException {

		//delete old log file.  special case when running this class standalone
		if(new File(logFile).exists()) {
			System.out.println("Deleting stale log file");
			new File(logFile).delete();
		}

		//Replace all instances of WrapperSettings.XML_INPUT_REPLACEMENT with the input file
		//in the xml code

		//calc checksum.  TODO: check against input file to check it is ok
		for(String s:(xml.getInputFiles())) {
			Tools.writeChecksumToLog(s, Tools.generateChecksum(tempDir+s), logFile);
		}

		List<String> commandLine = new ArrayList<String>();
		for(String s: xml.getCommandLine().split(" ")) {
			commandLine.add(s);
		}
		int exitCode = runCommand(commandLine, xml.getLibraryPath());
		if(exitCode==0) success = true;

		BufferedWriter outputFile;
		
		if(xml.redirectSTDOUT()) {
			//store the file of the stdout console output
			//HACK: we assume only one output file when doing this
			outputFile = new BufferedWriter(new FileWriter(tempDir+xml.getOutputFiles()[0]));
			Tools.writeBufferToFile(stdout, outputFile);
			outputFile.close();
		}
		
		//store the log file of the console output
		outputFile = new BufferedWriter(new FileWriter(logFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		if(!xml.redirectSTDOUT) Tools.appendBufferToFile("stdout", stdout, outputFile);
		Tools.appendBufferToFile("stderr", stderr, outputFile);
		outputFile.close();

	}

	/**
	 * Was the job successful?
	 */
	public boolean wasSuccessful() {
		return success;
	}	
	
	/**
	 * Clean up after this job has run
	 */
	public void cleanup() {
		//delete all the generated files - all being in the same directory makes this easy
		//DO NOT DELETE THE INPUT FILE WHEN RUNNING FROM THIS CLASS' MAIN!
		
		//we don't do this as the temp dir now keeps a copy of the generated files
		//to reduce copying back and forth
		//data will be deleted when the report is generated in XMLWorkflowReport
		
		//Tools.deleteDirectory(new File(tempDir));	
	}
	
	/**
	 * A test main method for this class to be run standalone
	 * @param args command line arguments
	 * @throws IOException file access error
	 */
	public static void main(String[] args) throws IOException  {
		
	}
	
}
