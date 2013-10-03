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

import eu.scape_project.tb.chutney.Settings;
import eu.scape_project.tb.chutney.Tools;
import eu.scape_project.tb.chutney.Settings.JobType;

/**
 * This class implements a workflow by invoking command line tools
 * that need to be available on all Hadoop nodes.
 * @author wpalmer
 *
 */
public class XMLCommandLineJob implements ChutneyJob {

	/**
	 * A class that represents an XML code file
	 * @author wpalmer
	 *
	 */
	private class XMLTool {
		/**
		 * List of input files, as defined in the xml
		 */
		private String[] gInFiles;
		/**
		 * List of output files, as defined in the xml
		 */
		private String[] gOutFiles;
		/**
		 * Command line, as defined in the xml
		 */
		private String gCommandLine = "";
		/**
		 * Library path, as defined in the xml
		 */
		private String gLibraryPath = "";
		/**
		 * Whether to redirect stdout to an output file, as defined in the xml
		 */
		private boolean gRedirectSTDOUT = false;
		/**
		 * Instantiate the class
		 * @param pXmlCode a full path to a local file containing XML code
		 */
		public XMLTool(String pXmlCode) {
			loadXML(pXmlCode);
		}
		/**
		 * Whether to redirect stdout to the output file
		 * @return true or false
		 */
		public boolean redirectSTDOUT() {
			return gRedirectSTDOUT;
		}
		/**
		 * Replaces the input file name already set with @param filename
		 * @param pFilename filename to use as a replacement
		 */
		//NOTE: this will only work the first time it is called!
		public void setInputFile(String pFilename) {
			//make sure these are local file references only
			String filename = new File(pFilename).getName();
			//System.out.println("setting xml input file: "+filename);
			//replace all instances of %input% in inFiles with filename
			for(int i=0;i<gOutFiles.length;i++) {
				gOutFiles[i] = gOutFiles[i].replaceAll(Settings.XML_INPUT_REPLACEMENT, filename);
			}
			//replace all instances of %input% in outFiles with filename
			for(int i=0;i<gInFiles.length;i++) {
				gInFiles[i] = gInFiles[i].replaceAll(Settings.XML_INPUT_REPLACEMENT, filename);
			}
			//replace all instances of %input[i]% in commandline
			for(int i=0;i<gInFiles.length;i++) {
				gCommandLine = gCommandLine.replaceAll("%input"+(i+1)+"%", gInFiles[i]);
			}
			//replace all instances of %output[i]% in commandline
			for(int i=0;i<gOutFiles.length;i++) {
				gCommandLine = gCommandLine.replaceAll("%output"+(i+1)+"%", gOutFiles[i]);
			}
			System.out.println(gCommandLine);
		}
		/**
		 * Get the library path
		 * @return get the library path defined in the xml
		 */
		public String getLibraryPath() {
			return gLibraryPath;
		}
		/**
		 * Get the list of output files
		 * @return list of output files
		 */
		public String[] getOutputFiles() {
			return gOutFiles;
		}
		/**
		 * Get the list of input files
		 * @return list of input files
		 */
		public String[] getInputFiles() {
			return gInFiles;
		}
		/**
		 * Get the command line
		 * @return get the command line from the xml
		 */
		public String getCommandLine() {
			return gCommandLine;
		}

		/**
		 * Load an XML file in to the class
		 * @param pXmlCode
		 */
		private void loadXML(String pXmlCode) {
			//parse the values returned
			DocumentBuilder docB = null;
			Document doc = null;
			try {
				docB = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			} catch(ParserConfigurationException pce) {
			}
			try {
				doc = docB.parse(pXmlCode);
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
				gInFiles = new String[count];
				for(int i=0;i<count;i++) {//xpath is 1-based
					num = new Integer(xpath.evaluate("/tool/input["+(i+1)+"]/@val", root));
					gInFiles[num-1] = xpath.evaluate("/tool/input["+(i+1)+"]", root);	
				}
				count = new Integer(xpath.evaluate("count(/tool/output)", root));
				gOutFiles = new String[count];
				for(int i=0;i<count;i++) {//xpath is 1-based
					num = new Integer(xpath.evaluate("/tool/output["+(i+1)+"]/@val", root));
					gOutFiles[num-1] = xpath.evaluate("/tool/output["+(i+1)+"]", root);
				}
				gLibraryPath = xpath.evaluate("/tool/librarypath", root);
				gCommandLine = xpath.evaluate("/tool/command", root);
				gRedirectSTDOUT = new Boolean(xpath.evaluate("/tool/redirectstdouttooutput", root));
			} catch (XPathExpressionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * XMLTool for this class
	 */
	private XMLTool gXml;
	
	/**
	 * Global stdout buffer
	 */
	private BufferedReader gStdout = null;
	/**
	 * Global stderr buffer
	 */
	private BufferedReader gStderr = null;
	
	/**
	 * Variable returned by wasSuccessful()
	 * Set in generateShortReport()
	 */
	private boolean gSuccess = false;
	
	//populate the input/output files into sensible variable names
	private String gTempDir = "";
	private String gLogFile = "";
	private String gXmlName = "";
	
	/**
	 * Construct a CommandLineJob
	 * @param pKeyFile key file for the tracker 
	 * @param pLocalTempDir local temporary directory
	 * @param pXmlCode xml defined job
	 */
	public XMLCommandLineJob(String pKeyFile, String pLocalTempDir, String pXmlCode) {
		gTempDir = pLocalTempDir + "/";
		gXml = new XMLTool(pXmlCode);
		gXmlName = new File(pXmlCode).getName().replace(".xml","");
		gLogFile = gTempDir + pKeyFile + "."+gXmlName+".log";		
		//Replace all instances of Settings.XML_INPUT_REPLACEMENT with the input file
		//in the xml code
		gXml.setInputFile(pKeyFile);
	}
	
	/**
	 * Get the name of the logfile
	 */
	public String getLogFilename() {
		return gLogFile;
	}
	
	/**
	 * Get the name of the xmlcode
	 * @return get the name of the job from the xml filename
	 */
	public String getXMLName() {
		return gXmlName;
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
		String[] files = new String[gXml.getOutputFiles().length];
		for(int i=0;i<gXml.getOutputFiles().length;i++) {
			files[i]=gTempDir+gXml.getOutputFiles()[i];
		}
		return files; 
	}

	/**
	 * Get a list of input files
	 * @return list of full path names of input files
	 */
	public String[] getInputFiles() {
		if(gXml==null) return null;
		String[] xmlFiles = gXml.getInputFiles();
		String files[] = new String[xmlFiles.length];
		for(int i=0;i<files.length;i++) {
			files[i] = gTempDir+xmlFiles[i];
		}
		return files; 
	}

	/**
	 * Executes a given command line.  Note stdout and stderr will be populated by this method.
	 * @param pCommandLine command line to run
	 * @return exit code from execution of the command line
	 * @throws IOException
	 */
	private int runCommand(List<String> pCommandLine, String pLibraryPath) throws IOException {
		//check there are no command line options that are empty
		while(pCommandLine.contains("")) {
			pCommandLine.remove("");
		}
		
		ProcessBuilder pb = new ProcessBuilder(pCommandLine);
		//don't redirect stderr to stdout as our output XML is in stdout
		pb.redirectErrorStream(false);
		//set the working directory to our temporary directory
		pb.directory(new File(gTempDir));
		
		//this is somewhat inelegant
		//HACK: add the library paths to the environment
		//This could be done by wrapping the command line
		//But this will allow us to use shared objects on the cluster
		//As matchbox doesn't seem to want to compile static binaries
		pb.environment().put(pLibraryPath.split("=")[0], pLibraryPath.split("=")[1]);
		
		//start the executable
		Process proc = pb.start();
		//create a log of the console output
		gStdout = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		gStderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
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
		if(new File(gLogFile).exists()) {
			System.out.println("Deleting stale log file");
			new File(gLogFile).delete();
		}

		//Replace all instances of Settings.XML_INPUT_REPLACEMENT with the input file
		//in the xml code

		//calc checksum.  TODO: check against input file to check it is ok
		for(String s:(gXml.getInputFiles())) {
			Tools.writeChecksumToLog(s, Tools.generateChecksum(gTempDir+s), gLogFile);
		}

		List<String> commandLine = new ArrayList<String>();
		for(String s: gXml.getCommandLine().split(" ")) {
			commandLine.add(s);
		}
		int exitCode = runCommand(commandLine, gXml.getLibraryPath());
		if(exitCode==0) gSuccess = true;

		BufferedWriter outputFile;
		
		if(gXml.redirectSTDOUT()) {
			//store the file of the stdout console output
			//HACK: we assume only one output file when doing this
			outputFile = new BufferedWriter(new FileWriter(gTempDir+gXml.getOutputFiles()[0]));
			Tools.writeBufferToFile(gStdout, outputFile);
			outputFile.close();
		}
		
		//store the log file of the console output
		outputFile = new BufferedWriter(new FileWriter(gLogFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		if(!gXml.redirectSTDOUT()) Tools.appendBufferToFile("stdout", gStdout, outputFile);
//		if(!xml.redirectSTDOUT()) Tools.appendBufferToFile("stdout", stdout, outputFile);
		Tools.appendBufferToFile("stderr", gStderr, outputFile);
		outputFile.close();

	}

	/**
	 * Was the job successful?
	 */
	public boolean wasSuccessful() {
		return gSuccess;
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

	public static JobType getJobType() {
		return JobType.XMLCommandLineJob;
	}

	public static String getShortJobType() {
		return "XML";
	}

}
