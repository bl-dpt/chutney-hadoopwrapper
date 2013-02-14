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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile.JP2Profile;

/**
 * This class implements a workflow by invoking command line tools
 * that need to be available on all Hadoop nodes.
 * @author wpalmer
 *
 */
public class CommandLineJob implements HadoopJob {

	/**
	 * Global stdout buffer
	 */
	private BufferedReader stdout = null;
	
	/**
	 * Global stderr buffer
	 */
	private BufferedReader stderr = null;
	
	/**
	 * Set this to true to enable matchbox tests, false skips them
	 */
	private boolean matchboxEnabled = false;
	
	/**
	 * Set this to true to enable matchbox tests, false skips them
	 */
	private boolean imagemagickEnabled = true;
	
	
	/**
	 * Variable returned by wasSuccessful()
	 * Set in generateShortReport()
	 */
	private boolean success = false;
	
	/**
	 * Which encoder to use: true = kakadu, false = openjpeg
	 */
	private final boolean useKakadu = false;
	
	//populate the input/output files into sensible variable names
	private String[] inFiles = null;
	private String tempDir = "";
	private String outFile = "";
	private String logFile = "";
	
	private LinkedList<String> generatedFiles = new LinkedList<String>();
	
	/**
	 * Construct a CommandLineJob
	 * @param localInputFiles list of local input files for the job
	 * @param localTempDir local temporary directory
	 */
	public CommandLineJob(String[] localInputFiles, String localTempDir) {
		inFiles = localInputFiles;
		outFile = localInputFiles[0]+".jp2";
		tempDir = localTempDir+"/";
		logFile = tempDir + inFiles[0]+".log";		
	}
	
	/**
	 * Get the full path to the log file
	 */
	public String getLogFilename() {
		return logFile;
	}
	
	/**
	 * Get a list of the full path to all the output files from this job
	 * @return full path to the output file(s)
	 */
	public String[] getOutputFiles() {
		return new String[] { tempDir+outFile+".zip" };
	}
	
	/**
	 * Get a list of the full path to all the input files for this job
	 * @return null (at all time)
	 */
	public String[] getInputFiles() {
		return null;
	}
	
	/**
	 * Set up the job
	 */
	public void setup() {
		//load a jpeg2000 profile?
		
	}

	/**
	 * Executes a given command line.  Note stdout and stderr will be populated by this method.
	 * @param commandLine command line to run
	 * @return exit code from execution of the command line
	 * @throws IOException
	 */
	private int runCommand(List<String> commandLine) throws IOException {
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
		pb.environment().put(WrapperSettings.LIBRARY_PATH.split("=")[0], 
				WrapperSettings.LIBRARY_PATH.split("=")[1]);
		
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
	 * Runs a tool to migrate a file to jpeg 2000
	 * @param inFile input file
	 * @param outFile output file
	 * @param logFile log file
	 * @throws IOException
	 */
	private void migrateFile(String inFile, String outFile, String logFile) throws IOException {
		System.out.println("migrateFile("+inFile+", "+outFile+", ...)");
		
		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		
		if(useKakadu) {
			commandLine.add(WrapperSettings.KAKADU_COMMAND);
			//things go wrong if this is empty and it's added to the command line!
			for(String opt : WrapperSettings.KAKADU_OPTIONS.split(" ")) {
				commandLine.add(opt);
			}
		
			//now set inputs to workflow ports
			commandLine.add(WrapperSettings.KAKADU_INPUT_FILE_OPT);
			commandLine.add(inFile);
			commandLine.add(WrapperSettings.KAKADU_OUTPUT_FILE_OPT);
			commandLine.add(outFile);
			
			//add jpeg commandline here
			commandLine.addAll(Jpeg2kProfile.getKakaduCommand(new JP2Profile()));
			
		} else {
			commandLine.add(WrapperSettings.OPENJPEG_COMMAND);
			//things go wrong if this is empty and it's added to the command line!
			for(String opt : WrapperSettings.OPENJPEG_OPTIONS.split(" ")) {
				commandLine.add(opt);
			}
		
			//now set inputs to workflow ports
			commandLine.add(WrapperSettings.OPENJPEG_INPUT_FILE_OPT);
			commandLine.add(inFile);
			commandLine.add(WrapperSettings.OPENJPEG_OUTPUT_FILE_OPT);
			commandLine.add(outFile);
			
			//add jpeg commandline here
			commandLine.addAll(Jpeg2kProfile.getOpenJpegCommand(new JP2Profile()));
		}
		
		int exitCode = runCommand(commandLine);

		//store the log file of the console output
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(logFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stdout", stdout, outputFile);
		Tools.appendBufferToFile("stderr", stderr, outputFile);
		outputFile.close();

	}

	/**
	 * Run jpylyzer
	 * @param inFile input file
	 * @param outFile output file
	 * @param logFile log file
	 * @throws IOException
	 */
	private void getValidationInfo(String inFile, String toutFile, String logFile) throws IOException {
		System.out.println("getValidationInfo("+inFile+", "+toutFile+", ...)");

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(WrapperSettings.JPYLYZER);
		//things go wrong if this is empty and it's added to the command line!
		for(String opt : WrapperSettings.JPYLYZER_OPTIONS.split(" ")) {
			commandLine.add(opt);
		}
		//inputfile for command
		commandLine.add(inFile);
		
		int exitValue = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(toutFile));
		Tools.writeBufferToFile(stdout, outputFile);
		outputFile.close();

		//append commandline and stderr console output to log file
		outputFile = new BufferedWriter(new FileWriter(logFile,true));
		Tools.appendProcessInfoToLog(exitValue, commandLine, outputFile);
		//write the log of stderr to the logfile
		Tools.appendBufferToFile("stderr", stderr, outputFile);
		outputFile.close();
		
	}
	
	/**
	 * Extract the metadata from the input file
	 * @param inFile input file
	 * @param outFile output file
	 * @param logFile log file
	 * @throws IOException
	 */
	private void extractMetadata(String inFile, String toutFile, String logFile) throws IOException {
		System.out.println("extractMetadata("+inFile+", "+toutFile+", ...)");

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(WrapperSettings.EXIFTOOL);
		//things go wrong if this is empty and it's added to the command line!
		for(String opt : WrapperSettings.EXIFTOOL_OPTIONS.split(" ")) {
			commandLine.add(opt);
		}
		//inputfile for command
		commandLine.add(inFile);
		
		int exitValue = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(toutFile));
		Tools.writeBufferToFile(stdout, outputFile);
		outputFile.close();

		//append commandline and stderr console output to log file
		outputFile = new BufferedWriter(new FileWriter(logFile,true));
		Tools.appendProcessInfoToLog(exitValue, commandLine, outputFile);
		//write the log of stderr to the logfile
		Tools.appendBufferToFile("stderr", stderr, outputFile);
		outputFile.close();		
		
	}

	/**
	 * Use Matchbox extractfeatures on given file.  There will be several new files after
	 * this operation - see WrapperSettings.MATCHBOX_EXT_...
	 * @param inFile input file
	 * @param logFile log file
	 * @throws IOException
	 */
	private void matchboxExtractFeatures(String inFile, String logFile) throws IOException {
		System.out.println("matchboxExtractFeatures("+inFile+" ...)");

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(WrapperSettings.MATCHBOX_EXTRACT_FEATURES);
		//things go wrong if this is empty and it's added to the command line!
		
		//now set inputs to workflow ports
		commandLine.add(inFile);
		
		int exitCode = runCommand(commandLine);

		//store the log file of the console output
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(logFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stdout", stdout, outputFile);
		Tools.appendBufferToFile("stderr", stderr, outputFile);
		outputFile.close();
		
	}

	/**
	 * Compare the Matchbox SIFT outputs for two files
	 * @param inFile input file
	 * @param outFile output file 
	 * @param logFile log file
	 * @throws IOException
	 */
	private void matchboxCompareSIFT(String inFile, String outFile, String logFile) throws IOException {
		System.out.println("matchboxCompareSIFT("+inFile+", "+outFile+", ...)");

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(WrapperSettings.MATCHBOX_COMPARE);
		//things go wrong if this is empty and it's added to the command line!
		
		//now set inputs to workflow ports
		commandLine.add(inFile+WrapperSettings.MATCHBOX_EXT_SIFTCOMPARISON);
		commandLine.add(outFile+WrapperSettings.MATCHBOX_EXT_SIFTCOMPARISON);
		
		int exitCode = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(outFile+WrapperSettings.MATCHBOX_COMP_SIFT_EXT));
		Tools.writeBufferToFile(stdout, outputFile);
		outputFile.close();
		
		//store the log file of the console output
		outputFile = new BufferedWriter(new FileWriter(logFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stderr", stderr, outputFile);
		outputFile.close();
		
		
	}

	/**
	 * Get the PSNR after comparing two image files
	 * @param inFile first file to compare
	 * @param outFile second file to compare
	 * @param logFile log file to store results
	 * @throws IOException
	 */
	private void imagemagickComparePSNR(String inFile, String outFile, String logFile) throws IOException {

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		for(String s:WrapperSettings.IMAGEMAGICK_PSNR.split(" ")) {
			commandLine.add(s);
		}
		
		//now set inputs 
		commandLine.add(inFile);
		commandLine.add(outFile);
		commandLine.add(WrapperSettings.NULL_DEVICE);
		
		int exitCode = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(outFile+WrapperSettings.IMAGEMAGICK_PSNR_EXT));
		Tools.writeBufferToFile(stderr, outputFile);
		outputFile.close();
		
		//store the log file of the console output
		outputFile = new BufferedWriter(new FileWriter(logFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stdout", stdout, outputFile);
		outputFile.close();
				
	}
	
	/**
	 * Compare the Matchbox profile outputs for two files
	 * @param inFile input file
	 * @param outFile output file 
	 * @param logFile log file
	 * @throws IOException
	 */
	private void matchboxCompareProfile(String inFile, String outFile, String logFile) throws IOException {
		System.out.println("matchboxCompareProfile("+inFile+", "+outFile+", ...)");
		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(WrapperSettings.MATCHBOX_COMPARE);
		//things go wrong if this is empty and it's added to the command line!
		
		//now set inputs to workflow ports
		commandLine.add(inFile+WrapperSettings.MATCHBOX_EXT_PROFILE);
		commandLine.add(outFile+WrapperSettings.MATCHBOX_EXT_PROFILE);
		
		int exitCode = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(outFile+WrapperSettings.MATCHBOX_COMP_PROFILE_EXT));
		Tools.writeBufferToFile(stdout, outputFile);
		outputFile.close();
		
		//store the log file of the console output
		outputFile = new BufferedWriter(new FileWriter(logFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stderr", stderr, outputFile);
		outputFile.close();
		
		
	}


	/**
	 * Checks the encode settings with the profile in the jpylyzer file
	 * @param jpylyzerFile
	 * @param logFile
	 * @throws IOException
	 */
	private void checkEncodeSettings(String jpylyzerFile, String logFile) throws IOException {
		System.out.println("checkEncodeSettings("+jpylyzerFile+", ...)");

		boolean encodeOK = false;
		try {
			encodeOK = Jpeg2kProfile.equalsJpylyzerProfile(jpylyzerFile, new JP2Profile());
		} catch(Exception e) {
			e.printStackTrace();
		}

		//log the results
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(logFile,true));

		outputFile.write("------------------------------");outputFile.newLine();
		outputFile.write("Checked Jpylyzer XML versus encode settings");outputFile.newLine();
		outputFile.write("Output JP2 matched encode settings: "+encodeOK);outputFile.newLine();
		outputFile.write("------------------------------");outputFile.newLine();		
		outputFile.close();	
		
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

		//NOTE: all methods *must* be passed absolute file references for new files
		
		//calc checksum.  TODO: check against input file to check it is ok
		Tools.writeChecksumToLog(inFiles[0], Tools.generateChecksum(tempDir+inFiles[0]), logFile);
		
		//store the metadata from the original file
		extractMetadata(inFiles[0], tempDir+inFiles[0]+WrapperSettings.EXIFTOOL_EXT, logFile);
		generatedFiles.add(inFiles[0]+WrapperSettings.EXIFTOOL_EXT);
		generatedFiles.add(logFile);
			
		//migrate file and write info to logfile
		migrateFile(inFiles[0], tempDir+outFile, logFile);
		generatedFiles.add(outFile);
		
		//store metadata for the new file
		extractMetadata(outFile, tempDir+outFile+WrapperSettings.EXIFTOOL_EXT, logFile);
		generatedFiles.add(outFile+WrapperSettings.EXIFTOOL_EXT);
		
		//gets the validation info for the new file (jpylyzer)
		getValidationInfo(outFile, tempDir+outFile+WrapperSettings.JPYLYZER_EXT, logFile);
		generatedFiles.add(outFile+WrapperSettings.JPYLYZER_EXT);
		
		//maybe extract the ICC profiles?
		
		//compare jpylyzer output with input profile here
		//should we do this instead?
		// http://openplanetsfoundation.org/blogs/2012-09-04-automated-assessment-jp2-against-technical-profile
		//no - that has a hardcoded profile 
		checkEncodeSettings(tempDir+outFile+WrapperSettings.JPYLYZER_EXT, logFile);
		
		if(matchboxEnabled) {
			//use matchbox to extract the characteristics of the original image
			matchboxExtractFeatures(tempDir+inFiles[0], logFile);
			generatedFiles.add(inFiles[0]+WrapperSettings.MATCHBOX_EXT_HISTOGRAM);
			generatedFiles.add(inFiles[0]+WrapperSettings.MATCHBOX_EXT_PROFILE);
			generatedFiles.add(inFiles[0]+WrapperSettings.MATCHBOX_EXT_METADATA);
			generatedFiles.add(inFiles[0]+WrapperSettings.MATCHBOX_EXT_SIFTCOMPARISON);

			//use matchbox to extract the characteristics of the new image
			matchboxExtractFeatures(tempDir+outFile, logFile);
			generatedFiles.add(outFile+WrapperSettings.MATCHBOX_EXT_HISTOGRAM);
			generatedFiles.add(outFile+WrapperSettings.MATCHBOX_EXT_PROFILE);
			generatedFiles.add(outFile+WrapperSettings.MATCHBOX_EXT_METADATA);
			generatedFiles.add(outFile+WrapperSettings.MATCHBOX_EXT_SIFTCOMPARISON);

			//use matchbox to compare the sift characteristics of the images
			matchboxCompareSIFT(tempDir+inFiles[0], tempDir+outFile, logFile);
			generatedFiles.add(outFile+WrapperSettings.MATCHBOX_COMP_SIFT_EXT);

			//use matchbox to compare the profile characteristics of the images
			matchboxCompareProfile(tempDir+inFiles[0], tempDir+outFile, logFile);
			generatedFiles.add(outFile+WrapperSettings.MATCHBOX_COMP_PROFILE_EXT);

			//matchbox histogram comparison doesn't work??
		} 
		if(imagemagickEnabled) {

				imagemagickComparePSNR(tempDir+inFiles[0], tempDir+outFile, logFile);
				generatedFiles.add(outFile+WrapperSettings.IMAGEMAGICK_PSNR_EXT);
				
		}
		
		//generate a short log
		String reportFile = outFile+".report.xml";
		generateShortReport(tempDir+reportFile);
		generatedFiles.add(reportFile);

		//TODO: add error resilience if files do not exist 

		//finally, generate a checksum for the all files incl the log file
		//a place to store the checksums
		HashMap<String, String> checksums = new HashMap<String, String>();
		System.out.println("Generating checksums...");
		for(String file : generatedFiles) {
			System.out.println(file);
			File temp = new File(tempDir+(new File(file).getName()));
			if(temp.exists())
				checksums.put(file, Tools.generateChecksum(temp.getAbsolutePath()));
		}
		
		System.out.println("Generating zip file (with bagit style info)");
		//zip all the generated files together
		
		Tools.zipGeneratedFiles(success, checksums, generatedFiles, tempDir+outFile+".zip", tempDir);
		
	}

	/**
	 * Generate a short report from the workflow data
	 * @param reportFile filename to write report to
	 * @return whether report reports overall success or failure
	 * @throws IOException
	 */
	private boolean generateShortReport(String reportFile) throws IOException {
		boolean generatedIsValid = Jpeg2kProfile.jpylyzerSaysValid(tempDir+outFile+WrapperSettings.JPYLYZER_EXT);
		boolean generatedMatchesInputProfile = Jpeg2kProfile.equalsJpylyzerProfile(tempDir+outFile+WrapperSettings.JPYLYZER_EXT, new JP2Profile());
		boolean ssimMatch = true;
		boolean imagemagickMatch = true;
		
		BufferedWriter out = new BufferedWriter(new FileWriter(reportFile));
		out.write("<?xml version='1.0' encoding='ascii'?>");out.newLine();
		out.write("<migrationReport>");out.newLine();
		out.write("     <jpylyzerSaysOutputValid>"+generatedIsValid+"</jpylyzerSaysOutputValid>");out.newLine();
		out.write("     <outputMatchesInputProfile>"+generatedMatchesInputProfile+"</outputMatchesInputProfile>");out.newLine();
		if(matchboxEnabled) {
			//note the following comparison (>0.9) is one used in Matchbox's MatchboxLib.py 
			ssimMatch = Tools.getSSIMCompareVal(tempDir+outFile+WrapperSettings.MATCHBOX_COMP_SIFT_EXT)>WrapperSettings.MATCHBOX_THRESHOLD;
			out.write("     <ssimImageMatches>"+ssimMatch+"</ssimImageMatches>");out.newLine(); 
		}
		if(imagemagickEnabled) {
			imagemagickMatch = Tools.getPSNRVal(tempDir+outFile+WrapperSettings.IMAGEMAGICK_PSNR_EXT)>WrapperSettings.PSNR_THRESHOLD;
			out.write("     <psnrMatches>"+imagemagickMatch+"</psnrMatches>");out.newLine(); 			
		}
		out.write("</migrationReport>");out.newLine();
		out.close();
		
		success = (((generatedIsValid&generatedMatchesInputProfile&ssimMatch&imagemagickMatch)==true));
		
		return success;
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
		Tools.deleteDirectory(new File(tempDir));			
	}
	
	/**
	 * A test main method for this class to be run standalone
	 * @param args command line arguments
	 * @throws IOException file access error
	 */
	public static void main(String[] args) throws IOException  {

		String[] inputFiles = new String[1];
		inputFiles[0] = WrapperSettings.STANDALONE_TEST_INPUT;

		//create new object and execute
		HadoopJob tsb = new CommandLineJob(inputFiles, WrapperSettings.STANDALONE_TEST_OUTPUT);
		tsb.setup();
		
		tsb.run();
		
		//NEVER DO THIS (if it deletes the files)
		//tsb.cleanup();
		
	}
	
}
