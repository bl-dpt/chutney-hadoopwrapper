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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import eu.scape_project.tb.chutney.Settings;
import eu.scape_project.tb.chutney.Settings.JobType;
import eu.scape_project.tb.chutney.Tools;
import uk.bl.dpt.qa.JP2Check;
import uk.bl.dpt.qa.JP2CommandLine;
import uk.bl.dpt.qa.JP2Profile;
import uk.bl.dpt.qa.JpylyzerSchematron;

/**
 * This class implements a workflow by invoking command line tools
 * that need to be available on all Hadoop nodes.
 * @author wpalmer
 *
 */
public class CommandLineJob implements ChutneyJob {

	/*
	 * Local job settings
	 * ===================================================================================
	 */

	/**
	 * Set this to true to enable matchbox tests, false skips them
	 */
	private final boolean gMatchboxEnabled = false;
	
	/**
	 * Set this to true to enable imagemagick tests, false skips them
	 */
	private final boolean gImagemagickEnabled = true;
	
	/**
	 * Set this to true to enable dissimilar tests, false skips them
	 */
	private final boolean gDissimilarEnabled = false;

	/**
	 * Which encoder to use: true = kakadu, false = openjpeg
	 */
	private final boolean gUseKakadu = false;

	/**
	 * Whether to check jpylyzer output using Schematron
	 */
	private final boolean gSchematron = true;
	
	/**
	 * Whether or not to add the log file to the output zip file
	 */
	private final boolean gAddLogToZip = true;
	/*
	 * ===================================================================================
	 */

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
	//assume input is valid
	//private boolean gInputValid = true;
	
	//populate the input/output files into sensible variable names
	private String[] gInFiles = null;
	private String gTempDir = "";
	private String gOutFile = "";
	private String gLogFile = "";
	
	private LinkedList<String> gGeneratedFiles = new LinkedList<String>();
	
	/**
	 * Construct a CommandLineJob
	 * @param pLocalInputFiles list of local input files for the job
	 * @param pLocalTempDir local temporary directory
	 */
	public CommandLineJob(String[] pLocalInputFiles, String pLocalTempDir) {
		gInFiles = pLocalInputFiles;
		gOutFile = pLocalInputFiles[0]+".jp2";
		gTempDir = pLocalTempDir+"/";
		gLogFile = gTempDir + gInFiles[0]+".log";		
	}
	
	/**
	 * Get the full path to the log file
	 */
	public String getLogFilename() {
		return gLogFile;
	}
	
	/**
	 * Get a list of the full path to all the output files from this job
	 * @return full path to the output file(s)
	 */
	public String[] getOutputFiles() {
	//	if(gInputValid) {
			return new String[] { gTempDir+gOutFile+".zip" };
		//}
	//	return new String[] {};
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
	 * @param pCommandLine command line to run
	 * @return exit code from execution of the command line
	 * @throws IOException
	 */
	private int runCommand(List<String> pCommandLine) throws IOException {
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
		pb.environment().put(Settings.LIBRARY_PATH.split("=")[0], 
				Settings.LIBRARY_PATH.split("=")[1]);
		
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
	 * NOTE: this is currently unused
	 * 
	 * Check input TIFF is valid - this is a hack but it's the easiest (and best?) way of ensuring 
	 * that the tiff data is readable, by just reading it!  It shouldn't impact performance too much
	 * as the file should already be cached in RAM.
	 * 
	 * @param pInFile input file
	 * @param pLogFile log file
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private boolean checkInputTIFFIsValid(String pInFile, String pLogFile) throws IOException {
		System.out.println("checkInputTIFFIsValid("+pInFile+", ...)");

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(Settings.TIFFTOPNM);
		commandLine.add(pInFile);
		commandLine.add(">");
		commandLine.add(Settings.NULL_DEVICE);
		
		int exitValue = runCommand(commandLine);

		//if tifftopnm exited ok then the tiff should be readable
		if(0==exitValue) {
			return true;
		}
		
		//write an error to the log file
		PrintWriter logFile = new PrintWriter(new FileWriter(pLogFile,true));
		logFile.println("ERROR: file not readable: "+pInFile);
		logFile.close();
		
		return false;
		
	}
	
	/**
	 * This is a hack to get the input data into a format that Kakadu can handle and shouldn't be used
	 * for production
	 * @param pInFile
	 * @param pLogFile
	 * @return
	 * @throws IOException
	 */
	private String tiffToPnm(String pInFile, String pLogFile) throws IOException {
		String outFile = pInFile+".pgm";
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(Settings.TAVERNA_SHELL);
		commandLine.add("-c");
		commandLine.add(Settings.TIFFTOPNM+" "+pInFile+" > "+outFile);
		int exitCode = runCommand(commandLine);
		//this is horrid!! - write the stdout buffer to the file as this is our pnm
//		Tools.writeBufferToFile(gStdout, new BufferedWriter(new FileWriter(outFile)));
		//store the log file of the console output
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pLogFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		outputFile.close();
		return outFile;		
	}
	
	/**
	 * Runs a tool to migrate a file to jpeg 2000
	 * @param pInFile input file
	 * @param pOutFile output file
	 * @param pLogFile log file
	 * @throws IOException
	 */
	private void migrateFile(String pInFile, String pOutFile, String pLogFile) throws IOException {
		System.out.println("migrateFile("+pInFile+", "+pOutFile+", ...)");
		
		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		
		if(gUseKakadu) {

			String pnmFile = tiffToPnm(pInFile, pLogFile);
			
			commandLine.add(Settings.KAKADU_COMMAND);
			//things go wrong if this is empty and it's added to the command line!
			for(String opt : Settings.KAKADU_OPTIONS.split(" ")) {
				commandLine.add(opt);
			}
		
			//now set inputs to workflow ports
			commandLine.add(Settings.KAKADU_INPUT_FILE_OPT);
			commandLine.add(pnmFile);
			commandLine.add(Settings.KAKADU_OUTPUT_FILE_OPT);
			commandLine.add(pOutFile);
			
			//add jpeg commandline here
			commandLine.addAll(JP2CommandLine.getKakaduCommand(new JP2Profile()));
			
		} else {
			commandLine.add(Settings.OPENJPEG_COMMAND);
			//things go wrong if this is empty and it's added to the command line!
			for(String opt : Settings.OPENJPEG_OPTIONS.split(" ")) {
				commandLine.add(opt);
			}
		
			//now set inputs to workflow ports
			commandLine.add(Settings.OPENJPEG_INPUT_FILE_OPT);
			commandLine.add(pInFile);
			commandLine.add(Settings.OPENJPEG_OUTPUT_FILE_OPT);
			commandLine.add(pOutFile);
			
			//add jpeg commandline here
			commandLine.addAll(JP2CommandLine.getOpenJpegCommand(new JP2Profile()));
		}
		
		int exitCode = runCommand(commandLine);

		//store the log file of the console output
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pLogFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stdout", gStdout, outputFile);
		Tools.appendBufferToFile("stderr", gStderr, outputFile);
		outputFile.close();

	}

	/**
	 * Run jpylyzer
	 * @param pInFile input file
	 * @param gOutFile output file
	 * @param pLogFile log file
	 * @throws IOException
	 */
	private void getValidationInfo(String pInFile, String pOutFile, String pLogFile) throws IOException {
		System.out.println("getValidationInfo("+pInFile+", "+pOutFile+", ...)");

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(Settings.JPYLYZER);
		//things go wrong if this is empty and it's added to the command line!
		for(String opt : Settings.JPYLYZER_OPTIONS.split(" ")) {
			commandLine.add(opt);
		}
		//inputfile for command
		commandLine.add(pInFile);
		
		int exitValue = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pOutFile));
		Tools.writeBufferToFile(gStdout, outputFile);
		outputFile.close();

		//append commandline and stderr console output to log file
		outputFile = new BufferedWriter(new FileWriter(pLogFile,true));
		Tools.appendProcessInfoToLog(exitValue, commandLine, outputFile);
		//write the log of stderr to the logfile
		Tools.appendBufferToFile("stderr", gStderr, outputFile);
		outputFile.close();
		
	}
	
	/**
	 * Extract the metadata from the input file
	 * @param pInFile input file
	 * @param gOutFile output file
	 * @param pLogFile log file
	 * @throws IOException
	 */
	private void extractMetadata(String pInFile, String pOutFile, String pLogFile) throws IOException {
		System.out.println("extractMetadata("+pInFile+", "+pOutFile+", ...)");

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(Settings.EXIFTOOL);
		//things go wrong if this is empty and it's added to the command line!
		for(String opt : Settings.EXIFTOOL_OPTIONS.split(" ")) {
			commandLine.add(opt);
		}
		//inputfile for command
		commandLine.add(pInFile);
		
		int exitValue = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pOutFile));
		Tools.writeBufferToFile(gStdout, outputFile);
		outputFile.close();

		//append commandline and stderr console output to log file
		outputFile = new BufferedWriter(new FileWriter(pLogFile,true));
		Tools.appendProcessInfoToLog(exitValue, commandLine, outputFile);
		//write the log of stderr to the logfile
		Tools.appendBufferToFile("stderr", gStderr, outputFile);
		outputFile.close();		
		
	}

	/**
	 * Use Matchbox extractfeatures on given file.  There will be several new files after
	 * this operation - see Settings.MATCHBOX_EXT_...
	 * @param pInFile input file
	 * @param pLogFile log file
	 * @throws IOException
	 */
	private void matchboxExtractFeatures(String pInFile, String pLogFile) throws IOException {
		System.out.println("matchboxExtractFeatures("+pInFile+" ...)");

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(Settings.MATCHBOX_EXTRACT_FEATURES);
		//things go wrong if this is empty and it's added to the command line!
		
		//now set inputs to workflow ports
		commandLine.add(pInFile);
		
		int exitCode = runCommand(commandLine);

		//store the log file of the console output
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pLogFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stdout", gStdout, outputFile);
		Tools.appendBufferToFile("stderr", gStderr, outputFile);
		outputFile.close();
		
	}

	/**
	 * Compare the Matchbox SIFT outputs for two files
	 * @param pInFile input file
	 * @param pOutFile output file 
	 * @param pLogFile log file
	 * @throws IOException
	 */
	private void matchboxCompareSIFT(String pInFile, String pOutFile, String pLogFile) throws IOException {
		System.out.println("matchboxCompareSIFT("+pInFile+", "+pOutFile+", ...)");

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(Settings.MATCHBOX_COMPARE);
		//things go wrong if this is empty and it's added to the command line!
		
		//now set inputs to workflow ports
		commandLine.add(pInFile+Settings.MATCHBOX_EXT_SIFTCOMPARISON);
		commandLine.add(pOutFile+Settings.MATCHBOX_EXT_SIFTCOMPARISON);
		
		int exitCode = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pOutFile+Settings.MATCHBOX_COMP_SIFT_EXT));
		Tools.writeBufferToFile(gStdout, outputFile);
		outputFile.close();
		
		//store the log file of the console output
		outputFile = new BufferedWriter(new FileWriter(pLogFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stderr", gStderr, outputFile);
		outputFile.close();
		
		
	}

	/**
	 * Get the PSNR after comparing two image files
	 * @param pInFile first file to compare
	 * @param pOutFile second file to compare
	 * @param pLogFile log file to store results
	 * @throws IOException
	 */
	private void imagemagickComparePSNR(String pInFile, String pOutFile, String pLogFile) throws IOException {

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		for(String s:Settings.IMAGEMAGICK_PSNR.split(" ")) {
			commandLine.add(s);
		}
		
		//now set inputs 
		commandLine.add(pInFile);
		commandLine.add(pOutFile);
		commandLine.add(Settings.NULL_DEVICE);
		
		int exitCode = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pOutFile+Settings.IMAGEMAGICK_PSNR_EXT));
		Tools.writeBufferToFile(gStderr, outputFile);
		outputFile.close();
		
		//store the log file of the console output
		outputFile = new BufferedWriter(new FileWriter(pLogFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stdout", gStdout, outputFile);
		outputFile.close();
				
	}
	
	/**
	 * Get PSNR&SSIM using Dissimilar after comparing two image files
	 * @param pInFile first file to compare
	 * @param pOutFile second file to compare
	 * @param pLogFile log file to store results
	 * @throws IOException
	 */
	private void runDissimilar(String pInFile, String pOutFile, String pLogFile) throws IOException {

		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add("time");
		commandLine.add("java");
		commandLine.add("-jar");
		commandLine.add(Settings.DISSIMILAR_JAR);
		
		//now set inputs 
		commandLine.add(pInFile);
		commandLine.add(pOutFile);
		
		int exitCode = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pOutFile+Settings.DISSIMILAR_EXT));
		Tools.writeBufferToFile(gStdout, outputFile);
		outputFile.close();
		
		//store the log file of the console output
		outputFile = new BufferedWriter(new FileWriter(pLogFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stderr", gStderr, outputFile);
		outputFile.close();
				
	}
	
	/**
	 * Compare the Matchbox profile outputs for two files
	 * @param pInFile input file
	 * @param pOutFile output file 
	 * @param pLogFile log file
	 * @throws IOException
	 */
	private void matchboxCompareProfile(String pInFile, String pOutFile, String pLogFile) throws IOException {
		System.out.println("matchboxCompareProfile("+pInFile+", "+pOutFile+", ...)");
		//build the command line
		//NOTE: each option must be separated otherwise things don't work
		//we could do this over a single string but we will need to make
		//sure it doesn't split filenames etc
		List<String> commandLine = new ArrayList<String>();
		commandLine.add(Settings.MATCHBOX_COMPARE);
		//things go wrong if this is empty and it's added to the command line!
		
		//now set inputs to workflow ports
		commandLine.add(pInFile+Settings.MATCHBOX_EXT_PROFILE);
		commandLine.add(pOutFile+Settings.MATCHBOX_EXT_PROFILE);
		
		int exitCode = runCommand(commandLine);

		//store the file of the stdout console output (this is the XML)
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pOutFile+Settings.MATCHBOX_COMP_PROFILE_EXT));
		Tools.writeBufferToFile(gStdout, outputFile);
		outputFile.close();
		
		//store the log file of the console output
		outputFile = new BufferedWriter(new FileWriter(pLogFile,true));
		//write the command line to the file
		Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
		//write the log of stdout and stderr to the logfile
		Tools.appendBufferToFile("stderr", gStderr, outputFile);
		outputFile.close();
		
		
	}


	/**
	 * Checks the encode settings with the profile in the jpylyzer file
	 * @param pJpylyzerFile
	 * @param pLogFile
	 * @throws IOException
	 */
	private void checkEncodeSettings(String pJpylyzerFile, String pLogFile) throws IOException {
		System.out.println("checkEncodeSettings("+pJpylyzerFile+", ...)");

		boolean encodeOK = false;
		try {
			if(gSchematron) {
				encodeOK = JpylyzerSchematron.checkJpylyzerOutput(Tools.getResource(CommandLineJob.class, Settings.SCHEMATRON), pJpylyzerFile);
			} else {
				encodeOK = JP2Check.checkJpylyzerProfile(pJpylyzerFile, new JP2Profile());
			}
		} catch(Exception e) {
			e.printStackTrace();
		}

		//log the results
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pLogFile,true));

		outputFile.write("------------------------------");outputFile.newLine();
		outputFile.write("Checked Jpylyzer XML versus encode settings");outputFile.newLine();
		if(gSchematron) {
			outputFile.write("Using Schematron check");outputFile.newLine();
		}
		outputFile.write("Output JP2 matched encode settings: "+encodeOK);outputFile.newLine();
		outputFile.write("------------------------------");outputFile.newLine();		
		outputFile.close();	
		
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

		//NOTE: all methods *must* be passed absolute file references for new files
		
		//calc checksum.  TODO: check against input file to check it is ok
		Tools.writeChecksumToLog(gInFiles[0], Tools.generateChecksum(gTempDir+gInFiles[0]), gLogFile);
		
		//store the metadata from the original file
		extractMetadata(gInFiles[0], gTempDir+gInFiles[0]+Settings.EXIFTOOL_EXT, gLogFile);
		gGeneratedFiles.add(gInFiles[0]+Settings.EXIFTOOL_EXT);
		gGeneratedFiles.add(gLogFile);

		//migrate file and write info to logfile
		migrateFile(gInFiles[0], gTempDir+gOutFile, gLogFile);
		gGeneratedFiles.add(gOutFile);
		
		//if the file wasn't migrated successfully then skip the following steps and
		//report failure.  Doing it this was obviates the need to verify the tiff in the
		//first place and thus removes a dependency (cf. checkInputTIFFIsValid())
		File jp2File = new File(gTempDir+gOutFile);
		//add a check that the output jp2 is not less than 200 bytes long as Kakadu will output a ~85 byte 
		//file even if migration fails (due to error in tiff etc)
		if(jp2File.exists()&(jp2File.length()>200)) {
		
			//store metadata for the new file
			extractMetadata(gOutFile, gTempDir+gOutFile+Settings.EXIFTOOL_EXT, gLogFile);
			gGeneratedFiles.add(gOutFile+Settings.EXIFTOOL_EXT);

			//gets the validation info for the new file (jpylyzer)
			getValidationInfo(gOutFile, gTempDir+gOutFile+Settings.JPYLYZER_EXT, gLogFile);
			gGeneratedFiles.add(gOutFile+Settings.JPYLYZER_EXT);

			//maybe extract the ICC profiles?

			//compare jpylyzer output with input profile here
			//should we do this instead?
			// http://openplanetsfoundation.org/blogs/2012-09-04-automated-assessment-jp2-against-technical-profile
			//no - that has a hardcoded profile 
			checkEncodeSettings(gTempDir+gOutFile+Settings.JPYLYZER_EXT, gLogFile);

			if(gMatchboxEnabled) {
				//use matchbox to extract the characteristics of the original image
				matchboxExtractFeatures(gTempDir+gInFiles[0], gLogFile);
				gGeneratedFiles.add(gInFiles[0]+Settings.MATCHBOX_EXT_HISTOGRAM);
				gGeneratedFiles.add(gInFiles[0]+Settings.MATCHBOX_EXT_PROFILE);
				gGeneratedFiles.add(gInFiles[0]+Settings.MATCHBOX_EXT_METADATA);
				gGeneratedFiles.add(gInFiles[0]+Settings.MATCHBOX_EXT_SIFTCOMPARISON);

				//use matchbox to extract the characteristics of the new image
				matchboxExtractFeatures(gTempDir+gOutFile, gLogFile);
				gGeneratedFiles.add(gOutFile+Settings.MATCHBOX_EXT_HISTOGRAM);
				gGeneratedFiles.add(gOutFile+Settings.MATCHBOX_EXT_PROFILE);
				gGeneratedFiles.add(gOutFile+Settings.MATCHBOX_EXT_METADATA);
				gGeneratedFiles.add(gOutFile+Settings.MATCHBOX_EXT_SIFTCOMPARISON);

				//use matchbox to compare the sift characteristics of the images
				matchboxCompareSIFT(gTempDir+gInFiles[0], gTempDir+gOutFile, gLogFile);
				gGeneratedFiles.add(gOutFile+Settings.MATCHBOX_COMP_SIFT_EXT);

				//use matchbox to compare the profile characteristics of the images
				matchboxCompareProfile(gTempDir+gInFiles[0], gTempDir+gOutFile, gLogFile);
				gGeneratedFiles.add(gOutFile+Settings.MATCHBOX_COMP_PROFILE_EXT);

				//matchbox histogram comparison doesn't work??
			} 
			if(gImagemagickEnabled) {

				imagemagickComparePSNR(gTempDir+gInFiles[0], gTempDir+gOutFile, gLogFile);
				gGeneratedFiles.add(gOutFile+Settings.IMAGEMAGICK_PSNR_EXT);

			}
			if(gDissimilarEnabled) {

				runDissimilar(gTempDir+gInFiles[0], gTempDir+gOutFile, gLogFile);
				gGeneratedFiles.add(gOutFile+Settings.DISSIMILAR_EXT);

			}

			//generate a short log
			String reportFile = gOutFile+".report.xml";
			generateShortReport(gTempDir+reportFile);
			gGeneratedFiles.add(reportFile);

		} else {
			//input tiff is not readable for some reason - ensure that en error is logged
			//write an error to the log file
			PrintWriter logFile = new PrintWriter(new FileWriter(gLogFile,true));
			logFile.println("ERROR: file not readable: "+gInFiles[0]);
			logFile.close();
			
			gSuccess = false;
			
		}
		//finally, generate a checksum for the all files incl the log file
		//a place to store the checksums
		HashMap<String, String> checksums = new HashMap<String, String>();
		System.out.println("Generating checksums...");
		for(String file : gGeneratedFiles) {
			System.out.println(file);
			File temp = new File(gTempDir+(new File(file).getName()));
			if(temp.exists())
				checksums.put(file, Tools.generateChecksum(temp.getAbsolutePath()));
		}

		if(gAddLogToZip) {
			//hack?
			File log = new File(gLogFile);
			checksums.put(log.getName(), Tools.generateChecksum(log.getAbsolutePath()));
			gGeneratedFiles.add(log.getName());
		}

		System.out.println("Generating zip file (with bagit style info)");
		//zip all the generated files together

		Tools.zipGeneratedFiles(gSuccess, checksums, gGeneratedFiles, gTempDir+gOutFile+".zip", gTempDir);
	} 

	/**
	 * Generate a short report from the workflow data
	 * @param pReportFile filename to write report to
	 * @return whether report reports overall success or failure
	 * @throws IOException
	 */
	private boolean generateShortReport(String pReportFile) throws IOException {
		boolean generatedIsValid = JP2Check.jpylyzerSaysValid(gTempDir+gOutFile+Settings.JPYLYZER_EXT);
		boolean generatedMatchesInputProfile;
		if(gSchematron) {
			generatedMatchesInputProfile = JpylyzerSchematron.checkJpylyzerOutput(Tools.getResource(CommandLineJob.class, Settings.SCHEMATRON), gTempDir+gOutFile+Settings.JPYLYZER_EXT);
		} else {
			generatedMatchesInputProfile = JP2Check.checkJpylyzerProfile(gTempDir+gOutFile+Settings.JPYLYZER_EXT, new JP2Profile());
		}
		boolean ssimMatch = true;
		boolean imagemagickMatch = true;
		
		BufferedWriter out = new BufferedWriter(new FileWriter(pReportFile));
		out.write("<?xml version='1.0' encoding='ascii'?>");out.newLine();
		out.write("<migrationReport>");out.newLine();
		out.write("     <jpylyzerSaysOutputValid>"+generatedIsValid+"</jpylyzerSaysOutputValid>");out.newLine();
		out.write("     <outputMatchesInputProfile>"+generatedMatchesInputProfile+"</outputMatchesInputProfile>");out.newLine();
		if(gMatchboxEnabled) {
			//note the following comparison (>0.9) is one used in Matchbox's MatchboxLib.py 
			ssimMatch = Tools.getSSIMCompareVal(gTempDir+gOutFile+Settings.MATCHBOX_COMP_SIFT_EXT)>Settings.MATCHBOX_THRESHOLD;
			out.write("     <ssimImageMatches>"+ssimMatch+"</ssimImageMatches>");out.newLine(); 
		}
		if(gImagemagickEnabled) {
			imagemagickMatch = Tools.getPSNRVal(gTempDir+gOutFile+Settings.IMAGEMAGICK_PSNR_EXT)>Settings.PSNR_THRESHOLD;
			out.write("     <psnrMatches>"+imagemagickMatch+"</psnrMatches>");out.newLine(); 			
		}
		out.write("</migrationReport>");out.newLine();
		out.close();
		
		gSuccess = (((generatedIsValid&generatedMatchesInputProfile&ssimMatch&imagemagickMatch)==true));
		
		return gSuccess;
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
		Tools.deleteDirectory(new File(gTempDir));			
	}
	
	/**
	 * A test main method for this class to be run standalone
	 * @param args command line arguments
	 * @throws IOException file access error
	 */
	public static void main(String[] args) throws IOException  {

		String[] inputFiles = new String[1];
		inputFiles[0] = Settings.STANDALONE_TEST_INPUT;

		//create new object and execute
		ChutneyJob tsb = new CommandLineJob(inputFiles, Settings.STANDALONE_TEST_OUTPUT);
		tsb.setup();
		
		tsb.run();
		
		//NEVER DO THIS (if it deletes the files)
		//tsb.cleanup();
		
	}

	public static JobType getJobType() {
		return JobType.CommandLineJob;
	}

	public static String getShortJobType() {
		return "CLJ";
	}
	
}
