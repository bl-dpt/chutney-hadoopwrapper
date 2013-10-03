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

package eu.scape_project.tb.chutney;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import uk.bl.dpt.fclib.FedoraDataConnector;
import uk.bl.dpt.fclib.WebdavDataConnector;
import eu.scape_project.tb.chutney.Settings.JobType;
import eu.scape_project.tb.chutney.fs.ChutneyFS;
import eu.scape_project.tb.chutney.fs.FedoraFS;
import eu.scape_project.tb.chutney.fs.HDFSFS;
import eu.scape_project.tb.chutney.fs.WebdavFS;
import eu.scape_project.tb.chutney.jobs.CommandLineJob;
import eu.scape_project.tb.chutney.jobs.ChutneyJob;
import eu.scape_project.tb.chutney.jobs.TavernaCommandLineJob;
import eu.scape_project.tb.chutney.jobs.TavernaServerJob;
import eu.scape_project.tb.chutney.jobs.XMLCommandLineJob;
import eu.scape_project.tb.chutney.jobs.XMLWorkflowReport;

/**
* This is the Map class that will be run on remote Hadoop nodes.
* @author wpalmer
*
*/
public class Chutney extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	/**
	 * Name of the job
	 */
	public static String gJobName;

	/**
	 * Job type class to use for job execution
	 */
	public static JobType gJobType;

	/**
	 * The file containing the XML code
	 */
	public static String gXmlCode;

	/**
	 * The main map method - this is where the work happens
	 * @param pArg0 Line number of the input file (unused)
	 * @param pInputFile Name of the local temp input file
	 * @param pCollector Collects information after the map is complete - just collects 
	 * input name and output name at the moment
	 * @param pArg3 (unused)
	 * @throws IOException file access issue
	 */
	@Override
	public void map(LongWritable pArg0, Text pInputFile,
			OutputCollector<Text, Text> pCollector, Reporter pArg3)
					throws IOException {

		long copyTimeGet = 0;
		long copyTimePut = 0;
		long inputSize = 0;
		long inputCount = 0;
		long outputSize = 0;
		long outputCount = 0;

		ChutneyFS chutneyFS = null;
		String shortFN = null;

		//make new local temp directory 
		File localTempDir = Tools.newTempDir();
		//boolean success = false;
		String prevsuccess = "";

		//check inputfile
		String inputLine = pInputFile.toString();
		//if input is blank then skip!
		if(inputLine.equals("")) return;

		//initialise HDFS connection
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		String hdfsOutputDir = Settings.OUTPUT_DIR+gJobName+"/";

		FileTracker fileTracker = null;

		//determine fs to use
		if(inputLine.contains(FedoraDataConnector.DC_URI)) {
			chutneyFS = new FedoraFS(fs, hdfsOutputDir);
		} else {
			if(inputLine.contains(WebdavDataConnector.DC_URI)) {
				chutneyFS = new WebdavFS();
			} else {
				//assume hdfs
				chutneyFS = new HDFSFS(fs, hdfsOutputDir);
			}
		}

		//NOTE: this is in the output format from the collector at the end of this method
		String[] split = inputLine.split(",");
		//hash in the output
		String hash = "";
		//chomp the string and look for any hash code
		List<String> inputFiles = new LinkedList<String>();
		for(int i=0;i<split.length;i++) {
			//chomp
			split[i] = split[i].trim();
			//find a hash
			if(split[i].startsWith("HASH:")) {
				hash = split[i].substring("HASH:".length());
				continue;
			}
			//find whether previous step was successful
			if(split[i].startsWith("SUCCESS:")) {
				prevsuccess = split[i].substring("SUCCESS:".length());
				continue;
			}	
			//assume that this must be an input file if we reach here
			inputFiles.add(split[i]);
		}

		//copy the input files to a local directory
		//this should be done above and
		//the xml jobs will copy in the files as required
		//but leave this here for now so it copies the files anyway
		if(!(gJobType==JobType.XMLCommandLineJob||gJobType==JobType.XMLWorkflowReport)) {
			//localTempDir = Tools.newTempDir();
			for(int i=0;i<inputFiles.size();i++) {
				String shortFile = inputFiles.get(i);
				if(inputFiles.get(i).contains("/")) {
					shortFile = inputFiles.get(i).substring(inputFiles.get(i).lastIndexOf("/"));
				}
				//if we copied the file above then don't copy it again
				//note this will be false for fedora streams due to shortFile containing the datastream 
				if(!(new File(localTempDir.getAbsolutePath()+"/"+shortFile).exists())) {
					long start = System.currentTimeMillis();
					File tInput = chutneyFS.getFile(inputFiles.get(i), localTempDir);
					copyTimeGet += System.currentTimeMillis() - start;
					inputFiles.set(i, tInput.getAbsolutePath());
					shortFN = tInput.getName();
					inputSize += tInput.length();
					inputCount++;
				}
			}
		}
		
		//we do this here so that entries here are replaced by proper names (cf. fedora)
		String[] shortNames = new String[inputFiles.size()];
		for(int i=0;i<inputFiles.size();i++) {
			shortNames[i] = inputFiles.get(0).substring(inputFiles.get(0).lastIndexOf("/")+1);//new File(inputFiles.get(i)).getName();
		}
		
		//don't use File - need to deal with HDFS:// URIs
		String shortInputFileName = null;
		if(inputFiles.size()>0) {
			shortInputFileName = shortNames[0];//
		}
		
		//this is the workflow setup for either Taverna job type 
		HashMap<String, String> tavernaInput = null; 
		if(gJobType==JobType.TavernaCommandLine||gJobType==JobType.TavernaServerJob) {
			//copy the workflow from the resource to the local temp directory
			Tools.copyResourceToFile(Chutney.class, Settings.TAVERNA_WORKFLOW, localTempDir.getAbsolutePath()+"/"+Settings.TAVERNA_WORKFLOW);
			//THESE VALUES ARE WORKFLOW SPECIFIC
			//load input ports/values to a list
			tavernaInput = new HashMap<String, String>();
			tavernaInput.put("inputTIFFFile", localTempDir.getAbsolutePath()+"/"+new File(inputFiles.get(0)).getName());
			//HACK for nicename workflows:
			tavernaInput.put("inputOriginalName", new File(inputFiles.get(0)).getName());
			//extract schematron file from jar to the local temp directory
			Tools.copyResourceToFile(Chutney.class, Settings.SCHEMATRON_COMPILED, localTempDir.getAbsolutePath()+"/"+Settings.SCHEMATRON_COMPILED);
			tavernaInput.put("inputSchematron", localTempDir.getAbsolutePath()+"/"+Settings.SCHEMATRON_COMPILED);
		}
		
		ChutneyJob job;
		//setup the class for execution
		switch(gJobType) {
		case XMLCommandLineJob: {
			//set up the filetracker - we rely on being passed a filename if there is no hash
			if(hash.length()>0) {
				fileTracker = new FileTracker(fs,hash);
			} else {
				//assume we are the first file to be used in the tracker
				if(fs.exists(new Path(inputFiles.get(0)))) {
					//we need to generate the hash code for the file here but
					//the file is in hdfs
					File file = Tools.copyInputToLocalTemp(localTempDir,fs,inputFiles.get(0));
					hash = Tools.generateChecksumOnly(file.toString());
					if(shortInputFileName.length()<1) throw new IOException("No input file defined");
					fileTracker = new FileTracker(fs,shortInputFileName,hash,inputFiles.get(0));
					//HACK: move the file to the fileTracker temp directory so we don't
					//have to copy it again
					file.renameTo(new File(fileTracker.getLocalTempDir()+file.getName()));

				} else {
					//DANGER - possible collision if code reaches here (identical
					//filenames but different data)
					if(shortInputFileName.length()<1) throw new IOException("No input file defined");
				}
			}

			localTempDir = new File(fileTracker.getLocalTempDir());

			File xml = new File(gXmlCode); 

			//check to see if we need to recover file from hdfs
			if(!xml.exists()) {
				System.out.print("loading xml from HDFS: ");
				xml = Tools.copyInputToLocalTemp(localTempDir, fs, gXmlCode);
				System.out.println("done");
			}

			//if(fileTracker==null) System.out.println("NULL fileTracker");
			//if(localTempDir==null) System.out.println("NULL localTempDir");
			if(xml==null) System.out.println("NULL xml");
			job = new XMLCommandLineJob(fileTracker.getKeyFile(), localTempDir.toString(), xml.getAbsolutePath());


			//copy the files defined in the xml from the tracker to the local temp directory
			String[] inf = job.getInputFiles();
			if(null!=inf)
				for(String file:inf) {
					String shortFile = file.substring(file.lastIndexOf("/")+1);
					//check that the file we want actually exists, if not then error out
					if(fileTracker.exists(shortFile)) {
						fileTracker.makeFileLocal(shortFile);
					} else {
						//throw new IOException("Required input file does not exist in tracker: "+shortFile);
						pCollector.collect(new Text(""), new Text("SUCCESS:"+false+", HASH:"+hash+""));
						return;
					}
				}

			break;
		}
		case XMLWorkflowReport: {
			//set up the filetracker properly
			if(hash.length()>0) {
				fileTracker = new FileTracker(fs,hash);
			} else {
				return;//panic as we don't have a hash!
			}
			job = new XMLWorkflowReport(fileTracker);
			//copy the files defined in the xml from the tracker to the local temp directory
			List<String> inf = fileTracker.getFileList();
			if(null!=inf)
				for(String file:inf) {
					String shortFile = file.substring(file.lastIndexOf("/")+1);
					//check that the file we want actually exists, if not then error out
					if(fileTracker.exists(shortFile)) {
						fileTracker.makeFileLocal(shortFile);
					} else {
						//this should never happen as we asked the fileTracker for 
						//a list of files

						pCollector.collect(new Text(""), new Text("SUCCESS:"+false+", HASH:"+hash+""));
						return;
					}
				}
			break;
		}
		case TavernaCommandLine: {
			job = new TavernaCommandLineJob(tavernaInput, Settings.TAVERNA_WORKFLOW, localTempDir);
			break;
		}
		case TavernaServerJob: {
			job = new TavernaServerJob(tavernaInput, Settings.TAVERNA_WORKFLOW);
			break;
		}
		default:
		case CommandLineJob: {
			job = new CommandLineJob(shortNames, localTempDir.toString());
			break;
		}
		}

		//set up job
		job.setup();

		//start execution
		job.run();

		boolean success = job.wasSuccessful(); 
		//copy the local output file and log file to hdfs
		String outputFiles = "";
		//iterate through all the output files
		for(String s:job.getOutputFiles()) {
			//copy the file to hdfs
			File sFile = new File(s);
			//only copy files that exist
			if(sFile.exists()) {
				//if this is an xml job do this
				if(gJobType==JobType.XMLCommandLineJob) {
					fileTracker.storeFile(s, sFile.getName());
					outputFiles += fileTracker.getHDFSFilePath(sFile.getName())+", ";
				} else {
					String outputName = sFile.getName();
					//correct the output name for the taverna workflows
					if(gJobType==JobType.TavernaCommandLine||gJobType==JobType.TavernaServerJob) {
						outputName = shortInputFileName;
						if(sFile.getName().endsWith(".error")) {
							outputName += ".error";
						} else {
							outputName += ".zip";
						}
					}
					System.out.println("Storing "+sFile.getAbsolutePath()+" as "+outputName);
					//store the files using chutneyFS object
					long time = System.currentTimeMillis();
					String outputFile = chutneyFS.putFile(success, sFile, outputName, "JP2-ZIP", "MigrateToJP2", "application/zip", false);
					copyTimePut += System.currentTimeMillis() - time;
					outputFiles += outputFile+", ";
					outputSize += sFile.length();
					outputCount++;
				}
			} else {
				System.out.println("Error, output does not exist: "+s);
			}
		}

		if(chutneyFS!=null) {
			//store log for time spent in copy operations
			String metricsFile = shortFN+".metrics";
			PrintWriter out = new PrintWriter(new FileWriter(metricsFile));
			out.println("Copy metrics for "+chutneyFS.getType());
			out.println("Input line: "+inputLine);
			out.println("Get: Files: "+inputCount+", Size: "+inputSize+", MS in copy operation: "+copyTimeGet);
			out.println("Put: Files: "+outputCount+", Size: "+outputSize+", MS in copy operation: "+copyTimePut);
			out.close();
			chutneyFS.saveLogFile(new File(metricsFile));

			//copy the log file seperately - we might have no output files or the job may have
			//gone badly - make sure to get at least a log
			File tLogFile = new File(job.getLogFilename()); 
			if(tLogFile.exists()) {
				chutneyFS.saveLogFile(tLogFile);
			}
		}

		//XML command line job specific cleanup/end of job code
		if(gJobType==JobType.XMLCommandLineJob) {
			//store whether this job was a success
			JMSComms.sendMessage(fileTracker.getHash(), "SUCCESS:"+success+":"+((XMLCommandLineJob)job).getXMLName());

			//if a previous stage failed then fail through this stage too
			if(prevsuccess.length()>0&(new Boolean(prevsuccess)==false)) {
				success = false;
			}
		}

		//Note this must be here for TavernaServerJob otherwise no new workflows could be processed
		job.cleanup();

		//doing it this way means that if we have no hash there is no hash output
		if(!hash.equals(""))
			hash = "HASH:"+hash+",";

		//store information in a log file in HDFS
		//TODO: don't put any output files here - just use the hash?
		pCollector.collect(new Text(outputFiles), new Text("SUCCESS:"+success+", "+hash+""));

		//delete the temp directory, if it still exists
		if(localTempDir.exists())
			Tools.deleteDirectory(localTempDir);

	}


	/**
	 * Method run at the end of the map
	 */
	@Override
	public void close() throws IOException {
		super.close();
	}

	/**
	 * Method run at the beginning of the map.  Note this is where we can recover settings
	 * passed to us by the parent class like job type and job name.
	 */
	@Override
	public void configure(JobConf pJob) {
		super.configure(pJob);

		//get the job name from the config
		gJobName = pJob.getJobName();
		//outputPath = job.get(Settings.OUTPUTPATH_CONF_SETTING);

		//get the type of job we are running from the config
		String jobType = pJob.get(Settings.JOBTYPE_CONF_SETTING);
		//could iterate over the enum instead?
		if(jobType.equals(JobType.CommandLineJob.toString())) {
			gJobType = JobType.CommandLineJob;
		} else if(jobType.equals(JobType.TavernaCommandLine.toString())) {
			gJobType = JobType.TavernaCommandLine;
		} else if(jobType.equals(JobType.TavernaServerJob.toString())) {
			gJobType = JobType.TavernaServerJob;
		} else if(jobType.equals(JobType.XMLWorkflowReport.toString())) {
			gJobType = JobType.XMLWorkflowReport;
		} else if(jobType.equals(JobType.XMLCommandLineJob.toString())) {
			gJobType = JobType.XMLCommandLineJob;
			gXmlCode = pJob.get(Settings.XMLCODE_CONF_SETTING);
		} 

	}

}

