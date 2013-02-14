/*
 * Copyright 2012, 2013 The SCAPE Project Consortium
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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import eu.scape_project.tb.tavernahadoopwrapper.WrapperSettings.JobType;

/**
 * This class initiates Hadoop and runs the specified job.
 * It is responsible for the initial retreival of input files
 * as well as the storage of output files
 * 
 * @author wpalmer
 *
 */
public class TavernaHadoopWrapper extends Configured implements Tool  {

	/**
	 * This is the Map class that will be run on remote Hadoop nodes.
	 * Note that the nodes will not have access to the parent class.
	 * @author wpalmer
	 *
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		/**
		 * Name of the job
		 */
		public static String jobName;
		
		/**
		 * Job type class to use for job execution
		 */
		public static JobType jobType;
		
		/**
		 * The file containing the XML code
		 */
		public static String xmlCode;
		
		/**
		 * <b>Not currently used</b>.  Path to pass to FileTracker for storage.
		 */
		public static String outputPath; 
		
		/**
		 * The main map method - this is where the work happens
		 * @param arg0 Line number of the input file (unused)
		 * @param inputFile Name of the local temp input file
		 * @param collector Collects information after the map is complete - just collects 
		 * input name and output name at the moment
		 * @param arg3 (unused)
		 * @throws IOException file access issue
		 */
		@Override
		public void map(LongWritable arg0, Text inputFile,
				OutputCollector<Text, Text> collector, Reporter arg3)
				throws IOException {
			
			System.out.println("map");
			
			//make new local temp directory 
			File localTempDir = Tools.newTempDir();
			//boolean success = false;
			String prevsuccess = "";
			
			//check inputfile
			String inputLine = inputFile.toString();
			//if input is blank then skip!
			if(inputLine.equals("")) return;
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

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			//don't use File - need to deal with HDFS:// URIs
			String shortInputFileName = "";
			if(inputFiles.size()>0) {
				shortInputFileName = inputFiles.get(0).substring(inputFiles.get(0).lastIndexOf("/")+1);
			}
			//this is initialized here to stop Eclipse complaining
			//this class should not be used until otherwise instantiated!
			FileTracker fileTracker = new FileTracker();
			
			//copy the input files to a local directory
			//this should be done above and
			//the xml jobs will copy in the files as required
			//but leave this here for now so it copies the files anyway
			if(!(jobType==JobType.XMLCommandLineJob||jobType==JobType.XMLWorkflowReport)) {
				//localTempDir = Tools.newTempDir();
				for(String file : inputFiles) {
					String shortFile = file;
					if(file.contains("/")) {
						shortFile = file.substring(file.lastIndexOf("/"));
					}
					//if we copied the file above then don't copy it again
					if(!(new File(localTempDir.getAbsolutePath()+"/"+shortFile).exists())) {
						//try and copy from the input path - this should be a full path...
						Tools.copyInputToLocalTemp(localTempDir,fs,file);											
					}
				}
			}
			
			HadoopJob job;
			//setup the class for execution
			switch(jobType) {
				case TavernaCommandLine: {
					//THESE VALUES ARE WORKFLOW SPECIFIC
					//load input ports/values to a list
					HashMap<String, String> list = new HashMap<String, String>();
					list.put(WrapperSettings.TAVERNA_WORKFLOW_INPUTFILEPORT, new File(inputFiles.get(0)).getName());
					//HACK for nicename workflows:
					list.put(WrapperSettings.TAVERNA_WORKFLOW_OUTPUTFILEPORT, new File(inputFiles.get(0)).getName());
					job = new TavernaCommandLineJob(list, WrapperSettings.TAVERNA_WORKFLOW, localTempDir);
					break;
				}
				case TavernaServerJob: {
					//THESE VALUES ARE WORKFLOW SPECIFIC
					//load input ports/values to a list
					HashMap<String, String> list = new HashMap<String, String>();
					list.put(WrapperSettings.TAVERNA_WORKFLOW_INPUTFILEPORT, new File(inputFiles.get(0)).getName());
					//HACK for nicename workflows:
					list.put(WrapperSettings.TAVERNA_WORKFLOW_OUTPUTFILEPORT, new File(inputFiles.get(0)).getName());
					job = new TavernaServerJob(list, WrapperSettings.TAVERNA_WORKFLOW);
					break;
				}
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
							//fileTracker = new FileTracker(fs,shortInputFileName);	
							//hash = fileTracker.getHash();
						}
					}
					
					localTempDir = new File(fileTracker.getLocalTempDir());
					
					File xml = new File(xmlCode); 
					
					//check to see if we need to recover file from hdfs
					if(!xml.exists()) {
						System.out.print("loading xml from HDFS: ");
						xml = Tools.copyInputToLocalTemp(localTempDir, fs, xmlCode);
						System.out.println("done");
					}
					
					if(fileTracker==null) System.out.println("NULL fileTracker");
					if(localTempDir==null) System.out.println("NULL localTempDir");
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
								collector.collect(new Text(""), new Text("SUCCESS:"+false+", HASH:"+hash+""));
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
						//collector.collect(new Text(""), new Text("SUCCESS:"+false+", HASH:"+hash+""));
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
								
								collector.collect(new Text(""), new Text("SUCCESS:"+false+", HASH:"+hash+""));
								return;
							}
						}
					break;
				}
				default:
				case CommandLineJob: {
					String[] shortNames = new String[inputFiles.size()];
					for(int i=0;i<inputFiles.size();i++) {
						shortNames[i] = new File(inputFiles.get(i)).getName();
					}
					job = new CommandLineJob(shortNames, localTempDir.toString());
					break;
				}
			}

			String hdfsOutputDir = WrapperSettings.OUTPUT_DIR+jobName+"/";
			
			//set up job
			job.setup();
						
			//start execution
			job.run();
			
			boolean success = job.wasSuccessful(); 
			//copy the local output file and log file to hdfs
			String outputFiles = "";
			//TODO: do we only want outputs when there is not a failure (would not allow 
			//failure to be diagnosed)
//			if(success) {
				//iterate through all the output files
				for(String s:job.getOutputFiles()) {
					//copy the file to hdfs
					String hdfsFilename = new File(s).getName();
					//hack to work around output port filename issue for taverna workflows
					if(jobType==JobType.TavernaCommandLine||jobType==JobType.TavernaServerJob) {
						hdfsFilename = shortInputFileName+"."+(new File(s).getName());
						fs.copyFromLocalFile(new Path(s), new Path(hdfsOutputDir+hdfsFilename));
						outputFiles += hdfsFilename+", ";
						continue;
					}
					if(new File(s).exists()) {
						//if this is an xml job do this
						if(jobType==JobType.XMLCommandLineJob) {
							fileTracker.storeFile(s, hdfsFilename);
							outputFiles += fileTracker.getHDFSFilePath(hdfsFilename)+", ";
						} else {
							//if not, just store the files normally
							fs.copyFromLocalFile(new Path(s), new Path(hdfsOutputDir+hdfsFilename));
							outputFiles += hdfsOutputDir+hdfsFilename+", ";
						}
					}
				}
//			}
			
			//copy the log file seperately - we might have no output files or the job may have
			//gone badly - make sure to get at least a log
			String logFile = job.getLogFilename();
			if(null!=logFile) {
				if(jobType==JobType.XMLCommandLineJob) {
					fileTracker.storeFile(logFile, new File(logFile).getName());
				} else {
					fs.copyFromLocalFile(new Path(logFile), new Path(hdfsOutputDir+new File(logFile).getName()));
				}
			}

			//XML command line job specific cleanup/end of job code
			if(jobType==JobType.XMLCommandLineJob) {
				//store whether this job was a success
				JMSComms.sendMessage(fileTracker.getHash(), "SUCCESS:"+success+":"+((XMLCommandLineJob)job).getXMLName());

				//if a previous stage failed then fail through this stage too
				if(prevsuccess.length()>0&(new Boolean(prevsuccess)==false)) {
					success = false;
				}
			}
			
			//TODO: delete temporary copy of input file?
			//Note this must be here for TavernaServerJob otherwise no new workflows could be processed
			job.cleanup();
			
			//doing it this way means that if we have no hash there is no hash output
			if(!hash.equals(""))
				hash = "HASH:"+hash+",";

			//store information in a log file in HDFS
			//TODO: don't put any output files here - just use the hash?
			collector.collect(new Text(outputFiles), new Text("SUCCESS:"+success+", "+hash+""));
			
			//TESTING AREA
			//for XML jobs only
			//if(jobType==JobType.XMLCommandLineJob)
			//	fileTracker.writeList("/tmp/"+jobName+".txt");			
		
			//delete the temp directory, if it still exists
			if(localTempDir.exists())
				Tools.deleteDirectory(localTempDir);
			
		}


		/**
		 * Method run at the end of the map
		 */
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			super.close();
		}

		/**
		 * Method run at the beginning of the map.  Note this is where we can recover settings
		 * passed to us by the parent class like job type and job name.
		 */
		@Override
		public void configure(JobConf job) {
			super.configure(job);

			//get the job name from the config
			jobName = job.getJobName();
			outputPath = job.get(WrapperSettings.OUTPUTPATH_CONF_SETTING);
			
			//get the type of job we are running from the config
			String jobType = job.get(WrapperSettings.JOBTYPE_CONF_SETTING);
			//could iterate over the enum instead?
			if(jobType.equals(JobType.CommandLineJob.toString())) {
				Map.jobType = JobType.CommandLineJob;
			} else if(jobType.equals(JobType.TavernaCommandLine.toString())) {
				Map.jobType = JobType.TavernaCommandLine;
			} else if(jobType.equals(JobType.TavernaServerJob.toString())) {
				Map.jobType = JobType.TavernaServerJob;
			} else if(jobType.equals(JobType.XMLWorkflowReport.toString())) {
				Map.jobType = JobType.XMLWorkflowReport;
			} else if(jobType.equals(JobType.XMLCommandLineJob.toString())) {
				Map.jobType = JobType.XMLCommandLineJob;
				xmlCode = job.get(WrapperSettings.XMLCODE_CONF_SETTING);
			} 
			
		}

	}

	/**
	 * This method sets up and runs the job on Hadoop
	 * @param args The passed through command line arguments
	 */
	public int run(String[] args) {
		
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("j", "jobname", true, "name to assign to the hadoop job");
		options.addOption("i", "inputlist", true, "text file containing list of input files (ensure no trailing carriage returns)");
		options.addOption("t", "jobtype", true, "type of job; CLJ (command line job), TSJ (Taverna Server job), TCL (Taverna command line job), XML (XML defined command line job), XWR (XML workflow report)");
		options.addOption("x", "xmlcode", true, "xml definition of job to run for XML jobs");
		options.addOption("o", "output", true, "hdfs output path for files");
		options.addOption("m", "maps", true, "number of maps to run in parallel (e.g. (# slaves)*2");
		options.addOption("h", "help", false, "help text");

		JobConf conf = new JobConf(TavernaHadoopWrapper.class);
		
		String input = null;
		String xmlcode = null;
		String outputPath = "";
		int maps = WrapperSettings.NUM_MAPS;

		CommandLine com;
		try {
			com = parser.parse(options, args);
			if(com.hasOption("help")) {
				throw(new ParseException(""));
			}

			String jobName = WrapperSettings.JOB_NAME+"default";
			if(com.hasOption("jobname")) {
			//set the job name to something better than the default
				jobName = WrapperSettings.JOB_NAME+com.getOptionValue("jobname");
			} 
			conf.setJobName(jobName);

			JobType jobType = JobType.CommandLineJob;
			if(com.hasOption("jobtype")) {
				String value = com.getOptionValue("jobtype").toUpperCase();
				if(value.equals("CLJ")) {
					jobType = JobType.CommandLineJob;
				} else
					if(value.equals("TCL")) {
						jobType = JobType.TavernaCommandLine;
					} else
						if(value.equals("TSJ")) {
							jobType = JobType.TavernaServerJob;
						}
						else 
							if(value.equals("XML")) {
								jobType = JobType.XMLCommandLineJob;
							} else 
								if(value.equals("XWR")) {
									jobType = JobType.XMLWorkflowReport;
								}
			}
			System.out.println("JobType: "+jobType.toString());
			conf.set(WrapperSettings.JOBTYPE_CONF_SETTING,jobType.toString());

			if(com.hasOption("xmlcode")) {
				//jobType == JobType.XMLCommandLineJob
				xmlcode = com.getOptionValue("xmlcode");
				//if it is a local file get the full path
				if(new File(xmlcode).exists()) xmlcode = new File(xmlcode).getAbsolutePath();
				conf.set(WrapperSettings.XMLCODE_CONF_SETTING, xmlcode);
			}
			if((jobType == JobType.XMLCommandLineJob)&(xmlcode==null)) {
				//i.e. no code specified
				System.out.println("No XML code specified on the command line");
				return -1;
			}
			
			if(com.hasOption("inputlist")) {
				input = com.getOptionValue("inputlist");
			} 
			if(input.equals(null)) {
				System.out.println("no input given");
				return -2;
			}
			
			if(com.hasOption("output")) {
				outputPath = com.getOptionValue("output");
				conf.set(WrapperSettings.OUTPUTPATH_CONF_SETTING, outputPath);
			}

			if(com.hasOption("maps")) {
				maps = Integer.parseInt(com.getOptionValue("maps"));
			} 
			
		} catch (ParseException e) {
			HelpFormatter help = new HelpFormatter();
			help.printHelp("hadoop jar TavernaHadoopWrapper.jar", options);
			return -1;
		}
				
		//using matchbox it may take a while to process the jobs
		//set a longer timeout than the default (10 mins)
		conf.set("mapred.task.timeout", Integer.toString(15*60*1000));

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(conf.getJobName()));
		
		//set the mapper to this class' mapper
		conf.setMapperClass(Map.class);
		//we don't want to reduce
		//conf.setReducerClass(Reducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		//sets how the output is written cf. OutputFormat
		//we can use nulloutputformat if we are writing our own output
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		//this sets maximum jvm reuse
		conf.set("mapred.job.reuse.jvm.num.tasks", "-1");
		
		//set the number of maps to use (default is 2)
		conf.setNumMapTasks(maps);
		
		try {
			JobClient.runJob(conf);
		} catch(IOException ioe) {
			ioe.printStackTrace();
			return -1;
		}
		
		return 0;
	}

	/**
	 * This main runs only on the local machine when the job is initiated
	 * 
	 * @param args command line arguments
	 * @throws ParseException command line parse issue
	 */
	public static void main(String[] args) throws ParseException {
		
		System.out.println("TavernaHadoopWrapper v"+WrapperSettings.VERSION);

		//warning code
		String yes = "YeS";
		System.out.println("WARNING: this code has not been fully tested and it may delete/modify/alter your systems");
		System.out.println("You run this development code at your own risk, no liability is assumed");
		System.out.print("To continue, type \""+yes+"\": ");
		try {
			String input = new BufferedReader(new InputStreamReader(System.in)).readLine();
			if(!input.equals(yes)) {
				return;
			}
		} catch (IOException e1) {
			return;
		}
		//end of warning code
		
		try {
			ToolRunner.run(new Configuration(), new TavernaHadoopWrapper(), args);
		} catch(Exception e) {
			e.printStackTrace();
		}
		
	}

}
