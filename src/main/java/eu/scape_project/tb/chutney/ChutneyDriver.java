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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import eu.scape_project.tb.chutney.Settings.JobType;
import eu.scape_project.tb.chutney.jobs.CommandLineJob;
import eu.scape_project.tb.chutney.jobs.TavernaCommandLineJob;
import eu.scape_project.tb.chutney.jobs.TavernaServerJob;
import eu.scape_project.tb.chutney.jobs.XMLCommandLineJob;
import eu.scape_project.tb.chutney.jobs.XMLWorkflowReport;

/**
 * This class initiates Hadoop and runs the specified job.
 * It is responsible for the initial retreival of input files
 * as well as the storage of output files
 * 
 * @author wpalmer
 *
 */
public class ChutneyDriver extends Configured implements Tool {

	/**
	 * This method sets up and runs the job on Hadoop
	 * @param args The passed through command line arguments
	 */
	public int run(String[] args) {
		
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("n", "jobname", true, "name to assign to the hadoop job");
		options.addOption("i", "inputlist", true, "text file containing list of input files (ensure no trailing carriage returns)");
		options.addOption("t", "jobtype", true, "type of job; CLJ (command line job), TSJ (Taverna Server job), TCL (Taverna command line job), XML (XML defined command line job), XWR (XML workflow report)");
		options.addOption("x", "xmlcode", true, "xml definition of job to run for XML jobs");
		options.addOption("h", "help", false, "help text");

		JobConf conf = new JobConf(ChutneyDriver.class);
		
		String input = null;
		String xmlcode = null;

		CommandLine com;
		try {
			com = parser.parse(options, args);
			if(com.hasOption("help")) {
				throw(new ParseException(""));
			}

			String jobName = Settings.JOB_NAME+"default";
			if(com.hasOption("jobname")) {
			//set the job name to something better than the default
				jobName = Settings.JOB_NAME+com.getOptionValue("jobname");
			} 
			conf.setJobName(jobName);

			JobType jobType = JobType.CommandLineJob;
			if(com.hasOption("jobtype")) {
				String value = com.getOptionValue("jobtype").toUpperCase();
				if(value.equals(CommandLineJob.getShortJobType())) {
					jobType = CommandLineJob.getJobType();
				} else
					if(value.equals(TavernaCommandLineJob.getShortJobType())) {
						jobType = TavernaCommandLineJob.getJobType();
					} else
						if(value.equals(TavernaServerJob.getShortJobType())) {
							jobType = TavernaServerJob.getJobType();
						} else 
							if(value.equals(XMLCommandLineJob.getShortJobType())) {
								jobType = XMLCommandLineJob.getJobType();
							} else 
								if(value.equals(XMLWorkflowReport.getShortJobType())) {
									jobType = XMLWorkflowReport.getJobType();
								} 
			}
			System.out.println("JobType: "+jobType.toString());
			conf.set(Settings.JOBTYPE_CONF_SETTING,jobType.toString());

			if(com.hasOption("xmlcode")) {
				//jobType == JobType.XMLCommandLineJob
				xmlcode = com.getOptionValue("xmlcode");
				//if it is a local file get the full path
				if(new File(xmlcode).exists()) xmlcode = new File(xmlcode).getAbsolutePath();
				conf.set(Settings.XMLCODE_CONF_SETTING, xmlcode);
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
			
		} catch (ParseException e) {
			HelpFormatter help = new HelpFormatter();
			help.printHelp("hadoop jar TavernaHadoopWrapper.jar", options);
			return -1;
		}
				
		//using matchbox it may take a while to process the jobs
		//set a longer timeout than the default (10 mins)
		//six hours should be more than enough :/        MMM*SS*MS
		//QAJob testing for 9 tests on ANJO files can take ~4.5hrs+
		conf.set("mapred.task.timeout", Integer.toString(360*60*1000));

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(conf.getJobName()));
		
		//set the mapper to this class' mapper
		conf.setMapperClass(Chutney.class);
		//we don't want to reduce
		//conf.setReducerClass(Reducer.class);
		
		//this input format should split the input by one line per map by default.
		conf.setInputFormat(NLineInputFormat.class);
		conf.setInt("mapred.line.input.format.linespermap", 1);
		
		//sets how the output is written cf. OutputFormat
		//we can use nulloutputformat if we are writing our own output
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		//this sets maximum jvm reuse
		conf.set("mapred.job.reuse.jvm.num.tasks", "-1");
		
		//we only want one reduce task
		conf.setNumReduceTasks(1);
		
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
	@SuppressWarnings("unused")
	public static void main(String[] args) throws ParseException {
		
		System.out.println(Settings.JOB_NAME+"v"+Settings.VERSION);

		//warning code
		if(true) {
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
		}
		//end of warning code
		
		try {
			ToolRunner.run(new Configuration(), new ChutneyDriver(), args);
		} catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
