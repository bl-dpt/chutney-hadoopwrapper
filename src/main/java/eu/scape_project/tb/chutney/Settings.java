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

/**
 * A class containing constants for use elsewhere in the code
 * 
 * @author wpalmer
 *
 */
public final class Settings {

	private Settings() {}
	
	/**
	 * Username that will be used to run the code
	 */
	private static final String SYSTEM_USER = "dpt";
	private static final String HDFS_USER = "wpalmer";
	/**
	 * Default home directory that contains binaries etc
	 */
	private static final String HOME_DIR = "/home/"+SYSTEM_USER+"/";
	/**
	 * Default home directory in Hadoop
	 */
	public static final String OUTPUT_DIR = "/user/"+HDFS_USER+"/";
	
	/**
	 * Directory in HDFS where files will be stored
	 * -> move this to WrapperSettings?
	 */
	public static final String TRACKER_STORAGE_DIR = OUTPUT_DIR+"/fileTracker/";

	/**
	 * Default number of maps
	 */
	//public static final int NUM_MAPS = 2;
	
	/**
	 * Version number
	 */
	public static final double VERSION = 0.8;
	
	//command line settings
	/**
	 * Path to the OpenJPEG image_to_j2k command
	 */
	//public static final String OPENJPEG_COMMAND = "/usr/bin/image_to_j2k";//use debian built in openjpeg
	public static final String OPENJPEG_COMMAND = HOME_DIR+"/local/bin/image_to_j2k";//use openjpeg 1.5.1
	/**
	 * Path to the OpenJPEG j2k_to_image command
	 */
	public static final String OPENJPEG_EXPAND = HOME_DIR+"/local/bin/j2k_to_image";//use openjpeg 1.5.1
	/**
	 * Any hard coded options to pass to image_to_j2k
	 */
	public static final String OPENJPEG_OPTIONS = "";
	/**
	 * Option to signify the input file is the next argument
	 */
	public static final String OPENJPEG_INPUT_FILE_OPT = "-i";
	/**
	 * Option to signify the output file is the next argument
	 */
	public static final String OPENJPEG_OUTPUT_FILE_OPT = "-o";

	//kakadu commands
	/**
	 * Path to the Kakadu kdu_compress command
	 */
	public static final String KAKADU_COMMAND = HOME_DIR+"/local/bin/kdu_compress";
	/**
	 * Any hard coded options to pass to KAKADU
	 */
	public static final String KAKADU_OPTIONS = "";
	/**
	 * Option to signify the input file is the next argument
	 */
	public static final String KAKADU_INPUT_FILE_OPT = "-i";
	/**
	 * Option to signify the output file is the next argument
	 */
	public static final String KAKADU_OUTPUT_FILE_OPT = "-o";

	//jpylyzer commands
	/**
	 * Path to the Jpylyzer binary
	 */
	public static final String JPYLYZER = HOME_DIR+"/local/bin/jpylyzer";
	/**
	 * Hard coded options to pass to Jpylyzer
	 */
	//the current version of the schematron checks fail if verbose is set due to not full xpath names
	//in schematron.  therefore disable verbose output
	public static final String JPYLYZER_OPTIONS = "";//"--verbose";
	/**
	 * Extension to add to extracted exif data
	 */
	public static final String JPYLYZER_EXT = ".jpylyzer.xml";
	
	//exiftool commands
	/**
	 * Path to Exiftool
	 */
	public static final String EXIFTOOL = "/usr/bin/exiftool";
	/**
	 * Hard coded options to pass to Exiftool
	 */
	public static final String EXIFTOOL_OPTIONS = "-X";	
	/**
	 * Extension to add to extracted exif data
	 */
	public static final String EXIFTOOL_EXT = ".exiftool.xml";

	//matchbox tools
	/**
	 * Path to the Matchbox extractfeatures tool
	 */
	public static final String MATCHBOX_EXTRACT_FEATURES = HOME_DIR+"/local/bin/extractfeatures";
	/**
	 * Path to the Matchbox compare tool
	 */
	public static final String MATCHBOX_COMPARE = HOME_DIR+"/local/bin/compare";
	/**
	 * Extension added to the output by Matchbox extractfeatures
	 */
	public static final String MATCHBOX_EXT_HISTOGRAM = ".ImageHistogram.feat.xml.gz";
	/**
	 * Extension added to the output by Matchbox extractfeatures
	 */
	public static final String MATCHBOX_EXT_METADATA = ".ImageMetadata.feat.xml.gz";
	/**
	 * Extension added to the output by Matchbox extractfeatures
	 */
	public static final String MATCHBOX_EXT_PROFILE = ".ImageProfile.feat.xml.gz";
	/**
	 * Extension added to the output by Matchbox extractfeatures
	 */
	public static final String MATCHBOX_EXT_SIFTCOMPARISON= ".SIFTComparison.feat.xml.gz";
	/**
	 * Extension to use for outputs from Matchbox compare for sift comparison
	 */
	public static final String MATCHBOX_COMP_SIFT_EXT = ".siftcomp.xml";
	/**
	 * Extension to use for outputs from Matchbox compare for profile comparison
	 */
	public static final String MATCHBOX_COMP_PROFILE_EXT = ".profilecomp.xml";
	/**
	 * Threshold to use for a matchbox match 
	 */
	public static final double MATCHBOX_THRESHOLD = 0.9;	

	//imagemagick settings
	/**
	 * Extension to add to result file for imagemagick psnr test
	 */
	public static final String IMAGEMAGICK_PSNR_EXT = ".psnr";
	/**
	 * Command to run imagemagick PSNR test
	 */
	public static final String IMAGEMAGICK_PSNR = "/usr/bin/compare -metric PSNR";
	/**
	 * Default PSNR threshold for a successful match
	 */
	public static final double PSNR_THRESHOLD = 48;	
	/**
	 * tifftopnm command for checking tiff data is readable
	 */
	public static final String TIFFTOPNM = "/usr/bin/tifftopnm";
	
	/**
	 * Jar file containing Dissimilar
	 */
	public static final String DISSIMILAR_JAR = "/home/"+SYSTEM_USER+"/dissimilar-0.3-SNAPSHOT-jar-with-dependencies.jar";
	/**
	 * Extension for Dissimilar outputs
	 */
	public static final String DISSIMILAR_EXT = ".diss";
	
	//taverna command line settings
	/**
	 * Shell required to execute Taverna command line shell script
	 */
	public static final String TAVERNA_SHELL = "/bin/sh";
	/**
	 * Path to the Taverna command line binary
	 */
	public static final String TAVERNA_COMMAND = HOME_DIR+"/taverna-commandline-2.4.0/executeworkflow.sh";
	/**
	 * Hard coded Taverna command line options
	 */
	public static final String TAVERNA_OPTIONS = "-inmemory";
	
	//taverna server settings
	/**
	 * URL of Taverna Server installation
	 */
	public static final String TAVERNA_SERVER = "http://127.0.0.1:8080/tavernaserver/rest/runs/";
	/**
	 * Username to access Taverna Server
	 */
	public static final String TAVERNA_SERVER_USER = "taverna";
	/**
	 * Password to access Taverna Server
	 */
	public static final String TAVERNA_SERVER_PASS = "taverna";
	
	//shared Taverna settings
	/**
	 * Default Taverna workflow - this is a resource in the jar file
	 */
	public static final String TAVERNA_WORKFLOW = "HadoopUsingTaverna.t2flow";
	//these are the port names as defined in the workflow file
	/**
	 * Default Taverna workflow input port name
	 */
	public static final String TAVERNA_WORKFLOW_INPUTFILEPORT = "inputOriginalName";
	//output ports in the workflow - the case *must* match that in the workflow
	/**
	 * Output ports from the Taverna workflow 
	 */
	public static final String[] TAVERNA_WORKFLOW_OUTPUTPORTS = { "zip/1/1" };
	
	//settings for standalone class runs (i.e. tests) in each class
	/**
	 * A test input file for standalone job class runs
	 */
	public static final String STANDALONE_TEST_INPUT = HOME_DIR+"/jpeg2000/test2.tif";
	/**
	 * A test output file for standalone job class runs
	 */
	public static final String STANDALONE_TEST_OUTPUT = "/tmp/test-1.jp2";
	
	//general settings
	/**
	 * Default Hadoop job name
	 */
	public static final String JOB_NAME = "Chutney-";
	/**
	 * Location of the temporary directory to use for processing
	 */
	public static final String TMP_DIR = "/tmp/hadooptmp-6/";
	/**
	 * Default buffer size to use when copying file data
	 */
	public static final int BUFSIZE = 32768; //32k io buffer
	/**
	 * Types of job that can be run, a new entry should be added when using a new HadoopJob 
	 * implementation
	 */
	@SuppressWarnings("javadoc")
	public static enum JobType { CommandLineJob, TavernaCommandLine, TavernaServerJob, XMLCommandLineJob, XMLWorkflowReport };
	/**
	 * Setting key used to pass JobType enum to the Mappers 
	 */
	public static final String JOBTYPE_CONF_SETTING = "eu.scape_project.tb.tavernahadoopwrapper.jobtype";
	/**
	 * XML code for XMLCommandLineJob
	 */
	public static final String XMLCODE_CONF_SETTING = "eu.scape_project.tb.tavernahadoopwrapper.xmlcode";
	/**
	 * HDFS output path for files
	 */
	public static final String OUTPUTPATH_CONF_SETTING = "eu.scape_project.tb.tavernahadoopwrapper.outputpath";
	/**
	 * Paths with shared libraries to add to environment before running executables
	 */
	public static final String LIBRARY_PATH="LD_LIBRARY_PATH="+HOME_DIR+"/local/lib/";
	/**
	 * Jpeg 2000 profile to use (make sure this is stored in jar)
	 */
	//public static final String J2K_PROFILE = "test-data/bl_profile.xml";
	/**
	 * The regexp for a hash
	 */
	public static final String PATTERN_HASH = "[a-fA-F0-9]{32}";
	/**
	 * String replacement for input files in xml code
	 */
	public static final String XML_INPUT_REPLACEMENT = "%input%";
	/**
	 * ActiveMQ server URI
	 */
	public static final String ACTIVEMQ_ADDRESS = "tcp://127.0.0.1:61616/";
	/**
	 * The NULL device
	 */
	public static final String NULL_DEVICE = "/dev/null";

	/**
	 * Schematron file to use with jp2check library
	 */
	public static final String SCHEMATRON = "openjpeg-schematron.sch";
	/**
	 * Compiled Schematron file to use with Taverna workflows
	 */
	public static final String SCHEMATRON_COMPILED = "openjpeg-schematron.sch.xsl";
	
}
