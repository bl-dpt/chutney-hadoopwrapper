Chutney v0.9
============

This project is (c) 2012, 2013 The SCAPE Project Consortium, see LICENSE.txt
www.scape-project.eu

Chutney consists of two main parts; a wrapper for Hadoop that sets up an environment to 
run java/command line workflows and a set of workflows that can be executed.  There is an interface 
for a HadoopJob class that is implemented by a series of classes.

The code has only been run on Debian Linux "Wheezy" and Ubuntu 12.04.1 LTS, both x64.

This code has successfully migrated 1TB of TIFFs to JP2 with the CommandLineJob and TavernaCommandLineJob workflows - using HDFS, Fedora and Webdav data sources/destinations.

The wrapper code takes the following arguments:
jobname: 	the name of the job to run, this is propogated through to Hadoop and is used as a basis 
			for output files in HDFS
inputlist:	a file in HDFS containing a list of input files, also in HDFS	
jobtype:	which of the built in job classes to execute
			CLJ: a class that executes a workflow direct from the Java code
			TCL: a class that executes a Taverna workflow using the Taverna command line tool
			TSJ: a class that executes a Taverna workflow using the Taverna server (may not currently work)
			XML: a class that executes a job according to an XML definition
			XWR: a class that reports on and finalises a series of XML jobs 
xmlcode:	for an XML job, the XML file containing the job definition
output:		directory in HDFS in which to store the output files
maps:		number of maps to use, recommend 2*#slaves 

The wrapper is responsible for retrieving files from HDFS, and storing output files back in to HDFS.
For XML defined jobs there is a JobTracker class that stores files according to the original input file's 
MD5 checksum.  This will currently only work when there is one original input file.  Files can be recovered 
via the JobTracker for use in later XML defined jobs.

There is also a JPEG2000 class that will generate appropriate command line arguments for OpenJPEG, Kakadu,
JJ2000 and JasPer (note: not guaranteed to be fully correct at the moment).

Requirements
============
The following tools are needed for the included workflows:
OpenJPEG (Kakadu is also an option)
Jpylyzer (v1.6.3) (compiled with pyinstaller)
Exiftool (Debian version)
Matchbox (from SCAPE GitHub https://github.com/openplanets/scape/tree/master/pc-qa-matchbox)
Imagemagick (from Debian/Ubuntu)

The version of Hadoop tested is 1.0.4 and CDH4

For Taverna command line the following is required:
Taverna command line (2.4)

For Taverna server the following is required:
Taverna server (2.4)

For XML defined jobs the following is required:
ActiveMQ (5.6, Debian version) (see JMSComms class)

Settings
========
All user-configurable settings to get the code up and running quickly are contained within the 
WrapperSettings class

Example Workflows
=================
The batch file for migration is in ./batch_workflow/
	Note: this script does not zip the files, or provide any feedback on success/failure

The Hadoop workflow can be executed directly from the jar (CLJ) on the command line
	Note: JP2 profile, matchbox tests and imagemagick tests are configured in the java code

For Hadoop->Taverna, the workflow is ./workflows/HadoopUsingTaverna.t2flow 
	Note: the workflow can be executed directly from the jar (TCL) on the command line

For Taverna->Hadoop, the workflow is ./workflows/TavernaUsingHadoop.t2flow
	Note: the workflow should be run in Taverna (only via workbench tested) on the machine
	where the "hadoop" binary can be found
	
These workflows expect the following setup:
	Input TIFF files in HDFS, full paths of each in a text file, also in HDFS (note: do not put a 
		trailing line in this file!)
	XML definitions from ./xmlcode/ in /home/will/VMSharedFolder/xmlcode/
	Jar file is /home/will/VMSharedFolder/TavernaHadoopWrapper.jar
	OpenJPEG, Jpylyzer and Matchbox are installed in /home/will/local/
	*changes to workflows/code will be required if these settings are not matched

Issues
======
An ActiveMQ queue is set up for each input file and not deleted at the end of the run, leaving an empty
queue on the server.  This needs to be fixed.


