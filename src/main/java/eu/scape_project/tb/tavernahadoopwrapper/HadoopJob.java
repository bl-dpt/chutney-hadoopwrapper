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

import java.io.IOException;

/**
 * An interface for jobs to be run using the HadoopTavernaWrapper code
 * 
 * @author wpalmer
 *
 */

public interface HadoopJob {

	/**
	 * Setup the job
	 * @throws IOException file access error
	 */
	public void setup() throws IOException;
	/**
	 * Run the job
	 * @throws IOException file access error
	 */
	public void run() throws IOException;
	/**
	 * Clean up after job has run
	 * @throws IOException file access error
	 */
	public void cleanup() throws IOException;
	/**
	 * Was the job successful?
	 * @return whether the job was successful or not
	 */
	public boolean wasSuccessful();
	
	/**
	 * Get the local log filename
	 * @return full path to the log file
	 */
	public String getLogFilename();
	
	/**
	 * Get a list of files created by this job - *must* include full pathnames
	 * @return full path names to the output files
	 */
	public String[] getOutputFiles();
	
	/**
	 * Get a list of files required by this job - *must* include full pathnames
	 * @return full path names to the input files
	 */
	public String[] getInputFiles();
	
	
}
