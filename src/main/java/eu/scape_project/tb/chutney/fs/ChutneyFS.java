/*
 * Copyright 2013 The SCAPE Project Consortium
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

package eu.scape_project.tb.chutney.fs;

import java.io.File;

/**
 * A generic interface for file system access
 * @author wpalmer
 */
public interface ChutneyFS {

	/**
	 * Get a file from this file system
	 * @param pInput name of file to retrieve
	 * @param pTempDir local directory to put the file in to
	 * @return a File object for the newly copied file
	 */
	public File getFile(String pInput, File pTempDir);
	
	/**
	 * Put a file in to the file system
	 * @param pSuccess whether or not the workflow was successful (affects where the file is stored)
	 * @param pFrom File object to copy from
	 * @param pTo filename to copy to
	 * @param pDatastream datastream (for FedoraFS only)
	 * @param pMessage message (for FedoraFS only)
	 * @param pMimetype mimetype (for FedoraFS only)
	 * @param pOverwrite whether or not to overwrite an existing file or not
	 * @return a string containing information about the resulting location of the file
	 */
	public String putFile(boolean pSuccess, File pFrom, String pTo, String pDatastream, String pMessage, String pMimetype, boolean pOverwrite);
	
	/**
	 * Get the type of file system that is in use
	 * @return string containing the type of filesystem
	 */
	public String getType();
	
	/**
	 * Save a log file - note that this may go to a different file system, depending on the main file system type (cf. FedoraFS)
	 * @param pLogFile log file to save
	 */
	public void saveLogFile(File pLogFile);
	
}
