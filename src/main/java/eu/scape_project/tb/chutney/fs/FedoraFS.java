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
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import uk.bl.dpt.fclib.FedoraDataConnector;

/**
 * Implement a Fedora Commons file system (note log files go to HDFS)
 * @author wpalmer
 *
 */
public class FedoraFS implements ChutneyFS {

	private String gFedoraPid;
	private String gFedoraDatastream;
	//private String fedoraFilename;
	private FileSystem gFs = null;
	private String gHdfsOutputDir = null;
	
	/**
	 * Initialise FedoraFS
	 * @param pFS HDFS file system (for log files)
	 * @param pHdfsOutputDir HDFS output directory (for log files)
	 */
	public FedoraFS(FileSystem pFS, String pHdfsOutputDir) {
		//to save log files
		gFs = pFS;
		gHdfsOutputDir = pHdfsOutputDir;
	}
	
	@Override
	public File getFile(String pInput, File pTempDir) {
		gFedoraPid = pInput.substring(FedoraDataConnector.DC_URI.length()).split("/")[0];
		gFedoraDatastream = pInput.substring(FedoraDataConnector.DC_URI.length()).split("/")[1];
		//recover the file and pass the appropriate filename to the job class
		File ret = FedoraDataConnector.recoverDatastream(gFedoraPid, gFedoraDatastream, pTempDir.getAbsolutePath());
		//fedoraFilename = ret.getName();
		return ret;
	}

	@Override
	public String putFile(boolean pSuccess, File pFrom, String pTo, String pDatastream, String pMessage, String pMimetype, boolean pOverwrite) {
		if(pSuccess) {
			FedoraDataConnector.postDatastream(gFedoraPid, pDatastream, pFrom, pMessage, pMimetype);
			return FedoraDataConnector.DC_URI+gFedoraPid+":"+pDatastream;
		} else {
			//don't put an unsuccessful result in to the repository!
			Path dest = new Path(gHdfsOutputDir+pTo);
			try {
				gFs.copyFromLocalFile(new Path(pFrom.getAbsolutePath()), dest);
				return dest.toString();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public String getType() {
		return "FedoraDataConnector";
	}

	/**
	 * Saves log file to HDFS
	 */
	@Override
	public void saveLogFile(File pLogFile) {
		try {
			gFs.copyFromLocalFile(new Path(pLogFile.getAbsolutePath()), new Path(gHdfsOutputDir+pLogFile.getName()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
}
