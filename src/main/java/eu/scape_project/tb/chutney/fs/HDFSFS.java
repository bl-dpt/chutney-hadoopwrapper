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

import eu.scape_project.tb.chutney.Tools;

/**
 * Implement a HDFS ChutneyFS file system
 * @author wpalmer
 *
 */
public class HDFSFS implements ChutneyFS {

	private FileSystem gFs = null;
	private String gHdfsOutputDir = null;
	
	/**
	 * Initialise HDFSFS
	 * @param pFS HDFS file system 
	 * @param pHdfsOutputDir HDFS output directory
	 */
	public HDFSFS(FileSystem pFS, String pHdfsOutputDir) {
		gFs = pFS;
		gHdfsOutputDir = pHdfsOutputDir;
	}
	
	@Override
	public File getFile(String pInput, File pTempDir) {
		try {
			return Tools.copyInputToLocalTemp(pTempDir,gFs,pInput);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String putFile(boolean pSuccess, File pFrom, String pTo, String pDatastream,
			String pMessage, String pMimetype, boolean pOverwrite) {
		try {
			gFs.copyFromLocalFile(new Path(pFrom.getAbsolutePath()), new Path(gHdfsOutputDir+pTo));
			return gHdfsOutputDir+pTo;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String getType() {
		return "HDFS";
	}

	@Override
	public void saveLogFile(File pLogFile) {
		try {
			gFs.copyFromLocalFile(new Path(pLogFile.getAbsolutePath()), new Path(gHdfsOutputDir+pLogFile.getName()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
