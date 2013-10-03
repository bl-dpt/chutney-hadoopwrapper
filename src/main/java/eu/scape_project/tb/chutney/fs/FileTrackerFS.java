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

/*
package eu.scape_project.tb.chutney.fs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import eu.scape_project.tb.chutney.FileTracker;
import eu.scape_project.tb.chutney.Tools;

public class FileTrackerFS implements ChutneyFS {

	private FileTracker fileTracker = null;
	private FileSystem fs = null;
	
	public FileTrackerFS(FileTracker pFileTracker, FileSystem pFS) {
		fileTracker = pFileTracker;
		fs = pFS;
	}
	
	@Override
	public File getFile(String input, File tempDir) {
		fileTracker.makeFileLocal(input);//return Tools.copyInputToLocalTemp(tempDir,fs,input);
		return null;
	}

	@Override
	public String putFile(File from, String to, String datastream,
			String message, String mimetype, boolean overwrite) {
		fileTracker.storeFile(from.getAbsolutePath(), to);
		return fileTracker.getHDFSFilePath(to);
	}

	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return "FileTracker";
	}

	@Override
	public void saveLogFile(File logFile) {
		// TODO Auto-generated method stub
		fileTracker.storeFile(logFile.getAbsolutePath(), logFile.getName());
	}

}
*/