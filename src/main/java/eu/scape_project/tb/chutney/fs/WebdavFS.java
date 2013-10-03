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
import uk.bl.dpt.fclib.WebdavDataConnector;

/**
 * Implement a Webdav ChutneyFS file system
 * @author wpalmer
 */
public class WebdavFS implements ChutneyFS {

	private String gWebdavLoc = null;
	private String gDir = null;
	
	@Override
	public File getFile(String pInput, File pTempDir) {
		gWebdavLoc = pInput.substring(WebdavDataConnector.DC_URI.length());
		//put the files back in to the same directory structure
		//however, MKCOL is expensive and we don't do the same for other
		//storage types.  Therefore just put them in to the default directory
		//		gDir = gWebdavLoc.substring(0, gWebdavLoc.lastIndexOf("/"));
		gDir = "";
		File ret = WebdavDataConnector.recoverFile(gWebdavLoc, pTempDir.getAbsolutePath());
		return ret;
	}

	@Override
	public String putFile(boolean pSuccess, File pFrom, String pTo, String pDatastream,
			String pMessage, String pMimetype, boolean pOverwrite) {
		WebdavDataConnector.postFile(pFrom, gDir, pOverwrite);
		return gDir+pFrom.getName();
	}

	@Override
	public String getType() {
		return "WebdavDataConnector";
	}

	@Override
	public void saveLogFile(File pLogFile) {
		WebdavDataConnector.postFile(pLogFile, gDir, false);
	}

}
