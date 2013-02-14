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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class stores, tracks and provides access to files in HDFS and locally.
 * This class will be instantiated several times and needs to regenerate
 * state information when constructed.  We rely on the fact that the files
 * should exist by the time this class is called.  i.e. previous steps which
 * are dependencies in a Taverna workflow are completed.
 * 
 * @author wpalmer
 *
 */
public class FileTracker {

	/**
	 * Name of file which contains HDFS path to original file (key file)
	 * -> move this to WrapperSettings?
	 */
	private final String KEYFILEFILE = "keyfile.txt";
	
	/**
	 * Directory in HDFS where files will be stored
	 * -> move this to WrapperSettings?
	 */
	private String hdfsStorageDir = WrapperSettings.TRACKER_STORAGE_DIR;
	/**
	 * The local temporary directory to copy files to
	 */
	private String localTempDir = "";
	/**
	 * The key file for the workflow - this will be used as the basis for the
	 * output directory.  i.e. input filename. 
	 */
	private String keyFile = "";
	/**
	 * Hash code of the keyfile
	 */
	private String hashCode = "";
	/**
	 * List of files relating to the current keyfile that are in HDFS
	 */
	private List<String> hdfsFiles = null;
	/**
	 * HDFS FileSystem reference
	 */
	private FileSystem fileSystem;

	/**
	 * If this is set, do nothing.  This allows use of the tracker class without it
	 * being substantively created, by protecting access to methods. 
	 */
	private boolean doNothing = false;
	
	/**
	 * Do nothing in this instance.  Need this to initialize a class so Eclipse stops 
	 * complaining about it not being initialised, despite access being surrounded by checks.
	 */
	public FileTracker() {
		doNothing = true;
	}
	
	/**
	 * Constructor to use when first creating a FileTracker - note that if this is
	 * called more than once with the same file/hash pair it will re-use the existing
	 * FileTracker data
	 * @param tFileSystem HDFS file system for current job
	 * @param tKeyFile name of the keyfile for the FileTracker
	 * @param tHash hash of the keyfile
	 * @param keyFileLoc the full HDFS path to the key file 
	 */
	public FileTracker(FileSystem tFileSystem, String tKeyFile, String tHash, String keyFileLoc) {

		hashCode = tHash;
		fileSystem = tFileSystem;
		
		StringTokenizer tok = new StringTokenizer(tKeyFile, ".");
		//this makes sure we use the correct name between jobs
		//this will need to be more robust (use md5 of file?)
		//what if it's operating on later files and not using the original file?
		//then use this filename to begin with as all generated files should start with it
		if(tok.countTokens()<2) {
			keyFile = tKeyFile;
		} else {
			keyFile = tok.nextToken() + "." + tok.nextToken();
		}
		//this goes here as keyfile must be set
		localTempDir = makeLocalTempDir();

		hdfsStorageDir += keyFile + "-" + tHash + ".dir/";
		//make the directories if they don't exist
		try {
			if(!fileSystem.exists(new Path(hdfsStorageDir)))
				fileSystem.mkdirs(new Path(hdfsStorageDir));
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		//set this to the full hdfs path name
		try {
			hdfsStorageDir = fileSystem.getFileStatus(new Path(hdfsStorageDir)).getPath().toString()+"/";
		} catch (IOException e1) {
			//this should not fail
		} 
		
		if(keyFile.equals(tKeyFile)) {
			//as the keyfile is being passed as input to this tracker
			//store the location of the keyfile
			//note: this is not safe for concurrent use
			//however, the output file should only be overwritten with the
			//same information
			try {
				FSDataOutputStream output = fileSystem.create(new Path(hdfsStorageDir+KEYFILEFILE));
				//do it this way as writing as UTF creates unicode<->ascii issues
				output.write(keyFileLoc.getBytes());
				output.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		hdfsFiles = new LinkedList<String>();
		//add the existing files to the list
		try {
			generateFileList(new Path(hdfsStorageDir));
		} catch (IOException e) {
			
		}

	}

	/**
	 * Return the full path to the key file
	 * @return full path to the key file (in HDFS)
	 */
	private String getKeyFileLocation() {

		if(doNothing) return null;
		
		try {
			FSDataInputStream input = fileSystem.open(new Path(hdfsStorageDir+KEYFILEFILE));

			BufferedReader red = new BufferedReader(new InputStreamReader(input));
			String output = red.readLine();
			output = output.toString().trim();
			input.close();
			return output;
			
		} catch (IOException e) {
			return null;
		}
		
	}
	
	/**
	 * Return the hash code of the key file
	 * @return hash code of the key file
	 */
	public String getHash() {
		if(doNothing) return null;
		return hashCode;
	}

	/**
	 * Return the key file
	 * @return the name of the key file
	 */
	public String getKeyFile() {
		
		if(doNothing) return null;
		
		return keyFile;
	}

	/**
	 * Get a list of all the files in this tracker
	 * @return list of all the files in this tracker
	 */
	public List<String> getFileList() {

		if(doNothing) return null;

		return hdfsFiles;
	}
	
	/**
	 * Constructor when we have a hash code to use
	 * @param tFileSystem FileSystem reference
	 * @param tHash The hash code for the original input file (passed through jobs)
	 */
	public FileTracker(FileSystem tFileSystem, String tHash) {

		fileSystem = tFileSystem;

		//if this is an md5sum locate the keyfile and populate the list
		if(tHash.matches(WrapperSettings.PATTERN_HASH)) {
			
			//i.e. we were passed a hash so find the keyFile
			try {
				FileStatus[] fstatus = fileSystem.globStatus(new Path(hdfsStorageDir+"*-"+tHash+".dir*"));
				hdfsStorageDir = fstatus[0].getPath().toString();
				Pattern p = Pattern.compile("(.*)/([^/]+)-("+WrapperSettings.PATTERN_HASH+")(.dir.*)");
				Matcher m = p.matcher(fstatus[0].getPath().toString());
				m.find();
				//assume m.matches
				//m.group(0) is full pattern match, then (1)(2)(3)... for the above pattern
				keyFile = m.group(2);
				hashCode = tHash;
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		} else {

			//we shouldn't get here
			//FIXME: this is potentially dangerous if multiple files have the same
			//name - plus this should never be called as the hash should be there...?
			
		}

		//this goes here as hash and keyfile must be set
		localTempDir = makeLocalTempDir();
		
		//set this to the full hdfs path name
		try {
			hdfsStorageDir = fileSystem.getFileStatus(new Path(hdfsStorageDir)).getPath().toString()+"/";
		} catch (IOException e1) {
		} 

		hdfsFiles = new LinkedList<String>();
		//add the existing files to the list
		try {
			generateFileList(new Path(hdfsStorageDir));
		} catch (IOException e) {
			
		}
		
	}
	
	/**
	 * Populates the list of files in HDFS in the class  
	 * @param storageDir Directory in HDFS for the keyfile
	 * @throws IOException
	 */
	private void generateFileList(Path storageDir) throws IOException {
	
		//iterate through the files in the storage directory
		FileStatus[] fileStatus = fileSystem.listStatus(storageDir);
		if(null == fileStatus) return;
		for(FileStatus fs:fileStatus) {
			if(fs.isDir()) {
				generateFileList(fs.getPath());
			} else { //i.e. not a directory
				//add the file to the list if it is not the key file
				if(!fs.getPath().getName().equals(KEYFILEFILE)) {
					hdfsFiles.add(fs.getPath().toString().substring(hdfsStorageDir.toString().length()));
				}
			}
		}
		return;
	}
	
	/**
	 * Writes the list of files in this class to a text file
	 * @param localFile File to write the list of files to
	 */
	public void writeList(String localFile) {
		if(doNothing) return;
		BufferedWriter out;
		try {
			out = new BufferedWriter(new FileWriter(localFile));
			out.write("hdfsStorageDir: "+hdfsStorageDir);out.newLine(); 
			out.write("keyFile: "+keyFile);out.newLine(); 
			out.write("hashCode: "+hashCode);out.newLine(); 
			for(String file:hdfsFiles) {
				out.write(file);
				out.newLine();
			}
			out.close();
		} catch (IOException e) {
		}
	}
	
	/**
	 * Stores a file in HDFS at an appropriate location
	 * @param localFilename Filename incluuding full local path
	 * @param hdfsFilename Filename to use in hdfs
	 */
	public void storeFile(String localFilename, String hdfsFilename) {
		if(doNothing) return;
		try {
			fileSystem.copyFromLocalFile(new Path(localFilename), new Path(hdfsStorageDir+hdfsFilename));
			//push the stored file via JMS
			JMSComms.sendMessage(getHash(), "FILE:"+hdfsStorageDir+hdfsFilename);
		} catch (IOException e) {
		}
		hdfsFiles.add(hdfsFilename);
	}
	
	/**
	 * Does the file exist in the context of the current keyfile
	 * @param fileName file to check
	 * @return true if it exists, false if not
	 */
	public boolean exists(String fileName) {
		if(doNothing) return false;
		try {
		
			if(fileName.equals(keyFile)) {
//				if(fileSystem.exists(new Path(hdfsStorageDir+KEYFILEFILE))) {
					System.out.println("keyFile must exist");
					//i.e. the keyfile exists and must have the hdfs location of the
					//key file in it so say key file exists
					return true;
	//			}
			} else {

			}
			
			if(!fileName.toLowerCase().startsWith("hdfs")) {
				return fileSystem.exists(new Path(hdfsStorageDir+fileName));
			}
			return fileSystem.exists(new Path(fileName));
		} catch (IOException e) {
		}
		return false;
	}
	
	/**
	 * If the file exists return the full path to the file
	 * @param fileName file to get a full reference for (short name only)
	 * @return full pathname to the file
	 */
	public String getHDFSFilePath(String fileName) {
		if(doNothing) return null;
		if(fileName.equals(keyFile)) {
			return getKeyFileLocation();
		} 
		if(exists(fileName)) {
			if(!fileName.toLowerCase().startsWith("hdfs"))
				return hdfsStorageDir+fileName;
			return fileName;
		}
		return null;
	}
	
	/**
	 * Retrieves a file from HDFS, to a local directory, if required.  
	 * @param fileName file name of file to make local
	 */
	public void makeFileLocal(String fileName) {
		
		if(doNothing) return;
		
		//sanity check;
		if(!exists(fileName)) return;
		
		File localTempFile = new File(localTempDir+fileName);
		if(localTempFile.exists()) {
			//do nothing - assume file is ok
			//could check the md5 maybe?			
		} else {
			//check the directory exists
			File localTempDir = new File(getLocalTempDir());
			if(!localTempDir.exists()) {
				makeLocalTempDir();
			}
			//copy the file from hdfs to local storage
			try {
				Tools.copyInputToLocalTemp(localTempDir,fileSystem,getHDFSFilePath(fileName));
			} catch (IOException e) {
				return;
			}
		}
		
		return;
	}
	
	/**
	 * Make sure the local temp dir exists (create if necessary) 
	 * @return path name of local temporary directory
	 */
	private String makeLocalTempDir() {
		
		if(doNothing) return null;

		File localTempDir = new File(WrapperSettings.TMP_DIR+getKeyFile()+"-"+getHash()+".dir/");
		localTempDir.mkdirs();
		return localTempDir.toString()+"/";
		
	}
	
	/**
	 * @return path name of local temporary directory
	 */
	public String getLocalTempDir() {

		if(doNothing) return null;

		return localTempDir;
	}

	/**
	 * Delete all files tracked by this FileTracker
	 */
	public void deleteAllFiles() {

		if(doNothing) return;

		//delete the local files
		Tools.deleteDirectory(new File(localTempDir));
		
		//delete the files in hdfs
		try {
			fileSystem.delete(new Path(hdfsStorageDir), true /*delete recursively */);
		} catch (IOException e) {
		}

		//once we've done this, don't allow anything else to happen
		doNothing = true;
	}
	
}
