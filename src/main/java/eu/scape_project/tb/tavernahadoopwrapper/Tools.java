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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * This class collects useful methods to be shared between job classes
 * @author wpalmer
 *
 */
public class Tools {

	/**
	 * Generates a checksum for a file 
	 * @param inFile file to checksum
	 * @return A String with the format MD5:XXXXXX or SHA1:XXXXXX
	 * @throws IOException file access error
	 */
	public static String generateChecksum(String inFile) throws IOException {

		if(!new File(inFile).exists()) throw new IOException("File not found: "+inFile);
		
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			/*
			try {
					md = MessageDigest.getInstance("SHA1");
				} catch (NoSuchAlgorithmException e1) { 
					//don't generate a checksum then
					return null;
				}*/
			return null;
		}
		
		FileInputStream input;
		try {
			input = new FileInputStream(inFile);
			byte[] readBuffer = new byte[WrapperSettings.BUFSIZE];
			int bytesRead = 0;
			while(input.available()>0) {
				bytesRead = input.read(readBuffer);
				md.update(readBuffer, 0, bytesRead);
			}
			input.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String hash = "";
		for(byte b : md.digest()) hash+=String.format("%02x", b);
		
		return md.getAlgorithm()+":"+hash;
		
	}
	
	/**
	 * Generates a checksum for a file 
	 * @param inFile file to checksum
	 * @return A String containing only the checksum
	 * @throws IOException file access error
	 */
	public static String generateChecksumOnly(String inFile) throws IOException {

		if(!new File(inFile).exists()) throw new IOException("File not found: "+inFile);
		
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			/*
			try {
					md = MessageDigest.getInstance("SHA1");
				} catch (NoSuchAlgorithmException e1) { 
					//don't generate a checksum then
					return null;
				}*/
			return null;
		}
		
		FileInputStream input;
		try {
			input = new FileInputStream(inFile);
			byte[] readBuffer = new byte[WrapperSettings.BUFSIZE];
			int bytesRead = 0;
			while(input.available()>0) {
				bytesRead = input.read(readBuffer);
				md.update(readBuffer, 0, bytesRead);
			}
			input.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String hash = "";
		for(byte b : md.digest()) hash+=String.format("%02x", b);
		
		return hash;
		
	}
	
	/**
	 * Generates a checksum for a file and writes it to the log file
	 * @param inFile file that has been checksummed
	 * @param hash file checksum
	 * @param logFile log file to write to
	 * @throws IOException file access error
	 */
	public static void writeChecksumToLog(String inFile, String hash, String logFile) throws IOException {

		BufferedWriter outputFile = new BufferedWriter(new FileWriter(logFile,true));

		outputFile.write("*** "+hash.split(":")[0]+" checksum for \""+(new File(inFile).getName()+"\": "+hash.split(":")[1]));
		outputFile.newLine();
		outputFile.close();
		
	}
	
	/**
	 * Bulk copy data from one buffer to another
	 * @param inData buffer to read from
	 * @param outputFile buffer to read to
	 * @throws IOException file access error
	 */
	public static void writeBufferToFile(BufferedReader inData, BufferedWriter outputFile) throws IOException {
		char[] readBuffer = new char[WrapperSettings.BUFSIZE];
		int bytesRead = 0;
		while(inData.ready()) {
			bytesRead = inData.read(readBuffer);
			outputFile.write(readBuffer, 0, bytesRead);
		}
	}
	
	/**
	 * Append a buffer to a file (used for stdout/stderr in to log file)
	 * @param name name of the buffer
	 * @param inData buffer to read from 
	 * @param outputFile buffer to write to
	 * @throws IOException file access error
	 */
	public static void appendBufferToFile(String name, BufferedReader inData, BufferedWriter outputFile) throws IOException {
		//append buffer to log file
		outputFile.write("--------------------------------------\n");
		outputFile.write(name+":\n");
		writeBufferToFile(inData, outputFile);
		outputFile.write("--------------------------------------\n");
	}
	
	/**
	 * Append information about the recently executed process to a log file.
	 * @param exitCode exit code of recently executed program
	 * @param commandLine command line of recently executed program
	 * @param logFile log file to write to 
	 * @throws IOException file access error
	 */
	public static void appendProcessInfoToLog(int exitCode, List<String> commandLine, BufferedWriter logFile) throws IOException {
		logFile.write("--------------------------------------\n");		
		logFile.write(commandLine.toString());
		logFile.write("\n");
		logFile.write("Exitcode: "+exitCode+"\n");
		logFile.write("--------------------------------------\n");		
		
	}
	
	/**
	 * Creates a new temporary directory 
	 * @return File object for new directory
	 * @throws IOException file access error
	 */
	public static File newTempDir() throws IOException {
		//create a temporary local output file name for use with the local tool in TMPDIR
		new File(WrapperSettings.TMP_DIR).mkdirs();
		File localOutputTempDir = File.createTempFile(WrapperSettings.JOB_NAME,"",
				new File(WrapperSettings.TMP_DIR));
		//change this to a directory and put all the files in there
		localOutputTempDir.delete();
		localOutputTempDir.mkdirs();
		localOutputTempDir.setReadable(true, false);
		localOutputTempDir.setWritable(true, false);//need this so output can be saved
		return localOutputTempDir;
	}

	/**
	 * Recursively delete a local directory
	 * @param dir directory to delete
	 * @return success
	 */
	public static boolean deleteDirectory(File dir) {
		boolean ret = true;
		for(File f:dir.listFiles()) {
			if(f.isDirectory()) ret&=deleteDirectory(f);
			ret&=f.delete();
		}
		ret&=dir.delete();
		return ret;
	}
	
	
	/**
	 * Copy an input file to a local temporary file and return the new local filename
	 * @param tmpDir temporary directory to copy files to
	 * @param fs The HDFS filesystem
	 * @param inputFile The URL/name of the input file
	 * @return A File instance for the new local temporary file
	 * @throws IOException file access error
	 */
	public static File copyInputToLocalTemp(File tmpDir, FileSystem fs, String inputFile) throws IOException {
		
		//put the file in the new temporary directory
		//we use lastindexof as path may start hdfs:// and File doesn't understand
		System.out.println(tmpDir+"<-"+inputFile);
		String tempFile = tmpDir.getAbsolutePath();
		if(inputFile.contains("/")) {
			tempFile+=(inputFile.substring(inputFile.lastIndexOf("/")));
		} else {
			tempFile+="/"+inputFile;
		}
		File tempInputFile = new File(tempFile);

		//if this file has already been copied - skip
		//FIXME: this thinks that the file exists when it doesn't
		if(tempInputFile.exists()) return tempInputFile;
		
		//i.e. this file is a local file
		if(new File(inputFile).exists()) {
		//	System.out.println("copying from local fs");
			FileInputStream fis = new FileInputStream(inputFile);
			FileOutputStream fos = new FileOutputStream(tempInputFile);
			byte[] buffer = new byte[WrapperSettings.BUFSIZE];
			int bytesRead = 0;
			while(fis.available()>0) {
				bytesRead = fis.read(buffer);
				fos.write(buffer, 0, bytesRead);
			}
			fis.close();
			fos.close();
			return tempInputFile;
		}
		//this file is in HDFS
		if(fs.exists(new Path(inputFile))) {
		//	System.out.println("copying from hdfs");
			fs.copyToLocalFile(new Path(inputFile), new Path(tempFile));
			tempInputFile = new File(tempFile);
			return tempInputFile;
		}
		//TODO: check for HTTP files etc

		System.out.println("file not found");
		return null;
	}
	
	/**
	 * Convenience method to zip the generated files together (no compression)
	 * @param success whether workflow was successful or not
	 * @param checksums checksums for all files to be zipped
	 * @param generatedFiles list of files to be zipped
	 * @param zipFile output zip file
	 * @param tempDir local temporary directory that contains files to zip 
	 * @throws IOException file access error
	 */
	public static void zipGeneratedFiles(boolean success, HashMap<String, String> checksums, 
			List<String> generatedFiles, String zipFile, String tempDir) throws IOException {
		
		System.out.println("zipGeneratedFiles("+zipFile+" ...)");
		
		ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(zipFile));

		int compression = ZipEntry.STORED;
		
		//add an empty file indicating success or failure
		ZipEntry status;
		if(success) status = new ZipEntry("SUCCESS");
		else status = new ZipEntry("FAILURE");
		status.setSize(0);
		status.setTime(0);
		status.setMethod(compression);
		status.setCrc(0);
		zip.putNextEntry(status);
		zip.closeEntry();
		
		//generate a manifest file and write it to the zip as the first entry
		ZipEntry manifest = new ZipEntry("manifest-md5.txt");
		manifest.setTime(0);
		manifest.setMethod(compression);
		CRC32 crc = new CRC32();
		long size = 0;
		for(String file : checksums.keySet()) {
			//nasty hack
			if(file.endsWith(".report.xml")) continue;
			
			//String fn = new File(file).getName();
			//only add the file if it exists!
			if(new File(tempDir+file).exists()) {
				String out = checksums.get(file).split(":")[1]+"  data/"+file+"\n";
				size+=out.getBytes().length;
				crc.update(out.getBytes());
			}
		}
		manifest.setCrc(crc.getValue());
		manifest.setSize(size);
		zip.putNextEntry(manifest);
		for(String file : checksums.keySet()) {
			//nasty hack
			if(file.endsWith(".report.xml")) continue;

			//String fn = new File(file).getName();
			
			//THIS MUST MATCH THE ABOVE STRING EXACTLY!
			//only add the file if it exists!
			if(new File(tempDir+file).exists()) {
				String out = checksums.get(file).split(":")[1]+"  data/"+file+"\n";
				zip.write(out.getBytes());
			}
		}
		zip.closeEntry();
		
		//copy all the files in
		for(String file : generatedFiles) {
			//add file to zip
			File input = new File(tempDir+file);
			
			//file does not exist - obvious error condition but continue anyhow
			if(!input.exists()) continue;
			
			FileInputStream inData = new FileInputStream(input);
			ZipEntry entry;
			//hack to shorten report and log file names
			if(file.endsWith(".report.xml")) {
				entry = new ZipEntry("report.xml");
			/* } else if(file.endsWith(".log")) {
				entry = new ZipEntry("log.txt"); */
			} else {
				String fn = new File(file).getName();

				entry = new ZipEntry("data/"+fn);
			}
			
			System.out.println(entry.getName());
			
			entry.setSize(input.length());
			entry.setTime(input.lastModified());
			entry.setMethod(compression);

			//there has to be a better way to generate the CRC than this!
			crc = new CRC32();
			byte[] readBuffer = new byte[WrapperSettings.BUFSIZE];
			int bytesRead = 0;
			while(inData.available()>0) {
				bytesRead = inData.read(readBuffer);
				crc.update(readBuffer, 0, bytesRead);
			}
			entry.setCrc(crc.getValue());
			//reset inData, again there has to be a better way
			inData.close();
			inData = new FileInputStream(input);
			
			zip.putNextEntry(entry);			

			readBuffer = new byte[WrapperSettings.BUFSIZE];
			bytesRead = 0;
			while(inData.available()>0) {
				bytesRead = inData.read(readBuffer);
				zip.write(readBuffer, 0, bytesRead);
			}
			
			inData.close();
			zip.closeEntry();
		}
		
		zip.close();
	}
	
	/**
	 * Recover the SSIM comparison value
	 * @param fileName file to retrieve ssim value from
	 * @return SSIM comparison value
	 */
	public static double getSSIMCompareVal(String fileName) {
		DocumentBuilder docB = null;
		Document doc = null;
		
		try {
			docB = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		} catch(ParserConfigurationException pce) {
			return 0;
		}
		try {
			doc = docB.parse(fileName);
		} catch(IOException ioe) {
			return 0;
		} catch(SAXException se) {
			return 0;
		}
		
		Node root = doc.getFirstChild();		
		XPath xpath = XPathFactory.newInstance().newXPath();
		String path = "/comparison/task/ssim";
		try {
			return new Double(xpath.evaluate(path, root));
		} catch (NumberFormatException e) {
			return 0;
		} catch (XPathExpressionException e) {
			return 0;
		}
	}
	
	/**
	 * Get the PSNR output from imagemagick
	 * @param file file to check
	 * @return PSNR value from file
	 */
	public static double getPSNRVal(String file) {
		double ret = 0;
		
		try {
			BufferedReader input = new BufferedReader(new FileReader(file));
			ret = new Double(input.readLine());
			input.close();
		} catch (FileNotFoundException e) {
		} catch (NumberFormatException e) {
		} catch (IOException e) {
		}
		
		return ret;
	}
	
	/**
	 * Fix up matchbox xml outputs - change absolute file reference to relative reference
	 * @param file matchbox xml file to fix
	 */
	public static void fixupMatchboxXML(String file) {
		
		try {
			BufferedReader input = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));		
			BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file+".new"))));

			//since the outputs are similar each time we can just look for the
			//line with the filename and perform a quick fixup
			
			while(input.ready()) {
				String line = input.readLine();
				if(line.trim().startsWith("<filename>")) {
					line = line.trim();
					Pattern p = Pattern.compile("(<filename>)(.*)(</filename>)");
					Matcher m = p.matcher(line);
					m.find();
					line = "<filename>"+(new File(m.group(2)).getName())+"</filename>";
				}
				output.write(line);
				output.newLine();
			}
			
			output.close();
			input.close();
			
			new File(file+".new").renameTo(new File(file));
			

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Test main method
	 * @param args command line arguments
	 */
	public static void main(String[] args) {
		fixupMatchboxXML(args[0]);
		
	}
	
}
