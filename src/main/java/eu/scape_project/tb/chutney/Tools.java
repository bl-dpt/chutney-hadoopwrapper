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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
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
	 * Gets an InputStream for a resource from a jar
	 * @param pClass Class reference
	 * @param pRes resource path in jar
	 * @return InputStream for resource
	 */
	public static InputStream getResource(Class<?> pClass, String pRes) {
		return pClass.getClassLoader().getResourceAsStream(pRes);
	}
	
	/**
	 * Generates a checksum for a file 
	 * @param pType type of checksum to generate (MD5/SHA1 etc)
	 * @param pInFile file to checksum
	 * @return A String with the format MD5:XXXXXX or SHA1:XXXXXX
	 * @throws IOException file access error
	 */
	public static String generateChecksum(String pType, String pInFile) throws IOException {

		if(!new File(pInFile).exists()) throw new IOException("File not found: "+pInFile);
		
		MessageDigest md;
		try {
			md = MessageDigest.getInstance(pType.toUpperCase());
		} catch (NoSuchAlgorithmException e) {
			return null;
		}
		
		FileInputStream input;
		try {
			input = new FileInputStream(pInFile);
			byte[] readBuffer = new byte[Settings.BUFSIZE];
			int bytesRead = 0;
			while(input.available()>0) {
				bytesRead = input.read(readBuffer);
				md.update(readBuffer, 0, bytesRead);
			}
			input.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String hash = "";
		for(byte b : md.digest()) hash+=String.format("%02x", b);
		return hash;
	}	
	
	/**
	 * Generates a checksum for a file 
	 * @param pInFile file to checksum
	 * @return A String with the format MD5:XXXXXX or SHA1:XXXXXX
	 * @throws IOException file access error
	 */
	public static String generateChecksum(String pInFile) throws IOException {

		String type = "MD5";
		return type+":"+generateChecksum(type, pInFile);
		
	}
	
	/**
	 * Generates a checksum for a file 
	 * @param pInFile file to checksum
	 * @return A String containing only the checksum
	 * @throws IOException file access error
	 */
	public static String generateChecksumOnly(String pInFile) throws IOException {

		String type = "MD5";
		return generateChecksum(type, pInFile);
		
	}
	
	/**
	 * Generates a checksum for a file and writes it to the log file
	 * @param pInFile file that has been checksummed
	 * @param pHash file checksum
	 * @param pLogFile log file to write to
	 * @throws IOException file access error
	 */
	public static void writeChecksumToLog(String pInFile, String pHash, String pLogFile) throws IOException {

		BufferedWriter outputFile = new BufferedWriter(new FileWriter(pLogFile,true));

		outputFile.write("*** "+pHash.split(":")[0]+" checksum for \""+(new File(pInFile).getName()+"\": "+pHash.split(":")[1]));
		outputFile.newLine();
		outputFile.close();
		
	}
	
	/**
	 * Bulk copy data from one buffer to another
	 * @param pInData buffer to read from
	 * @param pOutputFile buffer to read to
	 * @throws IOException file access error
	 */
	public static void writeBufferToFile(BufferedReader pInData, BufferedWriter pOutputFile) throws IOException {
		char[] readBuffer = new char[Settings.BUFSIZE];
		int bytesRead = 0;
		while(pInData.ready()) {
			bytesRead = pInData.read(readBuffer);
			pOutputFile.write(readBuffer, 0, bytesRead);
		}
	}
	
	/**
	 * Copies an inputstream to a file
	 * @param pClass base class to use to find resource
	 * @param pResource resource to find
	 * @param pFile local file to write to
	 * @throws IOException if an error occurred
	 */
	public static void copyResourceToFile(Class<?> pClass, String pResource, String pFile) throws IOException {
		BufferedWriter out = new BufferedWriter(new FileWriter(pFile));
		BufferedReader in = new BufferedReader(new InputStreamReader(getResource(pClass, pResource)));
		writeBufferToFile(in, out);
		in.close();
		out.close();
	}
	
	/**
	 * Append a buffer to a file (used for stdout/stderr in to log file)
	 * @param pName name of the buffer
	 * @param pInData buffer to read from 
	 * @param pOutputFile buffer to write to
	 * @throws IOException file access error
	 */
	public static void appendBufferToFile(String pName, BufferedReader pInData, BufferedWriter pOutputFile) throws IOException {
		//append buffer to log file
		pOutputFile.write("--------------------------------------\n");
		pOutputFile.write(pName+":\n");
		writeBufferToFile(pInData, pOutputFile);
		pOutputFile.write("--------------------------------------\n");
	}
	
	/**
	 * Append information about the recently executed process to a log file.
	 * @param pExitCode exit code of recently executed program
	 * @param pCommandLine command line of recently executed program
	 * @param pLogFile log file to write to 
	 * @throws IOException file access error
	 */
	public static void appendProcessInfoToLog(int pExitCode, List<String> pCommandLine, BufferedWriter pLogFile) throws IOException {
		pLogFile.write("--------------------------------------\n");		
		pLogFile.write(pCommandLine.toString());
		pLogFile.write("\n");
		pLogFile.write("Exitcode: "+pExitCode+"\n");
		pLogFile.write("--------------------------------------\n");		
		
	}
	
	/**
	 * Creates a new temporary directory 
	 * @return File object for new directory
	 * @throws IOException file access error
	 */
	public static File newTempDir() throws IOException {
		//create a temporary local output file name for use with the local tool in TMPDIR
		new File(Settings.TMP_DIR).mkdirs();
		File localOutputTempDir = File.createTempFile(Settings.JOB_NAME,"",
				new File(Settings.TMP_DIR));
		//change this to a directory and put all the files in there
		localOutputTempDir.delete();
		localOutputTempDir.mkdirs();
		localOutputTempDir.setReadable(true, false);
		localOutputTempDir.setWritable(true, false);//need this so output can be saved
		return localOutputTempDir;
	}

	/**
	 * Recursively delete a local directory
	 * @param pDir directory to delete
	 * @return success
	 */
	public static boolean deleteDirectory(File pDir) {
		boolean ret = true;
		for(File f:pDir.listFiles()) {
			if(f.isDirectory()) ret&=deleteDirectory(f);
			ret&=f.delete();
		}
		ret&=pDir.delete();
		return ret;
	}
	
	
	/**
	 * Copy an input file to a local temporary file and return the new local filename
	 * @param pTmpDir temporary directory to copy files to
	 * @param pFs The HDFS filesystem
	 * @param pInputFile The URL/name of the input file
	 * @return A File instance for the new local temporary file
	 * @throws IOException file access error
	 */
	public static File copyInputToLocalTemp(File pTmpDir, FileSystem pFs, String pInputFile) throws IOException {
		
		//put the file in the new temporary directory
		//we use lastindexof as path may start hdfs:// and File doesn't understand
		System.out.println(pTmpDir+"<-"+pInputFile);
		String tempFile = pTmpDir.getAbsolutePath();
		if(pInputFile.contains("/")) {
			tempFile+=(pInputFile.substring(pInputFile.lastIndexOf("/")));
		} else {
			tempFile+="/"+pInputFile;
		}
		File tempInputFile = new File(tempFile);

		//if this file has already been copied - skip
		//FIXME: this thinks that the file exists when it doesn't
		if(tempInputFile.exists()) return tempInputFile;
		
		//i.e. this file is a local file
		if(new File(pInputFile).exists()) {
		//	System.out.println("copying from local fs");
			FileInputStream fis = new FileInputStream(pInputFile);
			FileOutputStream fos = new FileOutputStream(tempInputFile);
			byte[] buffer = new byte[Settings.BUFSIZE];
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
		if(pFs.exists(new Path(pInputFile))) {
		//	System.out.println("copying from hdfs");
			pFs.copyToLocalFile(new Path(pInputFile), new Path(tempFile));
			tempInputFile = new File(tempFile);
			return tempInputFile;
		}
		//TODO: check for HTTP files etc

		System.out.println("file not found");
		return null;
	}
	
	/**
	 * Convenience method to zip the generated files together (no compression)
	 * @param pSuccess whether workflow was successful or not
	 * @param pChecksums checksums for all files to be zipped
	 * @param pGeneratedFiles list of files to be zipped
	 * @param pZipFile output zip file
	 * @param pTempDir local temporary directory that contains files to zip 
	 * @throws IOException file access error
	 */
	public static void zipGeneratedFiles(boolean pSuccess, HashMap<String, String> pChecksums, 
			List<String> pGeneratedFiles, String pZipFile, String pTempDir) throws IOException {
		
		System.out.println("zipGeneratedFiles("+pZipFile+" ...)");
		
		ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(pZipFile));

		int compression = ZipEntry.STORED;
		
		//add an empty file indicating success or failure
		ZipEntry status;
		if(pSuccess) status = new ZipEntry("SUCCESS");
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
		for(String file : pChecksums.keySet()) {
			//nasty hack
			if(file.endsWith(".report.xml")) continue;
			
			//String fn = new File(file).getName();
			//only add the file if it exists!
			if(new File(pTempDir+file).exists()) {
				String out = pChecksums.get(file).split(":")[1]+"  data/"+file+"\n";
				size+=out.getBytes().length;
				crc.update(out.getBytes());
			}
		}
		manifest.setCrc(crc.getValue());
		manifest.setSize(size);
		zip.putNextEntry(manifest);
		for(String file : pChecksums.keySet()) {
			//nasty hack
			if(file.endsWith(".report.xml")) continue;

			//String fn = new File(file).getName();
			
			//THIS MUST MATCH THE ABOVE STRING EXACTLY!
			//only add the file if it exists!
			if(new File(pTempDir+file).exists()) {
				String out = pChecksums.get(file).split(":")[1]+"  data/"+file+"\n";
				zip.write(out.getBytes());
			}
		}
		zip.closeEntry();
		
		//copy all the files in
		for(String file : pGeneratedFiles) {
			//add file to zip
			File input = new File(pTempDir+file);
			
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
			byte[] readBuffer = new byte[Settings.BUFSIZE];
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

			readBuffer = new byte[Settings.BUFSIZE];
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
	 * @param pFileName file to retrieve ssim value from
	 * @return SSIM comparison value
	 */
	public static double getSSIMCompareVal(String pFileName) {
		return new Double(getXpathVal(pFileName, "/comparison/task/ssim"));
	}
	

	/**
	 * Recover the value associated with a xpath expression
	 * @param pFileName xml file
	 * @param pXPath XPath expression to evaluate
	 * @return value value associated with the xpath expression (or null if error)
	 */
	public static String getXpathVal(String pFileName, String pXPath) {
		try {
			DocumentBuilder docB = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = docB.parse(pFileName);
			Node root = doc.getFirstChild();		
			XPath xpath = XPathFactory.newInstance().newXPath();
			return xpath.evaluate(pXPath, root);
		} catch (ParserConfigurationException pce) {
		} catch (NumberFormatException e) {
		} catch (XPathExpressionException e) {
		} catch (SAXException e) {
		} catch (IOException e) {
		}
		return null;
	}
	
	/**
	 * Get the PSNR output from imagemagick
	 * @param pFile file to check
	 * @return PSNR value from file
	 */
	public static double getPSNRVal(String pFile) {
		double ret = 0;
		
		try {
			BufferedReader input = new BufferedReader(new FileReader(pFile));
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
	 * @param pFile matchbox xml file to fix
	 */
	public static void fixupMatchboxXML(String pFile) {
		
		try {
			BufferedReader input = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(pFile))));		
			BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(pFile+".new"))));

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
			
			new File(pFile+".new").renameTo(new File(pFile));
			

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
		//fixupMatchboxXML(args[0]);
		
	}
	
}
