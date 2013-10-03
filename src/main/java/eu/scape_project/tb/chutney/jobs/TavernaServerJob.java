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

package eu.scape_project.tb.chutney.jobs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import eu.scape_project.tb.chutney.Settings;
import eu.scape_project.tb.chutney.Settings.JobType;

/**
 * Class to communicate with a Taverna Server instance.  This might be not work at the 
 * moment due to refactoring around other workflows.
 * @author wpalmer
 *
 */
public class TavernaServerJob implements ChutneyJob {

	/**
	 * Authentication class for HTTP connections
	 * @author wpalmer
	 *
	 */
	private static class Auth extends Authenticator {
		public PasswordAuthentication getPasswordAuthentication() {
			return new PasswordAuthentication(Settings.TAVERNA_SERVER_USER,
												Settings.TAVERNA_SERVER_PASS.toCharArray());
		}
	}
	
	//for details of this rest API look at http://127.0.0.1:8080/tavernaserver/ or equivalent
	
	private String gUUID = "";
	private HttpURLConnection gServer;
	private String gWorkflow = "";
	private HashMap<String, String> gInputValues;
	
	/**
	 * Convenience method to setup a server connection
	 * Note: may need to connect() to server after this, if appropriate
	 * @param pAddress URL 
	 * @param pMethod GET/PUT/DELETE/etc
	 * @return an HttpURLConnection set up according to the parameters
	 * @throws IOException
	 */
	private HttpURLConnection setupConnection(String pAddress, String pMethod) throws IOException {
		Authenticator.setDefault(new Auth());
		URL url = new URL(pAddress);
		HttpURLConnection server = (HttpURLConnection)url.openConnection();
		server = (HttpURLConnection)url.openConnection();
		server.setRequestMethod(pMethod);
		server.setDoOutput(true);
		return server;
	}
	
	/**
	 * Puts a Taverna workflow inputport value onto the server
	 * @param pInputPort
	 * @param pInputValue
	 * @throws IOException
	 */
	private void putInputValue(String pInputPort, String pInputValue) throws IOException {	
		gServer = setupConnection(Settings.TAVERNA_SERVER+gUUID+"/input/input/"+pInputPort,"PUT");		
		String xmlRequest = "<t2sr:runInput xmlns:t2sr=\"http://ns.taverna.org.uk/2010/xml/server/rest/\">" +
								"<t2sr:value>"+pInputValue+"</t2sr:value>"+
								"</t2sr:runInput>";
		BufferedWriter output = new BufferedWriter(new OutputStreamWriter(gServer.getOutputStream()));
		output.write(xmlRequest);
		output.close();
		gServer.connect();
	}
	
	/**
	 * Convenience method to copy a file to an OutputStream
	 * @param pFile
	 * @param pOutput
	 * @throws IOException
	 */
	private void writeFileToOutputStream(String pFile, OutputStream pOutput) throws IOException {
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		FileInputStream fileReader = new FileInputStream(pFile);
		byte[] buffer = new byte[Settings.BUFSIZE];
		while(fileReader.available()>0){
			int bytesRead = fileReader.read(buffer);
			outStream.write(buffer, 0, bytesRead);	
		}		
		outStream.writeTo(pOutput);
		outStream.close();
		fileReader.close();
	}
		
	/**
	 * Get a list of required output files
	 * @return always null (at the moment)
	 */
	public String[] getOutputFiles() {
		return null;
	}

	/**
	 * Get a list of required input files
	 * @return always null (at the moment)
	 */
	public String[] getInputFiles() {
		return null;
	}

	/**
	 * Query whether the job run was successful
	 * @return true (at the moment)
	 */
	public boolean wasSuccessful() {
		return true;
	}
	
	/**
	 * Get the full path to the log file
	 */
	public String getLogFilename() {
		return gInputValues.get(Settings.TAVERNA_WORKFLOW_INPUTFILEPORT)+".log";
	}
	
	/**
	 * Starts the workflow executing
	 * @throws IOException
	 */
	private void startWorkflow() throws IOException {
		gServer = setupConnection(Settings.TAVERNA_SERVER+gUUID+"/status","PUT");
		String str = "Operating";
		BufferedWriter output = new BufferedWriter(new OutputStreamWriter(gServer.getOutputStream()));
		output.write(str);
		output.close();
		gServer.connect();
	}

	/**
	 * Queries the server to see if job has finished executing
	 * @return
	 * @throws IOException
	 */
	private boolean isJobFinished() throws IOException {
		gServer = setupConnection(Settings.TAVERNA_SERVER+gUUID+"/status","GET");
		gServer.connect();
		BufferedReader bufR = new BufferedReader(new InputStreamReader(gServer.getInputStream()));
		String status = bufR.readLine();
		System.out.println(status);
		return status.equals("Finished");
	}

	/**
	 * Constructor
	 * @param pList input port names and values
	 * @param pWorkflow workflow to execute
	 */
	public TavernaServerJob(HashMap<String, String> pList, String pWorkflow) {
		gInputValues = pList;
		this.gWorkflow = pWorkflow;		
	}
	
	/**
	 * Recover stdout and stderr from the server and write to fileName
	 * @param pFileName
	 * @throws IOException
	 */
	private void getStdoutToFile(String pFileName) throws IOException {

		BufferedWriter logFile = new BufferedWriter(new FileWriter(pFileName));

		//write input and output file details to log
		String outFile = pFileName.substring(0, pFileName.lastIndexOf("."));
		logFile.write("Output file: "+outFile+
						" exists: "+(new File(outFile).exists())+
						" size: "+(new File(outFile).length()));
		logFile.newLine();
		
		String inFile = gInputValues.get(Settings.TAVERNA_WORKFLOW_INPUTFILEPORT);
		logFile.write("Input file: "+inFile+
						" exists: "+(new File(inFile).exists())+
						" size: "+(new File(inFile).length()));
		logFile.newLine();
		
		//recover stdout and write to log
		gServer = setupConnection(Settings.TAVERNA_SERVER+gUUID+"/listeners/io/properties/stdout","GET");
		gServer.connect();
		BufferedReader logBuf = new BufferedReader(new InputStreamReader(gServer.getInputStream()));

		logFile.newLine();
		logFile.write("stdout:");logFile.newLine();
		logFile.write("---------------------------");logFile.newLine();
		
		char[] readBuffer = new char[Settings.BUFSIZE];
		int bytesRead = 0;
		while(logBuf.ready()) {
			bytesRead = logBuf.read(readBuffer);
			logFile.write(readBuffer, 0, bytesRead);
		}

		//recover stderr and write to log
		gServer = setupConnection(Settings.TAVERNA_SERVER+gUUID+"/listeners/io/properties/stderr","GET");
		gServer.connect();
		logBuf = new BufferedReader(new InputStreamReader(gServer.getInputStream()));
		
		logFile.newLine();
		logFile.write("stderr:");logFile.newLine();
		logFile.write("---------------------------");logFile.newLine();

		readBuffer = new char[Settings.BUFSIZE];
		bytesRead = 0;
		while(logBuf.ready()) {
			bytesRead = logBuf.read(readBuffer);
			logFile.write(readBuffer, 0, bytesRead);
		}

		logFile.close();
		
	}
	
	/**
	 * Set up the job
	 */
	public void setup() throws IOException {
		
	}
	
	/**
	 * Run the job once it is set up
	 */
	public void run() { 
		
		try {
		//initial connection to server
		
			do {
				gServer = setupConnection(Settings.TAVERNA_SERVER, "POST");
				//we need to make sure the content type of the file is correct
				gServer.setRequestProperty("Content-type", "application/vnd.taverna.t2flow+xml");
				//write the workflow to the server
				writeFileToOutputStream(gWorkflow, gServer.getOutputStream());
				gServer.connect();
				
				//should have a 201 response code
				if(gServer.getResponseCode()!=HttpURLConnection.HTTP_CREATED) {
					//i.e. returned not ok
					System.out.println("Response: "+gServer.getResponseCode());
					System.out.println("Retrying in 5 seconds...");
					Thread.sleep(5000);
				} else {
					//i.e. returned ok
					//location returns the uuid
					gUUID = gServer.getHeaderField("Location").substring(Settings.TAVERNA_SERVER.length());
					System.out.println("UUID: "+gUUID);
				}
				//loop is not 201 here i.e. on a 403 
			} while(gServer.getResponseCode()!=HttpURLConnection.HTTP_CREATED);
			
		//print any response to the console
		//BufferedReader bufR = new BufferedReader(new InputStreamReader(server.getInputStream()));
		//while(bufR.ready())
			//System.out.println(bufR.readLine());
		
		//now set inputs to workflow ports
		for(String key : gInputValues.keySet()) {
			putInputValue(key,gInputValues.get(key));
			System.out.println("Response: "+gServer.getResponseCode());
		}
		
		//execute workflow
		startWorkflow();

		System.out.println("Response: "+gServer.getResponseCode());
		
		//poll until the job is done
		while(!isJobFinished()) 
			Thread.sleep(1000);
		
		//collect stdout and stderr and store in a log file
		//write a local log file
		//note this is a HACK to get the temp file name
		getStdoutToFile(getLogFilename());
		
		} catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * This method retrieves a list of currently outstanding jobs from the server
	 * and then deletes them all.
	 * This is particularly useful when the jobs have not been terminated correctly.
	 * This is executed by the main in this class which is used for testing purposes.
	 */
	@SuppressWarnings("unused")
	private void deleteJobs() {
		try {
			//get a list of open runs
			gServer = setupConnection(Settings.TAVERNA_SERVER,"GET");
			gServer.connect();

			//parse the values returned
			DocumentBuilder docB = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = docB.parse(gServer.getInputStream());

			//iterates through the nodes and delete each job
			NodeList list = doc.getElementsByTagName("ns3:run");
			for(int i=0;i<list.getLength();i++) {
				String job = list.item(i).getAttributes().getNamedItem("ns1:href").getNodeValue();
				gServer = setupConnection(job,"DELETE");
				gServer.connect();
				//this seems to return 204 if it worked ok
				System.out.println("Job deleted: "+gServer.getResponseCode()+" "+job);
			}
		} catch(IOException e) {

		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * This method should be run after local files have been recovered
	 */
	public void cleanup() throws IOException {
		//delete the job and tidy up
		HttpURLConnection server = setupConnection(Settings.TAVERNA_SERVER+gUUID,"DELETE");
		server.connect();
		
		System.out.println("Deleted job "+gUUID+": "+server.getResponseCode());
		
		//delete all the files
		//FIXME
		//new File(inputValues.get(Settings.TAVERNA_WORKFLOW_OUTPUTFILEPORT)).delete();
		//new File(inputValues.get(Settings.TAVERNA_WORKFLOW_OUTPUTFILEPORT)+".log").delete();
		//new File(inputValues.get(Settings.TAVERNA_WORKFLOW_INPUTFILEPORT)).delete();
		
	}

	/**
	 * This is a test main() - the class will usually be used in TavernaHadoopWrapper
	 * @param args command line arguments
	 * @throws IOException file access error
	 */
	public static void main(String[] args) throws IOException {
		
	}

	public static JobType getJobType() {
		return JobType.TavernaServerJob;
	}

	public static String getShortJobType() {
		return "TSJ";
	}

}
