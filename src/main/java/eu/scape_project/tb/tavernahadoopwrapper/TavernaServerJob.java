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

/**
 * Class to communicate with a Taverna Server instance.  This might be not work at the 
 * moment due to refactoring around other workflows.
 * @author wpalmer
 *
 */
public class TavernaServerJob implements HadoopJob {

	/**
	 * Authentication class for HTTP connections
	 * @author wpalmer
	 *
	 */
	private static class Auth extends Authenticator {
		public PasswordAuthentication getPasswordAuthentication() {
			return new PasswordAuthentication(WrapperSettings.TAVERNA_SERVER_USER,
												WrapperSettings.TAVERNA_SERVER_PASS.toCharArray());
		}
	}
	
	//for details of this rest API look at http://127.0.0.1:8080/tavernaserver/ or equivalent
	
	private String UUID = "";
	private HttpURLConnection server;
	private String workflow = "";
	private HashMap<String, String> inputValues;
	
	/**
	 * Convenience method to setup a server connection
	 * Note: may need to connect() to server after this, if appropriate
	 * @param address URL 
	 * @param method GET/PUT/DELETE/etc
	 * @return an HttpURLConnection set up according to the parameters
	 * @throws IOException
	 */
	private HttpURLConnection setupConnection(String address, String method) throws IOException {
		Authenticator.setDefault(new Auth());
		URL url = new URL(address);
		HttpURLConnection server = (HttpURLConnection)url.openConnection();
		server = (HttpURLConnection)url.openConnection();
		server.setRequestMethod(method);
		server.setDoOutput(true);
		return server;
	}
	
	/**
	 * Puts a Taverna workflow inputport value onto the server
	 * @param inputPort
	 * @param inputValue
	 * @throws IOException
	 */
	private void putInputValue(String inputPort, String inputValue) throws IOException {	
		server = setupConnection(WrapperSettings.TAVERNA_SERVER+UUID+"/input/input/"+inputPort,"PUT");		
		String xmlRequest = "<t2sr:runInput xmlns:t2sr=\"http://ns.taverna.org.uk/2010/xml/server/rest/\">" +
								"<t2sr:value>"+inputValue+"</t2sr:value>"+
								"</t2sr:runInput>";
		BufferedWriter output = new BufferedWriter(new OutputStreamWriter(server.getOutputStream()));
		output.write(xmlRequest);
		output.close();
		server.connect();
	}
	
	/**
	 * Convenience method to copy a file to an OutputStream
	 * @param file
	 * @param output
	 * @throws IOException
	 */
	private void writeFileToOutputStream(String file, OutputStream output) throws IOException {
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		FileInputStream fileReader = new FileInputStream(file);
		byte[] buffer = new byte[WrapperSettings.BUFSIZE];
		while(fileReader.available()>0){
			int bytesRead = fileReader.read(buffer);
			outStream.write(buffer, 0, bytesRead);	
		}		
		outStream.writeTo(output);
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
		return inputValues.get(WrapperSettings.TAVERNA_WORKFLOW_OUTPUTFILEPORT)+".log";
	}
	
	/**
	 * Starts the workflow executing
	 * @throws IOException
	 */
	private void startWorkflow() throws IOException {
		server = setupConnection(WrapperSettings.TAVERNA_SERVER+UUID+"/status","PUT");
		String str = "Operating";
		BufferedWriter output = new BufferedWriter(new OutputStreamWriter(server.getOutputStream()));
		output.write(str);
		output.close();
		server.connect();
	}

	/**
	 * Queries the server to see if job has finished executing
	 * @return
	 * @throws IOException
	 */
	private boolean isJobFinished() throws IOException {
		server = setupConnection(WrapperSettings.TAVERNA_SERVER+UUID+"/status","GET");
		server.connect();
		BufferedReader bufR = new BufferedReader(new InputStreamReader(server.getInputStream()));
		String status = bufR.readLine();
		System.out.println(status);
		return status.equals("Finished");
	}

	/**
	 * Constructor
	 * @param list input port names and values
	 * @param workflow workflow to execute
	 */
	public TavernaServerJob(HashMap<String, String> list, String workflow) {
		inputValues = list;
		this.workflow = workflow;		
	}
	
	/**
	 * Recover stdout and stderr from the server and write to fileName
	 * @param fileName
	 * @throws IOException
	 */
	private void getStdoutToFile(String fileName) throws IOException {

		BufferedWriter logFile = new BufferedWriter(new FileWriter(fileName));

		//write input and output file details to log
		String outFile = fileName.substring(0, fileName.lastIndexOf("."));
		logFile.write("Output file: "+outFile+
						" exists: "+(new File(outFile).exists())+
						" size: "+(new File(outFile).length()));
		logFile.newLine();
		
		String inFile = inputValues.get(WrapperSettings.TAVERNA_WORKFLOW_INPUTFILEPORT);
		logFile.write("Input file: "+inFile+
						" exists: "+(new File(inFile).exists())+
						" size: "+(new File(inFile).length()));
		logFile.newLine();
		
		//recover stdout and write to log
		server = setupConnection(WrapperSettings.TAVERNA_SERVER+UUID+"/listeners/io/properties/stdout","GET");
		server.connect();
		BufferedReader logBuf = new BufferedReader(new InputStreamReader(server.getInputStream()));

		logFile.newLine();
		logFile.write("stdout:");logFile.newLine();
		logFile.write("---------------------------");logFile.newLine();
		
		char[] readBuffer = new char[WrapperSettings.BUFSIZE];
		int bytesRead = 0;
		while(logBuf.ready()) {
			bytesRead = logBuf.read(readBuffer);
			logFile.write(readBuffer, 0, bytesRead);
		}

		//recover stderr and write to log
		server = setupConnection(WrapperSettings.TAVERNA_SERVER+UUID+"/listeners/io/properties/stderr","GET");
		server.connect();
		logBuf = new BufferedReader(new InputStreamReader(server.getInputStream()));
		
		logFile.newLine();
		logFile.write("stderr:");logFile.newLine();
		logFile.write("---------------------------");logFile.newLine();

		readBuffer = new char[WrapperSettings.BUFSIZE];
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
				server = setupConnection(WrapperSettings.TAVERNA_SERVER, "POST");
				//we need to make sure the content type of the file is correct
				server.setRequestProperty("Content-type", "application/vnd.taverna.t2flow+xml");
				//write the workflow to the server
				writeFileToOutputStream(workflow, server.getOutputStream());
				server.connect();
				
				//should have a 201 response code
				if(server.getResponseCode()!=HttpURLConnection.HTTP_CREATED) {
					//i.e. returned not ok
					System.out.println("Response: "+server.getResponseCode());
					System.out.println("Retrying in 5 seconds...");
					Thread.sleep(5000);
				} else {
					//i.e. returned ok
					//location returns the uuid
					UUID = server.getHeaderField("Location").substring(WrapperSettings.TAVERNA_SERVER.length());
					System.out.println("UUID: "+UUID);
				}
				//loop is not 201 here i.e. on a 403 
			} while(server.getResponseCode()!=HttpURLConnection.HTTP_CREATED);
			
		//print any response to the console
		//BufferedReader bufR = new BufferedReader(new InputStreamReader(server.getInputStream()));
		//while(bufR.ready())
			//System.out.println(bufR.readLine());
		
		//now set inputs to workflow ports
		for(String key : inputValues.keySet()) {
			putInputValue(key,inputValues.get(key));
			System.out.println("Response: "+server.getResponseCode());
		}
		
		//execute workflow
		startWorkflow();

		System.out.println("Response: "+server.getResponseCode());
		
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
	 * @throws IOException
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 */
	private void deleteJobs() {
		try {
			//get a list of open runs
			server = setupConnection(WrapperSettings.TAVERNA_SERVER,"GET");
			server.connect();

			//parse the values returned
			DocumentBuilder docB = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = docB.parse(server.getInputStream());

			//iterates through the nodes and delete each job
			NodeList list = doc.getElementsByTagName("ns3:run");
			for(int i=0;i<list.getLength();i++) {
				String job = list.item(i).getAttributes().getNamedItem("ns1:href").getNodeValue();
				server = setupConnection(job,"DELETE");
				server.connect();
				//this seems to return 204 if it worked ok
				System.out.println("Job deleted: "+server.getResponseCode()+" "+job);
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
		HttpURLConnection server = setupConnection(WrapperSettings.TAVERNA_SERVER+UUID,"DELETE");
		server.connect();
		
		System.out.println("Deleted job "+UUID+": "+server.getResponseCode());
		
		//delete all the files
		new File(inputValues.get(WrapperSettings.TAVERNA_WORKFLOW_OUTPUTFILEPORT)).delete();
		new File(inputValues.get(WrapperSettings.TAVERNA_WORKFLOW_OUTPUTFILEPORT)+".log").delete();
		new File(inputValues.get(WrapperSettings.TAVERNA_WORKFLOW_INPUTFILEPORT)).delete();
		
	}

	/**
	 * This is a test main() - the class will usually be used in TavernaHadoopWrapper
	 * @param args command line arguments
	 * @throws IOException file access error
	 */
	public static void main(String[] args) throws IOException {
		
		//load input ports/values to a list
		HashMap<String, String> list = new HashMap<String, String>();
		list.put(WrapperSettings.TAVERNA_WORKFLOW_INPUTFILEPORT, WrapperSettings.STANDALONE_TEST_INPUT);
		list.put(WrapperSettings.TAVERNA_WORKFLOW_OUTPUTFILEPORT, WrapperSettings.STANDALONE_TEST_OUTPUT);
		
		//create new object and execute
		TavernaServerJob tsb = new TavernaServerJob(list, WrapperSettings.TAVERNA_WORKFLOW);
		
		//delete any open jobs
		//NOTE: this job has not been set up on the server yet so it's ok to delete them all!
		tsb.deleteJobs();
		
		//run the job
		tsb.run();
		
		//clean up the job
		tsb.cleanup();
		
	}
	
}
