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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * A class to deal with JPEG2000, specifically the profiles, command line 
 * arguments for encoders and validating Jpylyzer outputs against input
 * profiles
 * 
 * @author wpalmer
 *
 */

public class Jpeg2kProfile {

	/**
	 * This class contains the constant strings for values contained in jpylyzer 
	 * outputs.  It is also used for loading profiles from XML property files
	 *  
	 * @author wpalmer
	 *
	 */
	private static final class Keys {
		 //these are element names used in the jpylyzer outputs
		 public final static String	ISVALID = "isValid";
		 public final static String	ORDER = "order";
		 public final static String	PRECINCTS = "precincts";
		 public final static String	SOP = "sop";
		 public final static String	EPH = "eph";
		 public final static String	LAYERS = "layers";
		 public final static String	LEVELS = "levels";
		 public final static String	CODEBLOCKWIDTH = "codeBlockWidth";
		 public final static String	CODEBLOCKHEIGHT = "codeBlockHeight";
		 public final static String	CODINGBYPASS = "codingBypass";
		 public final static String	TRANSFORMATION = "transformation";
		 public final static String	PRECINCTSIZE = "precinctSize";
		 public final static String	NUMBEROFTILES = "numberOfTiles";
		 public final static String TILEXDIM = "xTsiz";
		 public final static String TILEYDIM = "yTsiz";
		 //these are used for the xml loading/saving of profiles
		 public final static String TILED = "tiled";
		 public final static String TILEDIM = "tileDim";
		 public final static String CODEBLOCKSIZE = "codeBlockSize";
		 public final static String COMPRESSIONRATES = "compressionRates";
	}
	
	//these are the BL jpeg2000 settings loaded as defaults
	@SuppressWarnings("javadoc")
	public static class JP2Profile {
		public boolean irreversible = true;
		public String progressionOrder = "RPCL";
		public boolean tiled = false; 
		public int tileDim = 0;
		public int numTiles = 1;
		public int levels = 6;	
		//codestream markers (NOTE: SOP and EPH are not specified in the BL profile)
		public boolean SOP = false;
		public boolean EPH = false;
		//NOTE: we rely on these being in descending order
		//NOTE: openjpeg needs the extra 128 values
		//there should be levels+1 precincts listed here
		public int[] precincts = { 256, 256, 128, 128, 128, 128, 128 };
		public int codeblockSize = 64;
		//NOTE: when enabled with BL profile causes artefacts with openjpeg
		public boolean coderBypass = false; 
		//NOTE: these should be in ascending ratio of compression i.e. 1:1 -> 320:1
		//also, layers = compressionRates.length
		//FIXME: should 1 be 2.16??
		public double[] compressionRates = { 1, 2.4, 2.75, 3.4, 4.6, 7, 11.25,
			20, 40, 80, 160, 320 };
	}

	/**
	 * Generates a Kakadu command line from the currently loaded profile.
	 * NOTE: it has not been tested using Kakadu.
	 * @param profile profile to use to generate command
	 * @return A List of Strings containing the command line
	 */
	//this is untested - for debugging purposes only, so far
	@SuppressWarnings("unused")
	public static List<String> getKakaduCommand(JP2Profile profile) {
		
		LinkedList<String> command = new LinkedList<String>();
		
		//transform
		if(profile.irreversible) command.add("Creversible=no");
			else command.add("Creversible=yes");
		
		//progression order
		command.add("Corder="+profile.progressionOrder);
		
		//TODO components?
		
		//tile size
		if(profile.numTiles>1) {
			//TODO: add code here
		} else {
			//do nothing - openjpeg uses a tile for the whole image by default
		}
	
		//levels
		command.add("Clevels="+profile.levels);
		
		//this will not execute so codestream markers are not added
		if(false) {
		//codestream markers
		if(profile.EPH) {
			command.add("Cuse_eph=yes");
		}
		if(profile.SOP) {
			command.add("Cuse_sop=yes");
		}
		}
		
		//FIXME: unsure what the equivalent for this is with OpenJPEG etc
		command.add("ORGgen_plt=yes");
			
		//precincts
		String prec = "Cprecincts=";
		for(int precinct : profile.precincts) {
			prec+="{"+precinct+","+precinct+"},";
		}
		//get rid of the last comma
		command.add(prec.substring(0, prec.length()-1));
		
		//codeblock size
		command.add("Cblk={"+profile.codeblockSize+","+profile.codeblockSize+"}");
		
		//coder bypass
		if(profile.coderBypass) {
			command.add("Cmodes=BYPASS");
		}
		
		//rates
		command.add("-rate");
		String rates = "";
		for(double rate : profile.compressionRates) {
			if(1==rate) {
				rates += "-,";
			} else {
				//note in the BL format document rates are passed to Kakadu in relation
				//to 24bpp
				//check correctness of this for other bit depths
				rates += String.format("%.3f", 24/rate) + ",";
			}
		}
		//get rid of the last comma
		command.add(rates.substring(0, rates.length()-1));
				
		return command;
	}
	
	/**
	 * Generates an OpenJPEG command line from the currently loaded profile.
 	 * @param profile profile to use to generate command
	 * @return A List of Strings containing the command line
	 */
	public static List<String> getOpenJpegCommand(JP2Profile profile) {
		
		LinkedList<String> command = new LinkedList<String>();
		
		//transform
		if(profile.irreversible) command.add("-I");
		
		//progression order
		command.add("-p");
		command.add(profile.progressionOrder);
		
		//TODO components
		
		//tile size
		if(profile.numTiles>1) {
			//TODO: add code here
		} else {
			//do nothing - openjpeg uses a tile for the whole image by default
		}
	
		//levels
		//OpenJPEG says it should be passed #levels+1 for #levels to equal required levels 
		command.add("-n");
		command.add(new Integer(profile.levels+1).toString());
		
		//codestream markers
		if(profile.EPH) command.add("-EPH");
		if(profile.SOP) command.add("-SOP");
		
		//precincts
		command.add("-c");
		String prec = "";
		for(int precinct : profile.precincts) {
			prec+="["+precinct+","+precinct+"],";
		}
		//get rid of the last comma
		command.add(prec.substring(0, prec.length()-1));
		
		//codeblock size
		command.add("-b");
		command.add(profile.codeblockSize+","+profile.codeblockSize);

		//FIXME! this setting causes problems with the encode and produces files with
		//bad compression artefacts
		//if(false) {
		//coder bypass
		if(profile.coderBypass) {
			command.add("-M");
			command.add("1");
			
			//NOTE: jj2k help suggests that coder bypass needs a termination algorithm (term_type)
			//From using OPJViewer it seems that Kakadu just sets coder bypass and nothing else so
			//no term_type should be required.  Therefore that is an OpenJPEG bug, still present in 
			//version 2.0.0
		}
		//}
		
		//rates
		command.add("-r");
		String rates = "";
		for(double rate : profile.compressionRates) {
			//note this list is backwards for openjpeg
			rates = String.format("%.3f", rate) + "," + rates;
		}
		//get rid of the last comma
		command.add(rates.substring(0, rates.length()-1));
		
		return command;
	}
	
	/**
	 * Generates a Jasper command line from the currently loaded profile.
	 * @param profile profile to use to generate command
	 * @return A List of Strings containing the command line
	 */
	public static List<String> getJasperCommand(JP2Profile profile) {
		
		LinkedList<String> command = new LinkedList<String>();
		
		command.add("--output-format");
		command.add("jp2");
		
		//transform
		if(profile.irreversible) {
			command.add("--output-option");
			command.add("mode=real");
		}
		
		//progression order
		command.add("--output-option");
		command.add("prg="+profile.progressionOrder.toLowerCase());
		
		//TODO components
		
		//tile size
		if(profile.numTiles>1) {
			//TODO: add code here
		} else {
			//do nothing - openjpeg uses a tile for the whole image by default
		}
	
		//levels
		command.add("--output-option");
		command.add("numrlvls="+Integer.toString(profile.levels));
		
		//codestream markers
		if(profile.EPH) {
			command.add("--output-option");
			command.add("eph");
		}
		if(profile.SOP) {
			command.add("--output-option");
			command.add("sop");
		}
		
		//precincts
		//NOTE: can only set one precinct size for JasPer
		command.add("--output-option");
		command.add("prcwidth="+profile.precincts[0]);
		command.add("--output-option");
		command.add("prcheight="+profile.precincts[0]);
	
		//codeblock size
		command.add("--output-option");
		command.add("cblkwidth="+profile.codeblockSize);
		command.add("--output-option");
		command.add("cblkheight="+profile.codeblockSize);
		
		//coder bypass
		if(profile.coderBypass) {
			command.add("--output-option");
			command.add("lazy");
		}
		
		//must set an overall rate <1.0 to lossy encode =1/x (x:1)
		//this rate must be greater than the biggest rate in layer rates
		command.add("--output-option");
		command.add("rate="+(1/profile.compressionRates[0]));
		
		//rates
		command.add("--output-option");
		String rates = "";
		for(int i=1;i<profile.compressionRates.length;i++) {
			//note this list is backwards for openjpeg
			rates = String.format("%.3f", 1/profile.compressionRates[i]) + "," + rates;
		}
		//get rid of the last comma
		command.add("ilyrrates="+rates.substring(0, rates.length()-1));
		
		return command;
	}
	
	/**
	 * Generates a JJ2000 command line from the currently loaded profile.
	 * @param profile profile to use to generate command
	 * @return A List of Strings containing the command line
	 */
	public static List<String> getJJ2000Command(JP2Profile profile) {
		
		LinkedList<String> command = new LinkedList<String>();
		
		command.add("-file_format");
		command.add("on");
		
		command.add("-lossless");
		//transform
		if(profile.irreversible) {
			command.add("off");
		} else {
			command.add("on");
		}
		
		//progression order - should add the rest here (found with trial & error & jpylyzer)
		command.add("-Aptype");
		if (profile.progressionOrder.toLowerCase().equals("rlcp"))
			command.add("res");
		if (profile.progressionOrder.toLowerCase().equals("rpcl"))
			command.add("res-pos");
		if (profile.progressionOrder.toLowerCase().equals("lrcp"))
			command.add("layer");
		
		//TODO components
		
		//tile size
		if(profile.numTiles>1) {
			//TODO: add code here
		} else {
			//do nothing - openjpeg uses a tile for the whole image by default
		}
	
		//levels
		command.add("-Wlev");
		command.add(Integer.toString(profile.levels));
		
		//codestream markers
		command.add("-Peph");
		if(profile.EPH) {
			command.add("on");
		} else {
			command.add("off");
		}
		command.add("-Psop");
		if(profile.SOP) {
			command.add("on");
		} else {
			command.add("off");
		}
		
		//precincts
		command.add("-Cpp");
		String prec = "";
		for(int precinct : profile.precincts) {
			prec+=precinct + " " + precinct + " ";
		}
		//get rid of the last space
		command.add(prec.substring(0, prec.length()-1));
		
		//codeblock size
		command.add("-Cblksiz");
		command.add(Integer.toString(profile.codeblockSize));
		command.add(Integer.toString(profile.codeblockSize));

		//coder bypass
		command.add("-Cbypass");
		if(profile.coderBypass) {
			command.add("on");
		} else {
			command.add("off");
		}
		
		//rates/layers
		command.add("-Alayers");
		String rates = "";
		for(double rate : profile.compressionRates) {
			//note this list is backwards for openjpeg
			rates = String.format("%.3f", 24/rate) + " " + rates;
		}
		//get rid of the last comma
		command.add(rates);
		
		return command;
	}
	
	/**
	 * Writes a copy of the BL Jpeg 2000 profile to an XML properties file
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	@SuppressWarnings("unused")
	private static void writeBLProfile(String fileName) throws FileNotFoundException, IOException {

		Properties profile = new Properties();

		profile.setProperty(Keys.ORDER, "RPCL");
		profile.setProperty(Keys.SOP, "yes");
		profile.setProperty(Keys.EPH, "yes");
		//NOTE: should 1 be 2.16?
		profile.setProperty(Keys.COMPRESSIONRATES, "1,2.4,2.75,3.4,4.6,7,11.25,20,40,80,160,320");
		profile.setProperty(Keys.LEVELS, "6");
		profile.setProperty(Keys.CODEBLOCKSIZE, "64");
		profile.setProperty(Keys.CODINGBYPASS, "yes");
		profile.setProperty(Keys.TRANSFORMATION, "irreversible");
		profile.setProperty(Keys.PRECINCTS, "256,256,128,128,128,128");
		profile.setProperty(Keys.NUMBEROFTILES, "1");

		profile.storeToXML(new FileOutputStream(fileName), "British Library JPEG2000 Profile");
		
	}

	/**
	 * Load a j2k profile from an XML properties file
	 * @param fileName file to read profile properties from
	 * @return jpeg2000 profile from input file
	 * @throws IOException file access problem
	 * @throws InvalidPropertiesFormatException invalid input file 
	 */
	public static JP2Profile loadProfile(String fileName) throws InvalidPropertiesFormatException, IOException {
		return loadProfile(new FileInputStream(fileName));
	}
	
	/**
	 * Load a j2k profile from an XML properties file
	 * @return JP2Profile
	 * @throws IOException 
	 * @throws InvalidPropertiesFormatException 
	 */
	protected static JP2Profile loadProfile(InputStream profileStream) throws InvalidPropertiesFormatException, IOException {
		
		JP2Profile jp2Profile = new JP2Profile();
		Properties profile = new Properties();
		profile.loadFromXML(profileStream);
		
		jp2Profile.progressionOrder = profile.getProperty(Keys.ORDER);
		if(profile.getProperty(Keys.SOP).equals("yes")) {
			jp2Profile.SOP=true; 
		} else {
			jp2Profile.SOP=false;
		}
		if(profile.getProperty(Keys.EPH).equals("yes")) { 
			jp2Profile.EPH=true;
		} else{ 
			jp2Profile.EPH=false;
		}
		jp2Profile.levels = new Integer(profile.getProperty(Keys.LEVELS));
		jp2Profile.codeblockSize = new Integer(profile.getProperty(Keys.CODEBLOCKSIZE));
		if(profile.getProperty(Keys.CODINGBYPASS).equals("yes")) {
			jp2Profile.coderBypass=true;
		} else{ 
			jp2Profile.coderBypass=false;
		}
		if(profile.getProperty(Keys.TRANSFORMATION).equals("irreversible")) {
			jp2Profile.irreversible=true;
		} else {
			jp2Profile.irreversible=false;
		} 
		if(profile.getProperty(Keys.TILED)!=null) {
			jp2Profile.tiled = new Boolean(profile.getProperty(Keys.TILED).equals("yes"));
			if(jp2Profile.tiled) {
				jp2Profile.tileDim = new Integer(profile.getProperty(Keys.TILEDIM));
			}
		}

		String[] compRates = profile.getProperty(Keys.COMPRESSIONRATES).split(",");
		jp2Profile.compressionRates = new double[compRates.length];
		for(int i=0;i<compRates.length;i++) {
			jp2Profile.compressionRates[i] = new Double(compRates[i]);
		}

		String[] prec = profile.getProperty(Keys.PRECINCTS).split(",");
		if(prec[0].toLowerCase().equals("no")) {
			//i.e. no precincts
			jp2Profile.precincts = new int[0];
		} else {
			jp2Profile.precincts = new int[prec.length];
			for(int i=0;i<prec.length;i++) {
				jp2Profile.precincts[i] = new Integer(prec[i]);
			}
		}
	
		return jp2Profile;
	}
	
	/**
	 * Loads jpylyzer XML output from a file
	 * @return Pairs of relevant key/values as read  
	 */
	private static HashMap<String, String> loadJpylyzerXML(String fileName) {
		//parse the values returned
		DocumentBuilder docB = null;
		Document doc = null;
		
		try {
			docB = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		} catch(ParserConfigurationException pce) {
			return new HashMap<String, String>();
		}
		try {
			doc = docB.parse(fileName);
		} catch(IOException ioe) {
			ioe.printStackTrace();
		} catch(SAXException se) {
			se.printStackTrace();
		}
		
		Node root = doc.getFirstChild();
		HashMap<String, String> items = new HashMap<String, String>();
		
		XPath xpath = XPathFactory.newInstance().newXPath();
		String pathCCB = "/jpylyzer/properties/contiguousCodestreamBox/";
		String path = pathCCB+"cod/";

		try {

			items.put(Keys.ISVALID, xpath.evaluate("/jpylyzer/isValidJP2", root));
			items.put(Keys.ORDER, xpath.evaluate(path+Keys.ORDER, root));
			items.put(Keys.PRECINCTS, xpath.evaluate(path+Keys.PRECINCTS, root));
			items.put(Keys.SOP, xpath.evaluate(path+Keys.SOP, root));
			items.put(Keys.EPH, xpath.evaluate(path+Keys.EPH, root));
			items.put(Keys.LAYERS, xpath.evaluate(path+Keys.LAYERS, root));
			items.put(Keys.LEVELS, xpath.evaluate(path+Keys.LEVELS, root));
			items.put(Keys.CODEBLOCKWIDTH, xpath.evaluate(path+Keys.CODEBLOCKWIDTH, root));
			items.put(Keys.CODEBLOCKHEIGHT, xpath.evaluate(path+Keys.CODEBLOCKHEIGHT, root));
			items.put(Keys.CODINGBYPASS, xpath.evaluate(path+Keys.CODINGBYPASS, root));
			items.put(Keys.TRANSFORMATION, xpath.evaluate(path+Keys.TRANSFORMATION, root));

			if(items.get(Keys.PRECINCTS).toLowerCase().equals("yes")) {
				int x = 0;
				int y = 0;
				int count = new Integer(xpath.evaluate("count("+path+Keys.PRECINCTSIZE+"X)", root));
				items.put(Keys.PRECINCTSIZE, new Integer(count).toString());
				for(int i=0;i<count;i++) {
					//note xpath array references are 1-based, not 0-based
					x = new Integer(xpath.evaluate(path+Keys.PRECINCTSIZE+"X["+(i+1)+"]", root));
					y = new Integer(xpath.evaluate(path+Keys.PRECINCTSIZE+"Y["+(i+1)+"]", root));
					//HACK: this is a slightly funny way of saving this data, but it works
					if (x==y) items.put(Keys.PRECINCTSIZE+i,new Integer(x).toString()); 
					else System.out.println("ERROR in precinctSize parsing");
				}
			}

			path = pathCCB+"siz/";
			items.put(Keys.NUMBEROFTILES, xpath.evaluate(path+Keys.NUMBEROFTILES, root));
			if(new Integer(items.get(Keys.NUMBEROFTILES))>1) {
				items.put(Keys.TILEXDIM, xpath.evaluate(path+Keys.TILEXDIM, root));
				items.put(Keys.TILEYDIM, xpath.evaluate(path+Keys.TILEYDIM, root));
			}

		} catch(XPathExpressionException xpee) {
			System.out.println("Only loaded the following:");
			//print all the loaded data
			for(String s:items.keySet())System.out.println(s+": "+items.get(s));
			xpee.printStackTrace();
		}

		return items;

	}
	
	/**
	 * Checks whether jpylyzer says the input file was valid
	 * @param fileName file containing jpylyzer outputs to check
	 * @return true if it is valid, false if not  
	 */
	public static boolean jpylyzerSaysValid(String fileName) {
		
		HashMap<String, String> items = loadJpylyzerXML(fileName);
		
		String key = Keys.ISVALID;
		if(items.get(key).toLowerCase().equals("true")) return true; 
		
		return false;
	}
	
	/**
	 * Checks whether the currently loaded profile matches the given profile 
	 * @param fileName file containing jpylyzer xml
	 * @param jp2Profile profile to check against
	 * @return true if it is, false if not  
	 */
	public static boolean equalsJpylyzerProfile(String fileName, JP2Profile jp2Profile) {
		
		HashMap<String, String> items = loadJpylyzerXML(fileName);
		//somewhere to store what doesn't match
		HashMap<String, String> mismatchItems = new HashMap<String, String>();
		//assume we have a match unless we get a false
		boolean matchesSettings = true;
		
		if(items.get(Keys.ISVALID).toLowerCase().equals("true")) {
		} else {
		}
		items.remove(Keys.ISVALID);

		//progression order
		if(!items.get(Keys.ORDER).toLowerCase().equals(jp2Profile.progressionOrder.toLowerCase())) {
			matchesSettings = false;
			mismatchItems.put(Keys.ORDER, items.get(Keys.ORDER));
		}
		items.remove(Keys.ORDER);
		
		//number of levels
		/*
		 * A file encoded with OpenJPEG will cause Jpylyzer to report (n-1) levels, given n on the command line
		 * so files should be encoded using "-n (n+1)".
		 * A file encoded with Kakadu will cause Jpylyzer to report n levels, given n on the command line
		 * See https://intranet.bl.uk:8443/confluence/display/%7Ewilliampalmer/J2K+codec+comparison 
		 */
		if(!new Integer(items.get(Keys.LEVELS)).equals(jp2Profile.levels)) {
			matchesSettings = false;
			mismatchItems.put(Keys.LEVELS, items.get(Keys.LEVELS));
		}
		items.remove(Keys.LEVELS);

		//sop
		if(!(jp2Profile.SOP==items.get(Keys.SOP).toLowerCase().equals("yes"))) {
			matchesSettings = false;
			mismatchItems.put(Keys.SOP, items.get(Keys.SOP));
		}
		items.remove(Keys.SOP);
		
		//eph
		if(!(jp2Profile.EPH==items.get(Keys.EPH).toLowerCase().equals("yes"))) {
			matchesSettings = false;
			mismatchItems.put(Keys.EPH, items.get(Keys.EPH));
		}
		items.remove(Keys.EPH);
		
		//precinct sizes
		if(items.get(Keys.PRECINCTS).toLowerCase().equals("yes")) {
			LinkedList<Integer> precinctVals = new LinkedList<Integer>();
			//add all the precinct values to a new list
			for(int i=new Integer(items.get(Keys.PRECINCTSIZE));i>0;i--) {
				precinctVals.add(new Integer(items.get(Keys.PRECINCTSIZE+""+(i-1))));			
				items.remove(Keys.PRECINCTSIZE+(i-1));		
			}
			//for each of the specified precinct values check if it is in the 
			//jpylyzer output, if so ok, if not then fail comparison
			for(int i=0;i<jp2Profile.precincts.length;i++) {
				if(precinctVals.contains(jp2Profile.precincts[i])) {
					precinctVals.removeFirstOccurrence(jp2Profile.precincts[i]);
				} else {
					//this precinct value is not in the precincts in the jpylyzer file
					matchesSettings = false;
					mismatchItems.put(Keys.PRECINCTSIZE,new Integer(jp2Profile.precincts[i]).toString());
				}
			}
			//we end up with additional precinct values here - report it
			for(int i : precinctVals) {
				System.out.println("WARNING: precinctSize("+i+","+i+") in jpylyzer output but not specified in header");
			}
			items.remove(Keys.PRECINCTSIZE);
		}
		
		//precincts
		if(!(items.get(Keys.PRECINCTS).toLowerCase().equals("yes")==(jp2Profile.precincts.length>0))) {
			matchesSettings = false;
			mismatchItems.put(Keys.PRECINCTS, items.get(Keys.PRECINCTS));
		}
		items.remove(Keys.PRECINCTS);

		//layers
		if(!new Integer(items.get(Keys.LAYERS)).equals(jp2Profile.compressionRates.length)) {
			matchesSettings = false;
			mismatchItems.put(Keys.LAYERS, items.get(Keys.LAYERS));
		}
		items.remove(Keys.LAYERS);
				
		//codeblockwidth
		if(!new Integer(items.get(Keys.CODEBLOCKWIDTH)).equals(jp2Profile.codeblockSize)) {
			matchesSettings = false;
			mismatchItems.put(Keys.CODEBLOCKWIDTH, items.get(Keys.CODEBLOCKWIDTH));
		}
		items.remove(Keys.CODEBLOCKWIDTH);
		
		//codeblockheight
		if(!new Integer(items.get(Keys.CODEBLOCKHEIGHT)).equals(jp2Profile.codeblockSize)) {
			matchesSettings = false;
			mismatchItems.put(Keys.CODEBLOCKHEIGHT, items.get(Keys.CODEBLOCKHEIGHT));
		}
		items.remove(Keys.CODEBLOCKHEIGHT);
		
		//number of tiles
		if(new Integer(items.get(Keys.NUMBEROFTILES))>1) {
			if(!new Integer(items.get(Keys.TILEXDIM)).equals(jp2Profile.tileDim)) {
				matchesSettings = false;
				mismatchItems.put(Keys.TILEXDIM, items.get(Keys.TILEXDIM));
			}
			items.remove(Keys.TILEXDIM);

			if(!new Integer(items.get(Keys.TILEYDIM)).equals(jp2Profile.tileDim)) {
				matchesSettings = false;
				mismatchItems.put(Keys.TILEYDIM, items.get(Keys.TILEYDIM));
			}
			items.remove(Keys.TILEYDIM);
		}
		
		if(!(new Integer(items.get(Keys.NUMBEROFTILES))>1==jp2Profile.tiled)) {
			matchesSettings = false;
			mismatchItems.put(Keys.NUMBEROFTILES, items.get(Keys.NUMBEROFTILES));
		}
		items.remove(Keys.NUMBEROFTILES);

		//codingbypass
		if(!(items.get(Keys.CODINGBYPASS).toLowerCase().equals("yes")==jp2Profile.coderBypass)) {
			matchesSettings = false;
			mismatchItems.put(Keys.CODINGBYPASS, items.get(Keys.CODINGBYPASS));
		}
		items.remove(Keys.CODINGBYPASS);
		
		//transformation
		if(!(items.get(Keys.TRANSFORMATION).toLowerCase().equals("9-7 irreversible")==jp2Profile.irreversible)) {
			matchesSettings = false;
			mismatchItems.put(Keys.TRANSFORMATION, items.get(Keys.TRANSFORMATION));
		}
		items.remove(Keys.TRANSFORMATION);
		
		//if there are any unchecked items, print them to the console
		if(items.size()>0) {
			System.out.println("WARNING: unchecked items: ");
			for(String k : items.keySet()) {
				System.out.println(k+": "+items.get(k));
			}
		}

		if(matchesSettings) {
			//System.out.println("matches settings: true");
		} else {
			System.out.println("Settings in jpylyzer xml that don't match loaded j2k profile:");
			for(String k : mismatchItems.keySet()) System.out.println(k+": "+mismatchItems.get(k));
		}
		
		return matchesSettings;
	}
	
	/**
	 * A main method for testing - should not be used
	 * @param args arguments
	 */
	public static void main(String[] args) {
		
		HashMap<String, String> items = loadJpylyzerXML(args[0]);
		for(String key:items.keySet()) {
			System.out.println(key+": "+items.get(key));
		}
//		String out = "";
//		for(String s:getOpenJpegCommand())out+=s+" ";
//		System.out.println(out);
//		out="";
//		for(String s:getKakaduCommand())out+=s+" ";
//		System.out.println(out);
//		out="";
//		for(String s:getJasperCommand())out+=s+" ";
//		System.out.println(out);
//		out="";
//		for(String s:getJJ2000Command())out+=s+" ";
//		System.out.println(out);

	}
	
}
