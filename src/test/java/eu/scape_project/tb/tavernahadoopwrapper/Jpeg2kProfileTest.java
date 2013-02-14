/**
 * Author: William Palmer (William.Palmer@bl.uk)
 * Copyright: British Library, 2012
 * 
 */
package eu.scape_project.tb.tavernahadoopwrapper;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.InvalidPropertiesFormatException;

import org.junit.Test;

import eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile.JP2Profile;

/**
 * Test class for {@link eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile}
 * @author wpalmer
 *
 */
public class Jpeg2kProfileTest {

	/**
	 * Test method for {@link eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile#equalsJpylyzerProfile(java.lang.String, eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile.JP2Profile)}.
	 */
	@Test
	public final void testEqualsJpylyzerProfileBLProfileGood() {
		String profile = "test-data/bl_profile.xml";
		JP2Profile jp2Profile = new JP2Profile();
		String jpylyzerOutputGood = "test-data/jpylyzer_bl_nocoderbypass.xml";
		
		try {
			jp2Profile = Jpeg2kProfile.loadProfile(profile);
		} catch (InvalidPropertiesFormatException e) {
			// TODO Auto-generated catch block
			fail("Invalid properties in file: "+profile);
			//e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			fail("File not found: "+profile);
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			fail("IO exception: "+profile);
			//e.printStackTrace();
		}
		
		//check to match against a known good xml file
		if(Jpeg2kProfile.equalsJpylyzerProfile(jpylyzerOutputGood, jp2Profile)) {
			//this should be the case
			
		} else {
			fail("Input profile is not matching known good output");
		}

	}

	/**
	 * Test method for {@link eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile#equalsJpylyzerProfile(java.lang.String, eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile.JP2Profile)}.
	 */
	@Test
	public final void testEqualsJpylyzerXMLBLProfileBad() {
		String profile = "test-data/bl_profile.xml";
		JP2Profile jp2Profile = new JP2Profile();
		String jpylyzerOutputBad = "test-data/jpylyzer_openjpeg_default.xml";		
		
		try {
			jp2Profile = Jpeg2kProfile.loadProfile(profile);
		} catch (InvalidPropertiesFormatException e) {
			// TODO Auto-generated catch block
			fail("Invalid properties in file: "+profile);
			//e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			fail("File not found: "+profile);
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			fail("IO exception: "+profile);
			//e.printStackTrace();
		}
		
		//check to match against a known bad xml file
		//check to match against a known good xml file
		if(Jpeg2kProfile.equalsJpylyzerProfile(jpylyzerOutputBad, jp2Profile)) {
			//this should not be the case
			fail("Input profile is erroneously matching a known bad output");
		} else {
			
		}		

	}

	/**
	 * Test method for {@link eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile#equalsJpylyzerProfile(java.lang.String, eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile.JP2Profile)}.
	 */
	@Test
	public final void testEqualsJpylyzerXMLBadProfileGood() {
		String profile = "test-data/bad_profile_1.xml";
		JP2Profile jp2Profile = new JP2Profile();
		String jpylyzerOutputGood = "test-data/jpylyzer_bl.xml";
		
		try {
			jp2Profile = Jpeg2kProfile.loadProfile(profile);
		} catch (InvalidPropertiesFormatException e) {
			// TODO Auto-generated catch block
			fail("Invalid properties in file: "+profile);
			//e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			fail("File not found: "+profile);
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			fail("IO exception: "+profile);
			//e.printStackTrace();
		}
		
		if(Jpeg2kProfile.equalsJpylyzerProfile(jpylyzerOutputGood, jp2Profile)) {
			//this should not be the case
			fail("Bad input profile is erroneously matching known good output");
			
		} else {
			
		}

	}
	
	/**
	 * Test method for {@link eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile#equalsJpylyzerProfile(java.lang.String, eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile.JP2Profile)}.
	 */
	@Test
	public final void testEqualsJpylyzerXMLBadProfileBad() {
		String profile = "test-data/bad_profile_1.xml";
		JP2Profile jp2Profile = new JP2Profile();
		String jpylyzerOutputBad = "test-data/jpylyzer_openjpeg_default.xml";		
		
		try {
			jp2Profile = Jpeg2kProfile.loadProfile(profile);
		} catch (InvalidPropertiesFormatException e) {
			// TODO Auto-generated catch block
			fail("Invalid properties in file: "+profile);
			//e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			fail("File not found: "+profile);
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			fail("IO exception: "+profile);
			//e.printStackTrace();
		}
		
		if(Jpeg2kProfile.equalsJpylyzerProfile(jpylyzerOutputBad, jp2Profile)) {
			//this should not be the case
			fail("Input profile is erroneously matching a known bad output");
		} else {
			
		}		

	}
	
	/**
	 * Test method for {@link eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile#jpylyzerSaysValid(java.lang.String)}.
	 */
	
	@Test
	public final void testJpylyzerSaysValidGood() {
		String jpylyzerOutputGood = "test-data/jpylyzer_bl.xml";

		if(!Jpeg2kProfile.jpylyzerSaysValid(jpylyzerOutputGood))
			fail("Output is good, method thinks it is bad");
		
	}
	
	/**
	 * Test method for {@link eu.scape_project.tb.tavernahadoopwrapper.Jpeg2kProfile#jpylyzerSaysValid(java.lang.String)}.
	 */
	
	@Test
	public final void testJpylyzerSaysValidBad() {
		String jpylyzerOutputBad = "test-data/jpylyzer_openjpeg_default.xml";		
	
		if(Jpeg2kProfile.jpylyzerSaysValid(jpylyzerOutputBad))
			fail("Output is bad, method thinks it is good");
		
	}

}
