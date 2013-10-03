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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

//initially based on example here: http://activemq.apache.org/version-5-hello-world.html

/**
 * This class communicates with the ActiveMQ server, logging and retreiving status messages
 * @author wpalmer
 */
public class JMSComms {

	/**
	 * Sends a message to the ActiveMQ server
	 * @param pKey key for the message (i.e. filename or hash)
	 * @param pMessage message to send
	 */
	public static void sendMessage(String pKey, String pMessage) {

		try {

			ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(Settings.ACTIVEMQ_ADDRESS);

			Connection conn = amq.createConnection();
			conn.start();

			Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

			Destination dest = sess.createQueue(pKey);

			MessageProducer prod = sess.createProducer(dest);
			//note use of persistent messages
			prod.setDeliveryMode(DeliveryMode.PERSISTENT);

			TextMessage tm = sess.createTextMessage(pMessage);

			//send the message
			prod.send(tm);

			prod.close();
			sess.close();
			conn.close();

		} catch(JMSException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Receive a message from the ActiveMQ server, associated with the key
	 * @param pKey key to recover messages for
	 * @return message
	 */
	public static String receiveMessage(String pKey) {
		String message = null;
		
		try {

			ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(Settings.ACTIVEMQ_ADDRESS);

			Connection conn = amq.createConnection();
			conn.start();
			
			//conn.setExceptionListener(this);

			Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

			Destination dest = sess.createQueue(pKey);

			MessageConsumer cons = sess.createConsumer(dest);

			//for some reason receiveNoWait won't recover message that are stored
			//so we need to wait
			Message mess = cons.receive(1000);//we want the next message - wait

			//if null there is no message
			if(null!=mess) {
				if(mess instanceof TextMessage) {
					TextMessage tm = (TextMessage)mess;
					message = tm.getText();
				}
			}
			
			cons.close();
			sess.close();
			conn.close();

		} catch(JMSException e) {
			e.printStackTrace();
		}
		
		return message;
	}
	
	/**
	 * Delete the message queue from the ActiveMQ server
	 * (Note: this does not work at the moment)
	 * @param pKey Key for the queue to delete
	 */
	public static void deleteQueue(String pKey) {
		
		try {

			BrokerView bv = new BrokerService().getAdminView();
			bv.removeQueue(pKey);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Test main method for JMSComms
	 * @param args command line arguments
	 */
	public static void main(String[] args) {

		String key = args[0];
		System.out.println("Bleeding messages for key: "+key);
		String message = receiveMessage(key);
		while(message!=null) {
			System.out.println("Message received: "+receiveMessage(key));
			message = receiveMessage(key);
		} 
		
	}

}
