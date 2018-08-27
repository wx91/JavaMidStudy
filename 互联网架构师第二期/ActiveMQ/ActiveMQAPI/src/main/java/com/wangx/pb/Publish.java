package com.wangx.pb;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Publish {
	private ConnectionFactory factory;
	private Connection connection;
	private Session session;
	private MessageProducer producer;

	public Publish() {
		try {
			factory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER,
					ActiveMQConnection.DEFAULT_PASSWORD, "tcp://localhost:61616");
			connection = factory.createConnection();
			connection.start();
			session = connection.createSession(false, session.AUTO_ACKNOWLEDGE);
			producer = session.createProducer(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sendMessage() throws Exception {
		Destination destination = session.createTopic("topic1");
		TextMessage textMessage = session.createTextMessage("我是内容");
		producer.send(destination, textMessage);
	}

	public static void main(String[] args) throws Exception {
		Publish p = new Publish();
		p.sendMessage();
	}
}
