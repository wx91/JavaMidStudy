package com.wangx.pb;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.wangx.pb.Consumer2.Listener;

public class Consumer3 {
	private ConnectionFactory factory;
	private Connection connection;
	private Session session;
	private MessageConsumer consumer;

	public Consumer3() {
		try {
			factory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER,
					ActiveMQConnection.DEFAULT_PASSWORD, "tcp://localhost:61616");
			connection = factory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void receive() throws Exception {
		Destination destination = session.createTopic("topic1");
		consumer = session.createConsumer(destination);
		consumer.setMessageListener(new Listener());
	}

	class Listener implements MessageListener {
		public void onMessage(Message message) {
			try {
				if (message instanceof TextMessage) {
					TextMessage msg = (TextMessage) message;
					System.out.println("c3收到消息：——————————————————");
					System.out.println(msg.getText());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
