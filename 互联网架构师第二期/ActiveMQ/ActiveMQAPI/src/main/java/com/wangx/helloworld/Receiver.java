package com.wangx.helloworld;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Receiver {
	public static void main(String[] args) throws Exception {
		// 第一步：建立ConnectionFactory工厂对象，需要填入用户名、密码以及要连接的地址，均使用默认即可，默认端口为"tcp://localhost:61616"
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_USER,
				ActiveMQConnectionFactory.DEFAULT_PASSWORD, ActiveMQConnectionFactory.DEFAULT_BROKER_URL);
		// 第二步：通过ConnectionFactory工厂对象我们创建一个Connection对象，并且调用Connection
		Connection connection = connectionFactory.createConnection();
		// start方法开启连接，Connection默认是关闭的。
		connection.start();
		// 第三步：通过Connection对象创建Session会话（上下文环境对象），用于接收消息，参加配置1为是否开启事务，参数配置2为签收模式，一般我们设置为自动签收。
		Session session = connection.createSession(Boolean.FALSE, Session.CLIENT_ACKNOWLEDGE);
		// 第四步：通过Session创建Destination对象，指的是一个客户端用来指定生产消息目标和消费消息来源的对象，在PTP模式中，Destination被称作Queue即队列；在Pub/Sub模式。Destination被称作Topic即主题。在程序中可以使用多个Queue和Topic。
		Destination destination = session.createQueue("queue1");
		// 第五步：我们需要通过Session对象创建消息的发送和接收对象（生产者和消费者）Message Producer/Message Consumer。
		MessageConsumer messageConsumer = session.createConsumer(destination);
		// 第六步:最后我们使用JMS规范的TextMessage形式创建数据（通过Session对象），并用MessageProducer的send方法发送数据。同理客户端使用receive方法进行接收数据。最后不要忘记关闭Connection连接。
		while (true) {
			TextMessage msg = (TextMessage) messageConsumer.receive();
			if (msg == null) {
				break;
			}
			System.out.println("收到的内容：" + msg.getText());
		}

		if (connection != null) {
			connection.close();
		}

	}
}
