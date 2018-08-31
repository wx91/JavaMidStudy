package com.wangx.mq;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.SessionAwareMessageListener;

import com.alibaba.fastjson.JSONObject;
import com.wangx.entity.Mail;
import com.wangx.service.MailService;

public class MailQueueMessageListener implements SessionAwareMessageListener<Message> {

	@Autowired
	private JmsTemplate jmsTemplate;
	@Autowired
	private Destination mailQueue;
	@Autowired
	private MailService mailService;

	public void onMessage(Message message, Session session) throws JMSException {
		TextMessage msg = (TextMessage) message;
		String ms = msg.getText();
		System.out.println("收到消息：" + ms);
		// 转成对应的对象
		Mail mail = JSONObject.parseObject(ms, Mail.class);
		if (mail==null) {
			return ;
		}
		try {
			
		} catch (Exception e) {
		}

	}

}
