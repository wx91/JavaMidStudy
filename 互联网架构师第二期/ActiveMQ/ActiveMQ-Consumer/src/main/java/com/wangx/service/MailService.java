package com.wangx.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.wangx.entity.Mail;

public class MailService {

	@Autowired
	private JavaMailSender mailSender;
	@Autowired
	private SimpleMailMessage simpleMailMessage;
	@Autowired
	private ThreadPoolTaskExecutor threadPool;

	public void mailSend(final Mail mail) {
		threadPool.execute(new Runnable() {
			public void run() {
				try {
					simpleMailMessage.setFrom(simpleMailMessage.getFrom());
					simpleMailMessage.setTo(mail.getTo());
					simpleMailMessage.setSubject(mail.getSubject());
					simpleMailMessage.setText(mail.getContent());
//					mailSender.send(simpleMailMessage);
				} catch (MailException e) {
					e.printStackTrace();
					throw e;
				}
			}
		});
	}

}
