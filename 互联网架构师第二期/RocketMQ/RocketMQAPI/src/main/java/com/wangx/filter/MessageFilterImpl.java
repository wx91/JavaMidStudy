package com.wangx.filter;

import com.alibaba.rocketmq.common.filter.FilterContext;
import com.alibaba.rocketmq.common.filter.MessageFilter;
import com.alibaba.rocketmq.common.message.MessageExt;

public class MessageFilterImpl implements MessageFilter {

	public boolean match(MessageExt msg, FilterContext context) {
		// 尽量遵循贵方，使用getUserProperty做属性过滤
		String property = msg.getUserProperty("SequenceId");
		if (property != null) {
			int id = Integer.parseInt(property);
			// if ((id % 3) == 0 && (id > 10)) {
			if ((id % 2) == 0) {
				return true;
			}
		}
		return false;
	}

}
