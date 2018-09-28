package com.wangx.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//分组 保证同一个年月
public class TQGroup extends WritableComparator {

	public TQGroup() {
		super(Weather.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Weather w1 = (Weather) a;
		Weather w2 = (Weather) b;
		int y1 = Integer.compare(w1.getYear(), w2.getYear());
		if (y1 == 0) {
			int m1 = Integer.compare(w1.getMonth(), w2.getMonth());
			return m1;
		}
		return y1;
	}
}
