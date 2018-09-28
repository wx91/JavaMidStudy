package com.wangx.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TQSort extends WritableComparator {

	public TQSort() {
		super(Weather.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Weather w1 = (Weather) a;
		Weather w2 = (Weather) b;
		int y1 = Integer.compare(w1.getYear(), w2.getYear());
		if (y1 == 0) {
			int m1 = Integer.compare(w1.getMonth(), w2.getMonth());
			if (m1 == 0) {
				// 温度为降序排序
				return -Integer.compare(w1.getWd(), w2.getWd());
			}
			return m1;
		}
		return y1;
	}

}
