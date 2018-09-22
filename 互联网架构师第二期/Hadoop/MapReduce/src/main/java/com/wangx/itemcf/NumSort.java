package com.wangx.itemcf;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NumSort extends WritableComparator {
	public NumSort() {
		super(PairWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		PairWritable o1 = (PairWritable) a;
		PairWritable o2 = (PairWritable) b;
		int r = o1.getUid().compareTo(o2.getUid());
		if (r == 0) {
			return -Double.compare(o1.getNum(), o2.getNum());
		}
		return r;
	}
}
