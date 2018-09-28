package com.wangx.itemcf;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserGroup extends WritableComparator {

	public UserGroup() {
		super(PairWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		PairWritable o1 = (PairWritable) a;
		PairWritable o2 = (PairWritable) b;
		return o1.getUid().compareTo(o2.getUid());
	}
}
