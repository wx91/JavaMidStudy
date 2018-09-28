package com.wangx.itemcf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PairWritable implements WritableComparable<PairWritable> {

	private String uid;

	private double num;

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public double getNum() {
		return num;
	}

	public void setNum(double num) {
		this.num = num;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(uid);
		out.writeDouble(num);
	}

	public void readFields(DataInput in) throws IOException {
		this.uid = in.readUTF();
		this.num = in.readDouble();

	}

	public int compareTo(PairWritable o) {
		int r = this.uid.compareTo(o.getUid());
		if (r == 0) {
			return Double.compare(this.num, o.getNum());
		}
		return r;
	}

}
