package com.wangx.fof;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Friend implements WritableComparable<Friend> {

	private String friend1;

	private String friend2;
	// 亲密度
	private int hot;

	public String getFriend1() {
		return friend1;
	}

	public void setFriend1(String friend1) {
		this.friend1 = friend1;
	}

	public String getFriend2() {
		return friend2;
	}

	public void setFriend2(String friend2) {
		this.friend2 = friend2;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(friend1);
		out.writeUTF(friend2);
		out.writeInt(hot);

	}

	public void readFields(DataInput in) throws IOException {
		this.friend1 = in.readUTF();
		this.friend2 = in.readUTF();
		this.hot = in.readInt();
	}

	public int compareTo(Friend f) {
		int c = this.friend1.compareTo(f.getFriend1());
		if (c == 0) {
			return Integer.compare(this.hot, f.getHot());
		}
		return c;
	}

}
