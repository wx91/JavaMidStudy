package com.wangx.tq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Weather implements WritableComparable<Weather> {

	private int year;
	private int month;
	private int day;
	// 温度
	private int wd;

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(wd);
	}

	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.wd = in.readInt();
	}

	public int compareTo(Weather w) {
		int y1 = Integer.compare(this.year, w.getYear());
		if (y1 == 0) {
			int m1 = Integer.compare(this.month, w.getMonth());
			if (m1 == 0) {
				return Integer.compare(this.wd, w.getWd());
			}
			return m1;
		}
		return y1;
	}
}
