package com.wangx.hive1109;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Lower extends UDF {

	public Text evaluate(Text s) {
		if (s == null) {
			return null;
		}
		String str = s.toString();
		str = str.substring(1, 1) + "***" + str.substring(str.length() - 1, str.length());
		return new Text(str.toString().toLowerCase());
	}

}
