package com.wangx.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveDemo {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) {
		try {
			Class.forName(driverName);
			Connection connection = DriverManager.getConnection("jdbc:hive2://node3:10000/default", "hive", "");
			Statement stmt = connection.createStatement();
			String tableName = "testHiveDriverTable";
			stmt.execute("drop table if exists " + tableName);
			stmt.execute("create table " + tableName + "(key int, value string )");
			// show table
			String sql = "show table '" + tableName + "'";
			System.out.println("Running：" + sql);
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1) + "-" + res.getString("name"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
