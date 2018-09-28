package com.wangx.hbase_demo;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseDemo {

	Admin admin;
	HTable hTable;

	String TN = "phone";

	Random r = new Random();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	@Before
	public void begin() throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
		conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		Connection connection = ConnectionFactory.createConnection(conf);
		admin = connection.getAdmin();
//		hTable = new HTable(connection, connection.getTableBuilder(TableName.valueOf(TN), null), null, null, null);

	}

	@After
	public void end() throws Exception {
		if (admin != null) {
			try {
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void createTbl() throws Exception {
		if (admin.tableExists(TableName.valueOf(TN))) {
			admin.disableTable(TableName.valueOf(TN));
			admin.deleteTable(TableName.valueOf(TN));
		}
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
		HColumnDescriptor family = new HColumnDescriptor("cf1");
		family.setBlockCacheEnabled(true);
		family.setInMemory(true);
		family.setMaxVersions(1);
		desc.addFamily(family);
		admin.createTable(desc);
	}

	@Test
	public void insert() throws Exception {
		// 手机号_时间戳
		String rowkey = "1845454_20180927121212";
		Put put = new Put(rowkey.getBytes());
		put.addColumn("cf1".getBytes(), "type".getBytes(), "1".getBytes());
		put.addColumn("cf1".getBytes(), "time".getBytes(), "2016".getBytes());
		put.addColumn("cf1".getBytes(), "pnum".getBytes(), "17712341234".getBytes());
		hTable.put(put);
	}

	@Test
	public void get() throws Exception {
		// 手机号_时间戳
		String rowkey = "1845454_20180927121212";
		Get get = new Get(rowkey.getBytes());
		get.addColumn("cf1".getBytes(), "type".getBytes());
		get.addColumn("cf1".getBytes(), "time".getBytes());
		Result rs = hTable.get(get);
		Cell cell = rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
		System.out.println(new String(CellUtil.cloneValue(cell)));
	}

	/**
	 * 随机生成号码
	 * 
	 * @param prefix 手机号 前缀 eq：186，170
	 * @return
	 */
	public String getPhoneNum(String prefix) {
		return prefix + String.format("%08d", r.nextInt(99999999));
	}

	public String getDate(String year) {
		return year + String.format("%02d%02d%02d%02d%02d", new Object[] { r.nextInt(12) + 1, r.nextInt(30) + 1,
				r.nextInt(60) + 1, r.nextInt(60) + 1, r.nextInt(60) + 1 });
	}

	/**
	 * 插入10个手机号 100条通话记录 满足查询 时间做降序排序
	 * 
	 */
	@Test
	public void insertDB() throws Exception {
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 10; i++) {
			String rowkey;
			String phoneNum = getPhoneNum("186");
			for (int j = 0; j < 100; j++) {
				String phoneDate = getDate("2018");

				long dateLong = sdf.parse(phoneDate).getTime();
				rowkey = phoneNum + "_" + (Long.MAX_VALUE - dateLong);
				Put put = new Put(rowkey.getBytes());
				put.addColumn("cf1".getBytes(), "type".getBytes(), (r.nextInt(2) + "").getBytes());
				put.addColumn("cf1".getBytes(), "time".getBytes(), (phoneDate).getBytes());
				put.addColumn("cf1".getBytes(), "pnum".getBytes(), (getPhoneNum("170").getBytes()));
				puts.add(put);
			}
		}
		hTable.put(puts);
	}

	/**
	 * 查询某个手机号 某个月份下的所有童话的详单
	 */
	@Test
	public void scanDB() throws Exception {
		// 18682415935 2018年二月份的通过详单
		Scan scan = new Scan();
		String startRowKey = "18682415935" + (Long.MAX_VALUE - sdf.parse("20180401000000").getTime());
		scan.setStartRow(startRowKey.getBytes());
		String stopRowKey = "18682415935" + (Long.MAX_VALUE - sdf.parse("20180301000000").getTime());
		scan.setStopRow(stopRowKey.getBytes());
		ResultScanner rss = hTable.getScanner(scan);
		for (Result result : rss) {
			Cell cell1 = result.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
			System.out.println(new String(CellUtil.cloneValue(cell1)));
			Cell cell2 = result.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
			System.out.println(new String(CellUtil.cloneValue(cell2)));
			Cell cell3 = result.getColumnLatestCell("cf1".getBytes(), "pnum".getBytes());
			System.out.println(new String(CellUtil.cloneValue(cell3)));
		}
	}

	/**
	 * 某个手机号 所有type=0的通话详单
	 */
	@Test
	public void scanDB2() throws Exception {
		// 18699732127
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		PrefixFilter prefixFilter = new PrefixFilter("18699732127".getBytes());
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("cf1".getBytes(),
				"type".getBytes(), CompareOperator.EQUAL, "0".getBytes());
		list.addFilter(prefixFilter);
		list.addFilter(singleColumnValueFilter);
		Scan scan = new Scan();
		scan.setFilter(list);
		ResultScanner rss = hTable.getScanner(scan);
		for (Result result : rss) {
			System.out.println(new String(result.getRow()));
			Cell cell1 = result.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
			System.out.println(new String(CellUtil.cloneValue(cell1)));
			Cell cell2 = result.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
			System.out.println(new String(CellUtil.cloneValue(cell2)));
			Cell cell3 = result.getColumnLatestCell("cf1".getBytes(), "pnum".getBytes());
			System.out.println(new String(CellUtil.cloneValue(cell3)));
		}
	}

}
