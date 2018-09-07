package com.wangx.Solr;

import java.util.UUID;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.wangx.entity.User;

public class TestSolr2 {
	private static final String URL = "http://localhost:8983/solr/collection1";

	private SolrClient server;

	@Before
	public void init() {
		server = new HttpSolrClient.Builder(URL).build();
	}

	@Test
	public void testAddUser() {
		try {
			String prefix = "user_";
			User u1 = new User();
			u1.setId(prefix + UUID.randomUUID().toString().substring(4).substring(prefix.length()));
			u1.setName("张三");
			u1.setAge("25");
			u1.setSex("男");
			u1.setLike(new String[] { "足球", "篮球", "羽毛球" });
			this.server.addBean(u1);

			User u2 = new User();
			u2.setId(prefix + UUID.randomUUID().toString().substring(4).substring(prefix.length()));
			u2.setName("张四");
			u2.setAge("18");
			u2.setSex("女");
			u2.setLike(new String[] { "保龄球", "乒乓球", "羽毛球" });
			this.server.addBean(u2);
			this.server.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testChange() {
		try {
			SolrDocument doc = new SolrDocument();
			doc.addField("id", "123456");
			doc.setField("user_name", "名称");
			doc.setField("user_sex", "女");
			doc.setField("user_age", "18");
			doc.setField("user_like", new String[] { "music", "book", "sport" });
			User u = this.server.getBinder().getBean(User.class, doc);
			System.out.println(u);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSearchUser() {
		try {
			SolrQuery solrQuery = new SolrQuery();
			solrQuery.set("q", "user_name:张");
			QueryResponse response = this.server.query(solrQuery);
			SolrDocumentList solrList = response.getResults();
			long num = solrList.getNumFound();
			System.out.println("条目：" + num);
			for (SolrDocument sd : solrList) {
				User u = this.server.getBinder().getBean(User.class, sd);
				System.out.println(u.getId());
				System.out.println(u.getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@After
	public void destory() {
		server = null;
		// System.runFinalization();
		// System.gc();
	}

}
