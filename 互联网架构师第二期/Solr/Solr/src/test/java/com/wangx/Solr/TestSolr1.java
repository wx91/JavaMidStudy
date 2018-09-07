package com.wangx.Solr;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class TestSolr1 {

	private static final String URL = "http://localhost:8983/solr/collection1";

	@Test
	public void testAdd() throws Exception {
		// 实例化Solr对象
		SolrClient solrServer = new HttpSolrClient.Builder(URL).build();
		// 实例化添加数据类
		SolrInputDocument doc1 = new SolrInputDocument();
		doc1.setField("id", "1001");
		doc1.setField("name", "iphone6S手机");
		doc1.setField("price", "6000");
		doc1.setField("url", "/images/001.jpg");

		SolrInputDocument doc2 = new SolrInputDocument();
		doc2.setField("id", "1002");
		doc2.setField("name", "三星s6手机");
		doc2.setField("price", "5300");
		doc2.setField("url", "/images/002.jpg");

		// 设置服务器保存信息并提交
		solrServer.add(doc1);
		solrServer.add(doc2);
		solrServer.commit();
	}

	@Test
	public void testSerach() throws Exception {
		// 实例化Solr对象
		SolrClient solrServer = new HttpSolrClient.Builder(URL).build();
		// 查询类
		SolrQuery solrQuery = new SolrQuery();
		// 查询关键词
		solrQuery.set("q", "name:手机");
		// 查询数据
		QueryResponse response = solrServer.query(solrQuery);
		// 取数据
		SolrDocumentList solrList = response.getResults();
		long num = solrList.getNumFound();
		System.out.println("条数：" + num);
		for (SolrDocument sd : solrList) {
			String id = (String) sd.get("id");
			List<String> name = (List<String>) sd.get("name");
			List<Float> price = (List<Float>) sd.get("price");
			List<String> url = (List<String>) sd.get("url");
			System.out.println("id:" + id);
			System.out.println("name:" + name);
			System.out.println("price:" + price);
			System.out.println("url:" + url);
		}
	}

	@Test
	public void testDel() throws Exception {
		SolrClient solrServer = new HttpSolrClient.Builder(URL).build();
		solrServer.deleteById("1");
		List<String> ids = new ArrayList<String>();
		ids.add("1001");
		ids.add("1002");
		solrServer.deleteById(ids);
		solrServer.deleteByQuery("id:1001 id:1002");
		solrServer.commit();
	}

}
