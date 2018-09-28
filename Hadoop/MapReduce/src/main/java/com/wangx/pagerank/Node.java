package com.wangx.pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

public class Node {
	// 字符串 第一个元素 初始PR值为1
	private double pageRank;
	// 字符串 后面的节点列表 数组
	private String[] adjacentNodeNames;

	// PR值于数组的分隔符 \t
	public static final char fieldSeparator = '\t';

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public void setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
	}

	public boolean containsAdjacentNodes() {
		return adjacentNodeNames != null && adjacentNodeNames.length > 0;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);
		if (getAdjacentNodeNames() != null) {
			sb.append(fieldSeparator).append(StringUtils.join(getAdjacentNodeNames(), fieldSeparator));
		}
		return sb.toString();
	}

	// value = 1.0 B D
	public static Node fromMR(String value) throws IOException {
		String[] parts = StringUtils.splitPreserveAllTokens(value, fieldSeparator);
		if (parts.length < 1) {
			throw new IOException("Excepted 1 or more parts but received" + parts.length);
		}
		Node node = new Node();
		node.setPageRank(Double.valueOf(parts[0]));
		if (parts.length > 1) {
			node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
		}
		return node;
	}

}
