package com.wangx.hdfs1031;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSTest {
	FileSystem fs;

	Configuration conf;

	@Before
	public void begin() throws Exception {
		// 加载src目录下的配置文件
		conf = new Configuration();
		fs = FileSystem.get(conf);
	}

	@Test
	public void mkdir() throws Exception {
		Path path = new Path("/tmp");
		fs.mkdirs(path);
	}

	@Test
	public void upload() throws Exception {
		Path path = new Path("/tmp/test");
		FSDataOutputStream outputStream = fs.create(path);
		FileUtils.copyFile(new File("e://test.txt"), outputStream);
	}

	@Test
	public void list() throws Exception {
		Path path = new Path("/tmp");
		FileStatus[] fss = fs.listStatus(path);
		for (FileStatus fileStatus : fss) {
			System.out.println("全路径：" + fileStatus.getPath());
			System.out.println("文件长度：" + fileStatus.getLen());
			System.out.println("访问时间戳：" + fileStatus.getAccessTime());
		}
	}

	@Test
	public void download() throws Exception {
		Path path = new Path("/tmp/test.txt");
		FSDataInputStream inputStream = fs.open(path);
		File file = new File("e://test5.txt");
		FileOutputStream outputStream = new FileOutputStream(file);
		byte[] buf = new byte[1024];
		while (inputStream.read(buf) != -1) {
			outputStream.write(buf);
		}
	}

	@Test
	public void deleteFile() throws Exception {
		Path path = new Path("/tmp/test.txt");
		fs.deleteOnExit(path);
	}

	@Test
	public void upload2() throws Exception {
		Path path = new Path("/tmp/seq");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
		File file = new File("d://test");
		for (File f : file.listFiles()) {
			writer.append(new Text(f.getName()), new Text(FileUtils.readFileToString(f)));
		}
	}

	@Test
	public void download2() throws Exception {
		Path path = new Path("/tmp/seq");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		Text key = new Text();
		Text value = new Text();
		while (reader.next(key, value)) {
			System.out.println(key);
			System.out.println(value);
			System.out.println("---------------------------");
		}
	}

	@After
	public void end() {
		try {
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
