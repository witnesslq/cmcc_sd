package com.asiainfo.local.cmc.sd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class ReadFileSizeMapper extends
		Mapper<IntWritable, Text, IntWritable, NullWritable> {
	Configuration conf = null;

	protected void setup(Mapper.Context context) throws IOException,
			InterruptedException {
		this.conf = context.getConfiguration();
	}

	public void map(IntWritable key, Text value, Mapper.Context context)
			throws IOException, InterruptedException {
		System.out.println("[ReadFileSizeMapper][map] start handle file["
				+ value.toString() + "]");
		FileSystem fs = FileSystem.newInstance(this.conf);
		Path path = new Path(value.toString());
		FileStatus fstatus = fs.getFileStatus(path);
		long fileSize = fstatus.getLen();
		long fileLine = 0L;

		FSDataInputStream fsis = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(fsis));
		String line = null;
		while ((line = br.readLine()) != null) {
			fileLine += 1L;
		}

		context.getCounter("file_LineNum_Group", path.getName() + "_size")
				.setValue(fileSize);
		context.getCounter("file_LineNum_Group", path.getName() + "_line")
				.setValue(fileLine);
	}
}