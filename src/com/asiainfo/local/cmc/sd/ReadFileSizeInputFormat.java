package com.asiainfo.local.cmc.sd;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

class ReadFileSizeInputFormat extends InputFormat<IntWritable, Text> {
	public List<InputSplit> getSplits(JobContext jobContext) {
		List ret = new ArrayList();
		String[] fileArray = jobContext.getConfiguration().getStrings(
				"read_file_list");
		List groupedFile = makeGroup(fileArray);
		for (int i = 0; i < groupedFile.size(); i++) {
			ReadFileSplit rfs = new ReadFileSplit(
					((String) groupedFile.get(i)).split(","));
			System.out
					.println("[ReadFileSizeInputFormat][getSplits] get filearray from conf, filelist=["
							+ (String) groupedFile.get(i) + "]");
			ret.add(rfs);
		}
		return ret;
	}

	private List<String> makeGroup(String[] fileList) {
		List ret = new ArrayList();
		int groupNum = fileList.length > 10 ? 10 : fileList.length;
		for (int i = 0; i < groupNum; i++) {
			ret.add("");
		}
		for (int i = 0; i < fileList.length; i++) {
			int group = i % groupNum;
			String tmp = (String) ret.get(group);
			tmp = tmp + fileList[i] + ",";
			ret.set(group, tmp);
		}
		for (int i = 0; i < groupNum; i++) {
			String tmp = (String) ret.get(i);
			tmp = tmp.substring(0, tmp.length() - 1);
			ret.set(i, tmp);
		}
		return ret;
	}

	public RecordReader<IntWritable, Text> createRecordReader(
			InputSplit ignored, TaskAttemptContext taskContext)
			throws IOException {
		System.out
				.println("[ReadFileSizeInputFormat][getSplits] get filearray from conf, filelist=["
						+ ((ReadFileSplit) ignored).filesArray.toString() + "]");
		ReadFileReader rfr = new ReadFileReader();
		return rfr;
	}
}