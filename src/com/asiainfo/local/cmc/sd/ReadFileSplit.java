package com.asiainfo.local.cmc.sd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

class ReadFileSplit extends InputSplit implements Writable {
	public String[] filesArray;
	private int length;

	public ReadFileSplit() {
	}

	public ReadFileSplit(String[] fileArray) {
		this.filesArray = fileArray;
		this.length = fileArray.length;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.length);
		for (String file : this.filesArray)
			Text.writeString(out, file);
	}

	public void readFields(DataInput in) throws IOException {
		this.length = in.readInt();
		this.filesArray = new String[this.length];
		for (int i = 0; i < this.length; i++)
			this.filesArray[i] = Text.readString(in);
	}

	public long getLength() {
		return 0L;
	}

	public String[] getLocations() {
		return new String[0];
	}
}