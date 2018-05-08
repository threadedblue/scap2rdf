package iox.scap2rdf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
	
	private static final Logger log = LoggerFactory.getLogger(WholeFileRecordReader.class);

	private FileSplit fileSplit;
	private Configuration conf;
	private byte[] value;
	private boolean processed = false;

	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		log.trace("initialize in whole record reader");
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			log.trace("next key value");
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				log.trace("Read==>");
				IOUtils.readFully(in, contents, 0, contents.length);
				log.trace("<==Read");
				value = contents;
				log.trace("value=" + new String(value).substring(0, 20));
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text(this.fileSplit.getPath().getName());
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return new BytesWritable(value);
	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}
}