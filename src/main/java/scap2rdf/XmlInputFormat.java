package scap2rdf;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XmlInputFormat extends TextInputFormat {

	private static final Logger log = LoggerFactory.getLogger(XmlInputFormat.class);

	public static final String START_TAG_KEY = "<asset-report-collection>";
	public static final String END_TAG_KEY = "</asset-report-collection>";

	/* Krishna - Creating XMLInputformat Class for reading XML File */
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		log.trace("createRecordReader");
		return new XmlRecordReader();
	}

	public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable key = new LongWritable();
		private Text value = new Text();

		@Override
		public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
			log.trace("initialize0");
			FileSplit fileSplit = (FileSplit) is;
			log.trace("initialize1");
			startTag = START_TAG_KEY.getBytes("utf-8");
			endTag = END_TAG_KEY.getBytes("utf-8");

			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			Path file = fileSplit.getPath();
			log.trace("initialize2 start=" + start + " path=" + file);

			FileSystem fs = file.getFileSystem(tac.getConfiguration());
			fsin = fs.open(fileSplit.getPath());
			fsin.seek(start);
			log.trace("initialize3");
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			log.trace("nextKeyValue");
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {

							value.set(buffer.getData(), 0, buffer.getLength());
							key.set(fsin.getPos());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			log.trace("nextKeyValue value=" + value);
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			log.trace("key=" + key.toString());
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			log.trace("value=" + value.toString());
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void close() throws IOException {
			log.trace("close");
			fsin.close();
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();

				if (b == -1)
					return false;

				if (withinBlock)
					buffer.write(b);

				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;

				if (!withinBlock && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}
	}
}
