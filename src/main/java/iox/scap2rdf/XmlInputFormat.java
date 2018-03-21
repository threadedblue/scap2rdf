package iox.scap2rdf;

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

	public static final String START_TAG_KEY = "<arf:asset-report-collection xmlns:arf=\"http://scap.nist.gov/schema/asset-reporting-format/1.1\" xmlns:core=\"http://scap.nist.gov/schema/reporting-core/1.1\" xmlns:ai=\"http://scap.nist.gov/schema/asset-identification/1.1\">";
	public static final String END_TAG_KEY = "</arf:asset-report-collection>";

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
			log.debug("nextKeyValue");
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					log.debug("readUntilMatch(startTag, false)");
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							log.debug("readUntilMatch(startTag, true)");
							value.set(buffer.getData(), 0, buffer.getLength());
							log.trace("readUntilMatch value=" + value);
							key.set(fsin.getPos());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			log.debug("nextKeyValue value=" + value.toString().substring(0, 20));
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
			log.trace("while match=" + new String(match));
			while (true) {
				int b = fsin.read();

				if (b == -1)
					return false;

				if (withinBlock) {
//					log.trace("withinBlock b=" + b);
					buffer.write(b);
				}

				if (b == match[i]) {
					log.trace("while b=" + b + " match[" + i + "]=" + Byte.toString(match[i]));
					i++;
					if (i >= match.length) {
						log.trace("return true i=" + i);
						return true;
					}
				} else {
					log.trace("reset b=" + b + " match[" + i + "]=" + Byte.toString(match[i]));
					i = 0;
				}

				if (!withinBlock && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}
	}
}
