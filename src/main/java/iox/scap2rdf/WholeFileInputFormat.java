package iox.scap2rdf;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
	
	private static final Logger log = LoggerFactory.getLogger(WholeFileInputFormat.class);

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		log.trace("set isplitable");
		return false;
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		log.trace("createRecordReader==>0");
		WholeFileRecordReader reader = new WholeFileRecordReader();
		log.trace("createRecordReader==>1");
		reader.initialize(split, context);
		log.trace("createRecordReader==>2");
		return reader;
	}
}