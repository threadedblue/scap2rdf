package iox.scap2rdf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SCAP2RDFReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	
	private static final Logger log = LoggerFactory.getLogger(SCAP2RDFReducer.class);
	
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values, Context ctx)
			throws IOException, InterruptedException {
		log.debug("reduce==>");
		int count = 0;
		for (Text value : values) {
			log.trace("count=" + count++);
			ctx.write(null, value);
		}
	}
}
