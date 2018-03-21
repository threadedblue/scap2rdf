package scap2rdf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SCAP2RDFMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private static final Logger log = LoggerFactory.getLogger(SCAP2RDFMapper.class);

	Text textOut = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
		log.debug("map==>");
		log.debug("key=" + key + " value=" + value.toString().substring(0, 20));
		try {
//			StringReader reader = new StringReader(value.toString());
//			EObject eObject = Deserialize.it(reader, "http://arf.xml");
//
//			Repository repo = new SailRepository(new MemoryStore());
//			repo.initialize();
//
//			ResourceSet resourceSet = Registrar.getResourceSet();
//			resourceSet.getURIConverter().getURIHandlers().add(0, new RepositoryHandler(repo));
//			Resource resource = resourceSet.createResource(URI.createURI("file:///arf.rdf"));
//			DocumentRoot root = (DocumentRoot) eObject;
//			AssetReportCollectionType coll = root.getAssetReportCollection();
//			resource.getContents().add(coll);
//			ByteArrayOutputStream stream = new ByteArrayOutputStream();
//			resource.save(stream, Collections.EMPTY_MAP);
//			resource.getContents().clear();
//			textOut.set(stream.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		ctx.write(NullWritable.get(), value);
	}
}