package iox.scap2rdf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Collections;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nist.scap.schema.asset.reporting.format._1.AssetReportCollectionType;
import gov.nist.scap.schema.asset.reporting.format._1.DocumentRoot;
import iox.emf2rdf.RDFFormat;
import iox.emf2rdf.handlers.RepositoryHandler;
import iox.emf2rdf.resource.NTResourceFactory;
import iox.emf2rdf.resource.RDFResourceFactory;
import iox.emf2rdf.resource.TTLResourceFactory;
import iox.sds4emf.Registrar;

public class SCAP2RDFMapper extends Mapper<Text, BytesWritable, NullWritable, Text> {

	private static final Logger log = LoggerFactory.getLogger(SCAP2RDFMapper.class);

	Text textOut = new Text();

	@Override
	protected void setup(Mapper<Text, BytesWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		String outputFormat = context.getConfiguration().get("RDFFromat");
		log.trace("Registration==>");
		Registrar.associateExtension("xml",
				new gov.nist.scap.schema.asset.reporting.format._1.util._1ResourceFactoryImpl());

		if (RDFFormat.RDFXML.name().equals(outputFormat)) {
			log.debug("Using rdf format=" + RDFFormat.RDFXML.name());
			Registrar.associateExtension("rdf", new RDFResourceFactory());
		} else if (RDFFormat.TURTLE.name().equals(outputFormat)) {
			log.debug("Using rdf format=" + RDFFormat.TURTLE.name());
			Registrar.associateExtension("rdf", new TTLResourceFactory());
		} else {
			log.debug("Using rdf format=" + RDFFormat.NTRIPLES.name());
			Registrar.associateExtension("rdf", new NTResourceFactory());
		}

		Registrar.registerPackage(gov.nist.scap.schema.asset.identification._1._1Package.eNS_URI,
				gov.nist.scap.schema.asset.identification._1._1Package.eINSTANCE);
		Registrar.registerPackage(gov.nist.scap.schema.asset.reporting.format._1._1Package.eNS_URI,
				gov.nist.scap.schema.asset.reporting.format._1._1Package.eINSTANCE);
		Registrar.registerPackage(gov.nist.scap.schema.reporting.core._1._1Package.eNS_URI,
				gov.nist.scap.schema.reporting.core._1._1Package.eINSTANCE);
		Registrar.registerPackage(oasis.names.tc.ciq.xsdschema.xAL._2._0._0Package.eNS_URI,
				oasis.names.tc.ciq.xsdschema.xAL._2._0._0Package.eINSTANCE);
		Registrar.registerPackage(oasis.names.tc.ciq.xsdschema.xNL._2._0._0Package.eNS_URI,
				oasis.names.tc.ciq.xsdschema.xNL._2._0._0Package.eINSTANCE);
		Registrar.registerPackage(org.mitre.cpe.naming._2._2Package.eNS_URI,
				org.mitre.cpe.naming._2._2Package.eINSTANCE);
		Registrar.registerPackage(org.w3.xml._1998.namespace.NamespacePackage.eNS_URI,
				org.w3.xml._1998.namespace.NamespacePackage.eINSTANCE);
		log.trace("<==Registration");
	}

	@Override
	protected void map(Text key, BytesWritable value, Context ctx) throws IOException, InterruptedException {
		log.debug("map==>");
		String textIn = new String(value.getBytes());
		log.debug("key=" + key + " value=" + textIn.substring(0, 20));
		try {
			StringReader reader = new StringReader(textIn);

			URI uri = URI.createURI(key.toString());
			Charset charset = Charset.forName("US-ASCII");
			Resource resource = Registrar.getResourceSet().createResource(uri);
			resource.load(new ReaderInputStream(reader, charset), Collections.EMPTY_MAP);
			log.trace("0 resource=" + resource);
			EObject eObject = (EObject) resource.getContents().get(0);
			log.trace("1 eObject=" + eObject);

			// EObject eObject = Deserialize.it(reader, "http://" + key.toString());

			Repository repo = new SailRepository(new MemoryStore());
			repo.initialize();

			// ResourceSet resourceSet = Registrar.getResourceSet();
			Registrar.getResourceSet().getURIConverter().getURIHandlers().add(0, new RepositoryHandler(repo));
			Resource resource1 = Registrar.getResourceSet().createResource(URI.createURI("file:///arf.rdf"));
			log.trace("2 resource1=" + resource1);
			DocumentRoot root = (DocumentRoot) eObject;
			log.trace("3 root=" + root);
			AssetReportCollectionType coll = root.getAssetReportCollection();
			resource1.getContents().add(coll);
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			resource1.save(stream, Collections.EMPTY_MAP);
			resource1.getContents().clear();
			textOut.set(stream.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		ctx.write(NullWritable.get(), textOut);
	}
}