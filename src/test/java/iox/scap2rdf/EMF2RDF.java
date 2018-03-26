package iox.scap2rdf;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import gov.nist.scap.schema.asset.reporting.format._1.AssetReportCollectionType;
import gov.nist.scap.schema.asset.reporting.format._1.DocumentRoot;
import iox.emf2rdf.handlers.RepositoryHandler;
import iox.emf2rdf.map.Extensions;
import iox.emf2rdf.resource.NTResourceFactory;
import iox.sds4emf.Deserialize;
import iox.sds4emf.Registrar;

public class EMF2RDF implements Runnable {

	private static final Logger log = LogManager.getLogger(EMF2RDF.class);
	
	static {
		Registrar.associateExtension("xml", new gov.nist.scap.schema.asset.reporting.format._1.util._1ResourceFactoryImpl());
		Registrar.associateExtension("rdf", new NTResourceFactory());
		Registrar.registerPackage(gov.nist.scap.schema.asset.identification._1._1Package.eNS_URI,  gov.nist.scap.schema.asset.identification._1._1Package.eINSTANCE);
		Registrar.registerPackage(gov.nist.scap.schema.asset.reporting.format._1._1Package.eNS_URI,  gov.nist.scap.schema.asset.reporting.format._1._1Package.eINSTANCE);
		Registrar.registerPackage(gov.nist.scap.schema.reporting.core._1._1Package.eNS_URI,  gov.nist.scap.schema.reporting.core._1._1Package.eINSTANCE);
		Registrar.registerPackage(oasis.names.tc.ciq.xsdschema.xAL._2._0._0Package.eNS_URI,  oasis.names.tc.ciq.xsdschema.xAL._2._0._0Package.eINSTANCE);
		Registrar.registerPackage(oasis.names.tc.ciq.xsdschema.xNL._2._0._0Package.eNS_URI,  oasis.names.tc.ciq.xsdschema.xNL._2._0._0Package.eINSTANCE);
		Registrar.registerPackage(org.mitre.cpe.naming._2._2Package.eNS_URI,  org.mitre.cpe.naming._2._2Package.eINSTANCE);
		Registrar.registerPackage(org.w3.xml._1998.namespace.NamespacePackage.eNS_URI,  org.w3.xml._1998.namespace.NamespacePackage.eINSTANCE);
	}
	
	Model graph = new LinkedHashModel();
	SimpleValueFactory factory = SimpleValueFactory.getInstance();
	Extensions extensions = new Extensions();
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		
		String[] ss = getFile("arf.xml");
		try {
			StringReader reader = new StringReader(ss[1]);
			EObject eObject = Deserialize.it(reader, ss[0]);
			
			Repository repo = new SailRepository(new MemoryStore());
			repo.initialize();

			ResourceSet resourceSet = Registrar.getResourceSet();
			resourceSet.getURIConverter().getURIHandlers().add(0, new RepositoryHandler(repo));
			Resource resource = resourceSet.createResource(URI.createURI("file:///arf.rdf"));
			DocumentRoot root = (DocumentRoot)eObject;
			AssetReportCollectionType coll = root.getAssetReportCollection();
			resource.getContents().add(coll);
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			resource.save(stream, Collections.EMPTY_MAP);
			resource.getContents().clear();
			String rval = stream.toString();
			log.info(rval);
			Files.write(Paths.get("./out/arf.rdf"), rval.getBytes());	
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	  private String[] getFile(String resourceName) {
		  
		  	
			StringBuilder result = new StringBuilder("");

			//Get file from resources folder
			ClassLoader classLoader = getClass().getClassLoader();
			URL url = classLoader.getResource(resourceName);
			File file = new File(url.getFile());

			try (Scanner scanner = new Scanner(file)) {

				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					result.append(line).append("\n");
				}

				scanner.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
			String[] ss = {url.toString(), result.toString()};
			return ss;
		  }
	  
	public static void main(String[] args) {
		EMF2RDF app = new EMF2RDF();
		app.run();
	}
}
