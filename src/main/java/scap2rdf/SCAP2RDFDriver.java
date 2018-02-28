package scap2rdf;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Option;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import iox.emf2rdf.RDFFormat;

public class SCAP2RDFDriver implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(SCAP2RDFDriver.class);

	@Option(names = "-i --input", required = false)
	private String input = "";

	@Option(names = "-o --output", required = false)
	private String output = "";

	@Option(names = "-ow --overwrite", required = false)
	private boolean overwrite;

	public SCAP2RDFDriver(String[] args) throws CmdLineException {
		super();
		CmdLineParser CLI = new CmdLineParser(this);
		try {
			CLI.parseArgument(args);
		} catch (CmdLineException e) {
			CLI.printUsage(System.out);
			throw e;
		}
		log.info(this.getClass().getName() + "==>");
	}

	public void run() {

		Configuration conf = new Configuration();
		final String HADOOP_HOME = "/usr/local/hadoop/etc/hadoop";
		conf.addResource(new Path(HADOOP_HOME + "/core-site.xml"));
		conf.addResource(new Path(HADOOP_HOME + "/hdfs-site.xml"));

		String scenarioText = null;

		try {
			Job job = Job.getInstance();
			job.setJarByClass(SCAP2RDFDriver.class);
			FileSystem fs = FileSystem.get(new java.net.URI("hdfs://localhost:9000/"), conf);
			FileStatus[] ffss = fs.listStatus(new Path("hdfs://libs/scap2rdf/lib"));
			for (FileStatus fs1 : ffss) {
				job.addArchiveToClassPath(fs1.getPath());
			}
			job.addCacheFile(new Path("hdfs://libs/scap2rdf/lib/arf2emf-0.0.1.jar").toUri());
			log.debug("scenarioText=" + scenarioText);
			Path pathRoot = new Path(fs.getUri());

			Path pathInput = new Path(pathRoot, input);
			log.info("pathInput=" + pathInput.toString());
			Path pathOutput = new Path(pathInput, output);
			log.info("pathOutput=" + pathOutput.toString());

			if (fs.exists(pathOutput)) {
				fs.delete(pathOutput, true);
			}
			job.setJarByClass(this.getClass());

			job.setMapperClass(SCAP2RDFMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, pathInput);
			FileOutputFormat.setOutputPath(job, pathOutput);

			job.setInputFormatClass(WholeFileInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.getConfiguration().set("mapred.child.java.opts", "-Xmx512m");
			job.waitForCompletion(true);
		} catch (IOException e) {
			log.error("", e.fillInStackTrace());
		} catch (NullPointerException e) {
			log.error("", e.fillInStackTrace());
		} catch (Exception e) {
			log.error("", e.fillInStackTrace());
		}
	}

	private static RDFFormat rdfFormat = RDFFormat.NTRIPLES;

	static Properties props = new Properties();

	public static void main(String[] args) throws Exception {
		try {
			SCAP2RDFDriver app = new SCAP2RDFDriver(args);
			app.run();
		} catch (CmdLineException e) {
			log.error("", e.fillInStackTrace());
		}
	}

	static class SCAP2RDFMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		Text textOut = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			try {
//				StringReader reader = new StringReader(value.toString());
//				EObject eObject = Deserialize.it(reader, "http://arf.xml");
//
//				Repository repo = new SailRepository(new MemoryStore());
//				repo.initialize();
//
//				ResourceSet resourceSet = Registrar.getResourceSet();
//				resourceSet.getURIConverter().getURIHandlers().add(0, new RepositoryHandler(repo));
//				Resource resource = resourceSet.createResource(URI.createURI("file:///arf.rdf"));
//				DocumentRoot root = (DocumentRoot) eObject;
//				AssetReportCollectionType coll = root.getAssetReportCollection();
//				resource.getContents().add(coll);
//				ByteArrayOutputStream stream = new ByteArrayOutputStream();
//				resource.save(stream, Collections.EMPTY_MAP);
//				resource.getContents().clear();
//				textOut.set(stream.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}

			ctx.write(NullWritable.get(), value);
		}
	}
}