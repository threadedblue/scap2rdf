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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import iox.emf2rdf.RDFFormat;

public class SCAP2RDFDriver implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(SCAP2RDFDriver.class);

	@Option(name = "-i", aliases = "--input", required = false, usage = "")
	private String input = "";

	@Option(name = "-o", aliases = "--output", required = false, usage = "")
	private String output = "";

	@Option(name = "-ow", aliases = "--write", required = false, usage = "")
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
		final String HADOOP_CONF = "/usr/local/hadoop/etc/hadoop";
		conf.addResource(new Path(HADOOP_CONF + "/core-site.xml"));
		conf.addResource(new Path(HADOOP_CONF + "/hdfs-site.xml"));
		conf.set("xmlinput.start", "");
		conf.set("xmlinput.end", "");
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		try {
			Job job = Job.getInstance();
			job.setJarByClass(SCAP2RDFDriver.class);
			FileSystem fs = FileSystem.get(new java.net.URI("hdfs://haz00.us-east4-a.c.bold-rain-193317.internal:9000"),
					conf);
			FileStatus[] ffss = fs.listStatus(new Path("/libs/scap2rdf/lib"));
			for (FileStatus fs1 : ffss) {
				job.addArchiveToClassPath(fs1.getPath());
			}
			Path pathRoot = new Path(fs.getUri());

			Path pathInput = new Path(pathRoot, input);
			log.info("pathInput=" + pathInput.toString());
			Path pathOutput = new Path(pathInput, output);
			log.info("pathOutput=" + pathOutput.toString());

			if (fs.exists(pathOutput)) {
				fs.delete(pathOutput, true);
			}

			job.setMapperClass(SCAP2RDFMapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(SCAP2RDFReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, pathInput);
			FileOutputFormat.setOutputPath(job, pathOutput);

			// job.setInputFormatClass(WholeFileInputFormat.class);
			job.setInputFormatClass(XmlInputFormat.class);
			// job.setOutputFormatClass(TextOutputFormat.class);

			job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
			log.debug("waitForCompletion==>");
			job.waitForCompletion(true);
			log.debug("<==waitForCompletion");
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
}