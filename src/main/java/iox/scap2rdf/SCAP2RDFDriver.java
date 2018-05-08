package iox.scap2rdf;

import java.io.FileNotFoundException;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import iox.emf2rdf.RDFFormat;

public class SCAP2RDFDriver implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(SCAP2RDFDriver.class);

	static final String PROPERTIES_FILE = "scap2rdf.properties";
	static String HADOOP_CONF;
	static String HDFS_FILE_SYSTEM_URL;

	@Option(name = "-i", aliases = "--input", required = true, usage = "hdfs Path to input. file or dir")
	private String input;

	@Option(name = "-o", aliases = "--output", required = true, usage = "hdfs Path to output. dir only")
	private String output;

	@Option(name = "-ow", aliases = "--write", required = false, usage = "Overwrite output")
	private boolean overwrite;

	@Option(name = "-c", aliases = "--conf", required = false, usage = "Location of hadooop config file.")
	private boolean hdfs_conf;

	@Option(name = "-f", aliases = "--fs", required = false, usage = "URL of hadoop name server.")
	private boolean hdfs_file_system_url;

	static Properties props = new Properties();

	public SCAP2RDFDriver(String[] args) throws CmdLineException, FileNotFoundException {
		super();
		CmdLineParser CLI = new CmdLineParser(this);
		try {
//			InputStream is = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE);
//			props.load(is);
//			List<String> list = Arrays.asList(args);
//			final PropertiesConsumer _function = new PropertiesConsumer(list);
//			props.forEach(_function);
//			args = list.toArray(new String[0]);
			CLI.parseArgument(args);
		} catch (CmdLineException e) {
			CLI.printUsage(System.out);
			throw e;
		} catch (IllegalStateException e) {
			log.error("", e);
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
			FileSystem fs = FileSystem.get(new java.net.URI("hdfs://haz00:9000"),
					conf);
			Path pathLibs = new Path("/libs/scap2rdf/lib");
//			if(!fs.exists(pathLibs)) {
//				fs.create(pathLibs);
//			}
			FileStatus[] ffss = fs.listStatus(pathLibs);
			for (FileStatus fs1 : ffss) {
				job.addArchiveToClassPath(fs1.getPath());
			}
			Path pathRoot = new Path(fs.getUri());

			Path pathInput = new Path(pathRoot, input);
			log.info("pathInput=" + pathInput.toString());
			Path pathOutput = new Path(pathInput, output);
			log.info("pathOutput=" + pathOutput.toString());

			if (fs.exists(pathOutput)) {
				fs.delete(pathOutput, overwrite);
			}

			job.setMapperClass(SCAP2RDFMapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(SCAP2RDFReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, pathInput);
			FileOutputFormat.setOutputPath(job, pathOutput);

			job.setInputFormatClass(XmlInputFormat.class);

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

	public static void main(String[] args) throws Exception {
		try {
			SCAP2RDFDriver app = new SCAP2RDFDriver(args);
			app.run();
		} catch (CmdLineException e) {
			log.error("", e.fillInStackTrace());
		}
	}

	String[] toArray(Properties props, String[] args) {
		List<String> list = Arrays.asList(args);
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			list.add((String) entry.getKey());
			String s = (String) entry.getValue();
			if (s != null && s.length() > 0) {
				list.add((String) entry.getValue());
			}
		}
		return list.toArray(new String[0]);
	}

//	class PropertiesConsumer implements BiConsumer<String, String> {
//
//		PropertiesConsumer(List<String> list) {
//			this.list = list;
//		}
//
//		@Override
//		public void accept(final String key, String value) {
//			list.add(key);
//			if (value != null) {
//				list.add(value);
//			}
//		}
//	}
}