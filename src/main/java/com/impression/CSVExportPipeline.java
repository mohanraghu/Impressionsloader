/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impression;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options: --project=
 * <YOUR_PROJECT_ID> --stagingLocation=
 * <STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */
public class CSVExportPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(CSVPipeline.class);

	public static void main(String[] args) {

		System.out.println("CSV Loader triggerred ..");
		for (int i = 0; i < args.length; i++)
			System.out.println("args " + args[i]);

		PipelineOptionsFactory.register(MyOptions.class);
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().as(MyOptions.class);
		Pipeline p = Pipeline.create(options);
		String BUCKET_NAME = "gs://client1_incoming/" + "Impressions*";
		String OUTGOING_BUCKET_NAME = "gs://client1_outgoing";
	
	    p.apply(TextIO.Read.from(BUCKET_NAME))   // Read input.
         .apply(new CountWords())               // Do some processing.
         .apply(TextIO.Write.to(OUTGOING_BUCKET_NAME));   // Write output.
 	 
		p.run();
		 
	}

	
	
}
