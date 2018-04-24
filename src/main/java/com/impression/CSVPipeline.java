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

import com.google.api.services.bigquery.model.TableRow;

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
public class CSVPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(CSVPipeline.class);

	public static void main(String[] args) {

		System.out.println("CSV Loader triggerred ..");
		for (int i = 0; i < args.length; i++)
			System.out.println("args " + args[i]);

		PipelineOptionsFactory.register(MyOptions.class);
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().as(MyOptions.class);
		Pipeline p = Pipeline.create(options);
		String BUCKET_NAME = "gs://client1_incoming/" + "Impressions*";

		
		//Pipeline p1 = Pipeline.create(options);
		//PCollection<String> wlines = p1.apply(TextIO.read().from(BUCKET_NAME));
	    //wlines.apply(TextIO.Write.named("WriteMyFile").to("gs://client1_outgoing"));      		
		
		PCollection<String> lines = p.apply(TextIO.read().from(BUCKET_NAME));
		PCollection<TableRow> row = lines.apply(ParDo.of(new StringToRowConverter()));				
		row.apply(BigQueryIO.<TableRow> writeTableRows()
				.to("lyrical-epigram-201816:doubleclick_client1.impressions")
				.withWriteDisposition(WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(CreateDisposition.CREATE_NEVER));
				
		String BUCKET_CLICK = "gs://client1_incoming/" + "click*";
		PCollection<String> clines = p.apply(TextIO.read().from(BUCKET_CLICK));
		PCollection<TableRow> crow = clines.apply(ParDo.of(new ClickStringToRowConverter()));
		//crow.apply(TextIO.Write.to("gs://client1_outgoing/client1_click_dataexport"));
		crow.apply(BigQueryIO.<TableRow> writeTableRows()
				.to("lyrical-epigram-201816:doubleclick_client1.clicks")
				.withWriteDisposition(WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(CreateDisposition.CREATE_NEVER));

		String BUCKET_ACTIVITY = "gs://client1_incoming/" + "activity*";
		PCollection<String> alines = p.apply(TextIO.read().from(BUCKET_ACTIVITY));
		PCollection<TableRow> arow = alines.apply(ParDo.of(new ActStringToRowConverter()));
		//arow.apply(TextIO.Write.to("gs://client1_outgoing/client1_activity_dataexport"));
		arow.apply(BigQueryIO.<TableRow> writeTableRows()
				.to("lyrical-epigram-201816:doubleclick_client1.activity")
				.withWriteDisposition(WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(CreateDisposition.CREATE_NEVER));				
				
		p.run();
		 
	}

	// StringToRowConverter
	static class StringToRowConverter extends DoFn<String, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			String[] split = c.element().split(",");
			// c.output(new TableRow().set("",c.element()));
			TableRow row = new TableRow();
			row.set("eventdate", split[0]);
			row.set("impressionid", split[1]);
			row.set("click", split[2]);
			row.set("device", split[3]);
			row.set("advertiser", split[4]);
			row.set("campaign", split[5]);
			row.set("client", split[6]);
			c.output(row);
		}
	}
	
	// ClickStringToRowConverter
	static class ClickStringToRowConverter extends DoFn<String, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			String[] split = c.element().split(",");
			// c.output(new TableRow().set("",c.element()));
			TableRow row = new TableRow();
			row.set("click", split[0]);
			row.set("impression", split[1]);
			c.output(row);
		}
	}
	
	// ClickStringToRowConverter
	static class ActStringToRowConverter extends DoFn<String, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			String[] split = c.element().split(",");
			TableRow row = new TableRow();
			row.set("activity", split[0]);
			row.set("campaign", split[1]);
			c.output(row);
		}
	}
	
	
}
