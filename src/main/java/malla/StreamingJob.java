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

package malla;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//		env.readTextFile("file:///home/smalla/Downloads/sample_dataset.csv");
//		DataStream<String> dataStream = env.readTextFile("file:///home/smalla/Downloads/test.txt");

		tEnv.executeSql("create TEMPORARY table products (" +
				"sn int," +
				"time_ref bigint," +
				"account string," +
				"code string," +
				"country_code string," +
				"product_type string," +
				"dvalue double," +
				"status string )" +
				"with (" +
				"'connector' = 'filesystem'," +
				"'path' = 'file:///home/smalla/Downloads/sample_dataset.csv'," +
				"'format' = 'csv'," +
				"'csv.ignore-parse-errors' = 'true'" +
				")");

		//total number of imports and exports
		tEnv.executeSql("select account, count(1) as cc " +
				"from products where sn is not null " +
				"group by account")
				.print();

		//product type wise import or export
		tEnv.executeSql("select product_type,account, count(1) as cc " +
				"from products where sn is not null " +
				"group by account,product_type")
				.print();

		//country, product type import and export
		tEnv.executeSql("select product_type,account,country_code, count(1) as cc " +
				"from products where sn is not null " +
				"group by account,product_type,country_code")
				.print();

		// unfortunately file sink is not available for table API :(
		// could not persist
	}
}
