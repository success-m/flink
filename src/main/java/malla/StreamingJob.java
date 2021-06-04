package malla;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
				"status string," +
				"ts as LOCALTIMESTAMP," +
				"WATERMARK FOR ts AS ts - INTERVAL '10' SECOND)" +
				"with (" +
				"'connector' = 'filesystem'," +
				"'path' = 'file:///home/smalla/Downloads/sample_dataset.csv'," +
				"'format' = 'csv'," +
				"'csv.ignore-parse-errors' = 'true'" +
				")");

//		tEnv.executeSql("select TUMBLE_START(ts, INTERVAL '0.5' SECOND), count(1) as c " +
//				"from products p where sn is not null " +
//				"group by TUMBLE(ts, INTERVAL '0.5' SECOND)")
//				.print();

		tEnv.executeSql("create table result1 (" +
				"sn int," +
				"ts timestamp," +
				"account string," +
				"total bigint," +
				"PRIMARY KEY (sn) NOT ENFORCED" +
				")" +
				"with (" +
				"'connector' = 'jdbc'," +
				"'url' = 'jdbc:mysql://localhost:3306/flink?username=smalla&password=password'," +
				"'table-name' = 'result1'" +
				")");
		// IMPORTANT: set a unique primary key. Otherwise the output will be less as the records would replace themselves.

		//total number of imports and exports
		tEnv.executeSql("insert into result1 select max(sn) as sn,TUMBLE_START(ts, INTERVAL '0.5' SECOND) as ts, account, count(1) as cc " +
				"from products where sn is not null " +
				"group by TUMBLE(ts, INTERVAL '0.5' SECOND),account");
		// insert overwrite didnt work.
		// adding unique key in mysql table worked with insert into

		tEnv.executeSql("create table result2 (" +
				"sn int," +
				"ts timestamp," +
				"account string," +
				"product_type string," +
				"total bigint," +
				"PRIMARY KEY (sn) NOT ENFORCED" +
				")" +
				"with (" +
				"'connector' = 'jdbc'," +
				"'url' = 'jdbc:mysql://localhost:3306/flink?username=smalla&password=password'," +
				"'table-name' = 'result2'" +
				")");

		//total number of imports and exports
		tEnv.executeSql("insert into result2 select max(sn) as sn, TUMBLE_START(ts, INTERVAL '0.5' SECOND) as ts, account,product_type, count(1) as cc " +
				"from products where sn is not null " +
				"group by TUMBLE(ts, INTERVAL '0.5' SECOND),account,product_type");

		tEnv.executeSql("create table result3 (" +
				"sn int," +
				"ts timestamp," +
				"account string," +
				"product_type string," +
				"country_code string," +
				"total bigint," +
				"PRIMARY KEY (sn) NOT ENFORCED" +
				")" +
				"with (" +
				"'connector' = 'jdbc'," +
				"'url' = 'jdbc:mysql://localhost:3306/flink?username=smalla&password=password'," +
				"'table-name' = 'result3'" +
				")");

		//total number of imports and exports
		tEnv.executeSql("insert into result3 select max(sn) as sn, TUMBLE_START(ts, INTERVAL '0.5' SECOND) as ts, account,product_type,country_code, count(1) as cc " +
				"from products where sn is not null " +
				"group by TUMBLE(ts, INTERVAL '0.5' SECOND),account,product_type, country_code");

		// unfortunately file sink is not available for table API when used with aggregate functions.
		// Plain selects work perfectly but not useful for analysis. :(
		// could not persist in file
	}
}
