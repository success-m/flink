package malla;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.util.Date;

public class StreamingJobDataStreamAPIExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("file:///home/smalla/Downloads/sample_dataset.csv");
        data.map(new MapFunction<String, Tuple3<String,Integer,Timestamp>>() {
                    @Override
                    public Tuple3<String, Integer, Timestamp> map(String line) throws Exception {
                        String[] cells = line.split(",");
                        return new Tuple3<String, Integer, Timestamp>(cells[2], 1, (new Timestamp((new Date()).getTime()) ) );
                    }
                }).assignTimestampsAndWatermarks(
                    WatermarkStrategy.forMonotonousTimestamps() )
                .keyBy(v->v.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple3<String, Integer, Timestamp>>() {
                    @Override
                    public Tuple3<String, Integer, Timestamp> reduce(Tuple3<String, Integer, Timestamp> t2, Tuple3<String, Integer, Timestamp> t1) throws Exception {
                        return new Tuple3<String, Integer, Timestamp>(t1.f0,t1.f1 + t2.f1, t1.f2);
                    }
                })
                .print();

        env.execute("Data Stream Example");
    }
}