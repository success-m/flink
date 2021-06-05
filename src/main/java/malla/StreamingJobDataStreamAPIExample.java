package malla;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJobDataStreamAPIExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("file:///home/smalla/Downloads/sample_dataset.csv");
        data.map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String line) throws Exception {
                        String[] cells = line.split(",");
                        return new Tuple2<String, Integer>(cells[2], 1);
                    }
                })
                .keyBy(v->v.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<String,Integer>(t1.f0,t1.f1 + t2.f1);
                    }
                })
                .print();

        env.execute("Data Stream Example");
    }
}