package org.morning.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建执行环境 -- 一个本地的模拟环境  == 注意这里切换了 执行环境的模式 == 变成了流式的处理环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 从文件中读取数据   按行读取，存储的数据就是每行的文本
        DataStreamSource<String> streamSource = environment.readTextFile("input/wc.txt");

//        streamSource.flatMap((string,out)->{
//            String[] words = string.split(" ");
//            for (String word : words) {
//                out.collect(Tuple2.of(word,1L));
//            }
//        }).keyBy(data -> data.f0)
//                .sum(1);
//        // 这种才是正确的lambda写法，参数的类型也是需要指定的
//        SingleOutputStreamOperator<Tuple2<String, Long>> sum = streamSource
//                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
//                    String[] words = line.split(" ");
//                    for (String word : words) {
//                        out.collect(Tuple2.of(word,1L));
//                    }
//                })
//                .keyBy(data -> data.f0)
//                .sum(1);


        // 3. 转换、分组、求和，得到统计结果
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {

                        String[] words = line.split(" ");

                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                }).keyBy(data -> data.f0)
                .sum(1);
        sum.print();
        environment.execute();

    }
}
