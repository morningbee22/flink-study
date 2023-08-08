package org.morning.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 *  这种方法实际上是 Flink本身并不推荐的
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建执行环境 -- 一个本地的模拟环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        //2. 从文件中读取数据   按行读取，存储的数据就是每行的文本
        DataSource<String> lineDataSource = environment.readTextFile("input/wc.txt");

        //3. 数据格式的转换
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {  // 这里可以使用函数式编程
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);
        sum.print();
    }
}
