package com.huangys.flink.map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import scala.Tuple2;


public class UserPreLogMapFunction extends RichMapFunction<Tuple2<String,String>,String> {
    private ValueState<String> logState;

    @Override
    public void open(Configuration conf){
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("pre string", String.class);
        logState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public String map(Tuple2<String,String> value) throws Exception {
        String preLog = logState.value();
        logState.update(value._2);
        return preLog;
    }
}
