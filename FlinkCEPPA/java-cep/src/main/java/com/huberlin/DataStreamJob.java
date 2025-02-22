package com.huberlin;

import com.huberlin.communication.OldSourceFunction;
import com.huberlin.config.JSONQueryParser;
import com.huberlin.config.QueryInformation;
import com.huberlin.event.SimpleEvent;
import com.huberlin.event.ComplexEvent;
import com.huberlin.event.Event;
import org.apache.commons.cli.*;

import java.util.List;
import java.util.Optional;

import com.huberlin.communication.TCPEventSender;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.*;

import org.apache.flink.core.fs.FileSystem;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.TimeCharacteristic;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamJob {
    final static private Logger log = LoggerFactory.getLogger(DataStreamJob.class);

    private static CommandLine parse_cmdline_args(String[] args) {
        final Options cmdline_opts = new Options();
        final HelpFormatter formatter = new HelpFormatter();
        cmdline_opts.addOption(new Option("localconfig", true, "Path to the local configuration file"));
        cmdline_opts.addOption(new Option("globalconfig", true, "Path to the global configuration file"));
        cmdline_opts.addOption(new Option("flinkconfig", true, "Path to the directory with the flink configuration"));
        final CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(cmdline_opts, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("java -jar cep-node.jar", cmdline_opts);
            System.exit(1);
        }
        assert (false);
        return null;
    }

    private static boolean shouldRunFinalAggregator(QueryInformation config) {
        if (config.processing == null || config.processing.isEmpty()) return false;
        return config.processing.get(0).final_aggregagte_evaluation;
    }

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parse_cmdline_args(args);
        String filePath_local = cmd.getOptionValue("localconfig", "./conf/config.json");
        String filePath_global = cmd.getOptionValue("globalconfig", "./conf/address_book.json");
        QueryInformation config = JSONQueryParser.parseJsonFile(filePath_local, filePath_global);
        if (config != null) {
            System.out.println("Parsed JSON successfully");
        } else {
            log.error("Failed to parse JSON");
            System.exit(1);
        }
        final int REST_PORT = 8081 + config.forwarding.node_id * 2;
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(cmd.getOptionValue("flinkconfig", "conf"));
        flinkConfig.set(JobManagerOptions.RPC_BIND_PORT, 6123+config.forwarding.node_id);
        flinkConfig.set(JobManagerOptions.PORT, 6123+config.forwarding.node_id);
        flinkConfig.set(RestOptions.BIND_PORT, REST_PORT + "-" + (REST_PORT + 1));
        flinkConfig.set(RestOptions.PORT, REST_PORT);
        flinkConfig.set(TaskManagerOptions.RPC_BIND_PORT, 51000+config.forwarding.node_id );
        flinkConfig.set(TaskManagerOptions.RPC_PORT, "0");
        flinkConfig.set(BlobServerOptions.PORT, "0");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(flinkConfig);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FileSystem.initialize(flinkConfig);
        env.setParallelism(1);

        DataStream<Tuple2<Integer, Event>> inputStream = env.addSource(new OldSourceFunction(
                5500+config.forwarding.node_id
        ));

        SingleOutputStreamOperator<Tuple2<Integer, Event>> monitored_stream = inputStream.map(new RichMapFunction<Tuple2<Integer, Event>, Tuple2<Integer, Event>>() {
            private transient MetricsRecorder.MetricsWriter memory_usage_writer;
            private transient MetricsRecorder.MemoryUsageRecorder memory_usage_recorder;
            private transient Thread memory_usage_recorder_thread;

            @Override
            public void open(Configuration parameters) {
                memory_usage_writer = new MetricsRecorder.MetricsWriter("memory_usage_node_" + config.forwarding.node_id + ".csv");
                memory_usage_recorder = new MetricsRecorder.MemoryUsageRecorder(memory_usage_writer);
                memory_usage_recorder_thread = new Thread(memory_usage_recorder);
                memory_usage_recorder_thread.start();
            }

            @Override
            public Tuple2<Integer, Event> map(Tuple2<Integer, Event> event_with_source_node_id) {
                return event_with_source_node_id;
            }

            @Override
            public void close() {
                memory_usage_recorder_thread.interrupt();
                try {
                    memory_usage_recorder_thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).uid("metrics");

        List<DataStream<Event>> outputstreams_by_query = PatternFactory_generic.processQueries(
                config.processing,
                inputStream.map(tuple -> tuple.f1)
        );

        DataStream<Tuple2<Integer, Event>> outputStream;
        if (outputstreams_by_query.isEmpty()) {
            outputStream = inputStream;
        } else {
            DataStream<Event> union = outputstreams_by_query.stream().reduce(DataStream::union).get();
            outputStream = union.map(new MapFunction<Event, Tuple2<Integer, Event>>() {
                @Override
                public Tuple2<Integer, Event> map(Event e) {
                    return new Tuple2<>(config.forwarding.node_id, e);
                }
            }).union(inputStream);
        }

        DataStream<Tuple2<Integer, Event>> filteredOutputStream = outputStream.filter(new FilterFunction<Tuple2<Integer, Event>>() {
            @Override
            public boolean filter(Tuple2<Integer, Event> event_with_source_id) {
                Event e = event_with_source_id.f1;
                int source_node_id = event_with_source_id.f0;
                if (source_node_id == config.forwarding.node_id && !e.isSimple()){
                    assert (e instanceof ComplexEvent);
                    ComplexEvent ce = (ComplexEvent) e;
                }
                return true;
            }
        });

        DataStream<SimpleEvent> finalAggregates;
        
        if (shouldRunFinalAggregator(config)) {
            finalAggregates = PatternFactory_generic.produceFinalTrackedEventsWithWindow(
                    outputstreams_by_query.stream().reduce(DataStream::union).orElse(inputStream.map(tuple -> tuple.f1))
            );
        } else {
            finalAggregates = PatternFactory_generic.produceSubqueryPartialAggregate(
                    outputstreams_by_query.stream().reduce(DataStream::union).orElse(inputStream.map(tuple -> tuple.f1)), 1000
            );
        }
        DataStream<Tuple2<Integer, Event>> finalAggregatesAsTuples = finalAggregates.map(new MapFunction<SimpleEvent, Tuple2<Integer, Event>>() {
            @Override
            public Tuple2<Integer, Event> map(SimpleEvent e) {
                return new Tuple2<>(config.forwarding.node_id, e);
            }
        });

        DataStream<Tuple2<Integer, Event>> combinedOutputStream = filteredOutputStream.union(finalAggregatesAsTuples);

        combinedOutputStream.addSink(new TCPEventSender(config.forwarding.address_book,
                config.forwarding.table,
                config.forwarding.node_id));

        env.execute("Flink Java API Skeleton");
    }
}
