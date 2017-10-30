package io.eventador;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;

import org.apache.flink.streaming.api.watermark.Watermark;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.String;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import redis.clients.jedis.*;

import org.apache.flink.api.common.functions.FilterFunction;

public class Planes {
    // Redis instance containing FAA data for Tail Num/Equipment mapping
    //static private String REDIS_HOST = "192.168.99.1";
    static private String REDIS_HOST = "192.168.1.50";
    static private Integer REDIS_PORT = 6379;
    static final JedisPoolConfig poolConfig = buildPoolConfig();
    static JedisPool jedisPool = new JedisPool(poolConfig, REDIS_HOST);
    static final Integer MAX_FLIGHT_DELAY = 10; // Max seconds ADS-B events may arrive out of order

    public static final String STATE_NAME = "faa";

	public static void main(String[] args) throws Exception {
		/* get properties file, eg. /tmp/planes.properties
            topic: defaultsink
            bootstrap.servers: XXXXXXXX-kafka0.va.eventador.io:9092
            auto.offset.reset: earliest
        */
		ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

		// create streaming environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// enable event time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

		// enable fault-tolerance, 60s checkpointing
		env.enableCheckpointing(60000);

		// enable restarts
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50, 500L));
		env.setStateBackend(new RocksDBStateBackend("file:///tmp/rocks_state_store"));

		Properties kParams = params.getProperties();
		kParams.setProperty("group.id", UUID.randomUUID().toString());
		//kParams.setProperty("group.id", "FLINK_KAFKA_GROUP");
		DataStream<ObjectNode> inputStream = env.addSource(new FlinkKafkaConsumer010<>(params.getRequired("topic"), new JSONDeserializationSchema(), kParams)).name("Kafka 0.10 Source");

		DataStream<PlaneModel> planes = inputStream
                                          .assignTimestampsAndWatermarks(new PlaneTimestampExtractor())
                                          .keyBy(jsonNode -> jsonNode.get("icao").textValue())
                                          .map(new PlaneMapper())
                                          .name("Timestamp -> KeyBy ICAO -> Map");

        DataStream<Tuple2<Integer,PlaneModel>> keyedplanes = planes
                                                   .keyBy("icao")
                                                   .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                                                   .apply(new PlaneWindow())
                                                   .name("Tumbling Window");

        // Print plane stream to stdout
        keyedplanes.print();

		DataStream<Tuple2<Integer,PlaneModel>> military_planes = keyedplanes.filter(new MilitaryPlaneFilter())
                                              .name("Military Plane Filter");
		military_planes.print();

        String app_name = String.format("Streaming Planes <- Kafka Topic: %s", params.getRequired("topic"));
		env.execute(app_name);

	}

	public static class PlaneTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<ObjectNode> {

		public PlaneTimestampExtractor() {
			super(Time.seconds(MAX_FLIGHT_DELAY));
		}

		@Override
		public long extractTimestamp(ObjectNode json) {
			long ts = json.get("timestamp").asLong() * 1000;
		    return ts;
		}
	}

    private static JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }

    private static class PlaneMapper extends RichMapFunction<ObjectNode, PlaneModel> {

        private transient ValueState<PlaneModel> state;

        @Override
        public PlaneModel map(ObjectNode planejson) {
            PlaneModel plane = new PlaneModel();

            PlaneModel currentState = new PlaneModel();

			// access the state value
			try {
				currentState = state.value();
			} catch (IOException e) {
				System.out.println("oh no ioexception");
			}

            if (currentState != null) {
                // Load whats in state cache
                plane.equipment = currentState.equipment;
                plane.tail = currentState.tail;
                plane.flight = currentState.flight;
                plane.timestamp_verbose = currentState.timestamp_verbose;
                plane.timestamp = currentState.timestamp;
                plane.altitude = currentState.altitude;
                plane.counter = currentState.counter;
                plane.icao = currentState.icao;
                plane.lat = currentState.lat;
                plane.lon = currentState.lon;
                plane.speed = currentState.speed;
            } else {
                System.out.println("State was null, new state entry");
            }

            // get new values from JSON, these should always be in the payload
			plane.msg_type = planejson.get("msg_type").asInt();
			plane.timestamp_verbose = planejson.get("timestamp_verbose").textValue();
			plane.timestamp = planejson.get("timestamp").asLong();
			plane.icao = planejson.get("icao").textValue();
			plane.counter = planejson.get("counter").asLong();

            switch (plane.msg_type) {
			    case 1: plane.flight = planejson.get("flight").textValue();
                        break;
                case 3: plane.altitude = planejson.get("altitude").asInt();
                        plane.lat = planejson.get("lat").asDouble();
                        plane.lon = planejson.get("lon").asDouble();
                        break;
                case 4: plane.speed = planejson.get("speed").asInt();
                        break;
                case 5: plane.altitude = planejson.get("altitude").asInt();
                        break;
                case 7: plane.altitude = planejson.get("altitude").asInt();
                        break;
            }

            // at this point, decide if we need to look for the plane data
            // if we've already cached NotAvail, then skip it
            // if it's Unknown, try a look from Redis
            if (plane.equipment == "Unknown" || plane.equipment == null) {
                try (Jedis jedis = jedisPool.getResource()) {
                    Map<String, String> plane_record = jedis.hgetAll(plane.icao);
                    plane.tail = plane_record.get("tailnumber");
                    plane.equipment = plane_record.get("equipment");
                }
            }

            // Update state record
            try {
                state.update(plane);
            } catch (IOException e) {
                System.out.printf("Unable to update state for icao %s", plane.icao);
            }

            //System.out.println("MAPPED RECORD: " + plane.toString());
            return plane;
        }

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<PlaneModel> descriptor =
					new ValueStateDescriptor<>(
							STATE_NAME, // the state store name
							TypeInformation.of(new TypeHint<PlaneModel>() {}), // type information
                            new PlaneModel());
            descriptor.setQueryable(STATE_NAME); // Use this name when querying state
			state = getRuntimeContext().getState(descriptor);
		}

    }

    private static class ErikSerSchema implements SerializationSchema<Tuple3<Long, String, Long>> {
        @Override
        public byte[] serialize(Tuple3<Long, String, Long> tuple3) {
            return (tuple3.f0.toString() + " - " + tuple3.toString()).getBytes();
        }
    }

	private static class MilitaryPlaneFilter implements FilterFunction<Tuple2<Integer,PlaneModel>> {

		@Override
		public boolean filter(Tuple2<Integer,PlaneModel> planeTuple) throws Exception {
			return planeTuple.f1.isMilitary();
		}
	}

    // Window functions
    public static class PlaneWindow implements WindowFunction<PlaneModel, Tuple2<Integer, PlaneModel>, Tuple, TimeWindow> {
		@Override
		public void apply(Tuple key, TimeWindow window, Iterable<PlaneModel> airplanes, Collector<Tuple2<Integer, PlaneModel>> collector) throws Exception {
            Integer count = 0;
            PlaneModel last_message = new PlaneModel();
            for (PlaneModel in: airplanes) {
                count++;
                // System.out.println("Updating count for " + in.icao + ", now at " + count);
                last_message = in;
            }
			collector.collect(new Tuple2<Integer, PlaneModel>(count, last_message));
		}
	}

}
