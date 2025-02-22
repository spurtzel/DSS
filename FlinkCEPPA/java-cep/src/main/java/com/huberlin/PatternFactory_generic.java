package com.huberlin;

import com.google.common.collect.Collections2;
import com.huberlin.config.QueryInformation;
import com.huberlin.event.ComplexEvent;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;


public class PatternFactory_generic {
    static private final Logger log = LoggerFactory.getLogger(PatternFactory_generic.class);
    static private AtomicInteger timestamp_counter = new AtomicInteger(0);
    static private final ConcurrentMap<String, Integer> eventMatchCounts = new ConcurrentHashMap<>();
    static private final Map<String, SimpleEvent> trackedEventMap = new ConcurrentHashMap<>();

    static private String currentCutEventType = null;
    static private String finalPrefix = null;

    private static long queryCounter = 0;

    private static String computePrefix(String subquery) {
        int start = subquery.indexOf('(');
        int end = subquery.indexOf(')');
        if (start < 0 || end < 0 || end <= start) {
            return subquery;
        }
        String inside = subquery.substring(start + 1, end);
        return inside.replaceAll("[,\\s]", "");
    }

    public static List<DataStream<Event>> processQueries(List<QueryInformation.Processing> allQueries, 
                                                         DataStream<Event> inputStream) {
        List<DataStream<Event>> matchingStreams = new ArrayList<>();
        for (QueryInformation.Processing q : allQueries) {
            currentCutEventType = q.cut_event_type;
            if (!q.subqueries.isEmpty()) {
                finalPrefix = computePrefix(q.subqueries.get(0));
            }
            try {
                matchingStreams.add(processQuery(q, inputStream));
                queryCounter++;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return matchingStreams;
    }

    public static DataStream<Event> processQuery(QueryInformation.Processing query, 
                                                 DataStream<Event> inputStream) throws Exception {
        if (query.subqueries.isEmpty()) {
            throw new Exception("No subqueries defined");
        }
        DataStream<Event> output = processSubquery(0, query, inputStream);
        for (int i = 1; i < query.subqueries.size(); i++) {
           output = processSubquery(i, query, output.union(inputStream));
        }
        return output;
    }

    public static DataStream<Event> processSubquery(final int num_subquery, 
                                                    QueryInformation.Processing queryInfo, 
                                                    DataStream<Event> inputStream) {
        List<String> inputs = queryInfo.inputs.get(num_subquery);
        Collection<List<String>> inputPermutations = Collections2.permutations(inputs);
        List<DataStream<Event>> streams = new ArrayList<>(2);
        for (List<String> inputPerm : inputPermutations) {
            streams.add(generateStream(inputStream, inputPerm, queryInfo, num_subquery));
        }
        return streams.stream().reduce(DataStream::union).get();
    }

	public static DataStream<Event> generateStream(
			DataStream<Event> input,
			List<String> eventTypes,      
			QueryInformation.Processing q,
			final int num_subquery
	) {
		final String patternBaseName = "pattern_q" + queryCounter + "_sq" + num_subquery;

		Pattern<Event, Event> pattern =
			Pattern.<Event>begin(patternBaseName + "_0")
				   .where(checkEventType(eventTypes.get(0)));

		for (int i = 1; i < eventTypes.size(); i++) {
			final int thisIndex = i;
			final String prevName = patternBaseName + "_" + (i - 1);
			final String thisName = patternBaseName + "_" + i;
			final String thisEventType = eventTypes.get(i);

			pattern = pattern.followedByAny(thisName)
							 .where(checkEventType(thisEventType))
							 .where(new IterativeCondition<Event>() {
				@Override
				public boolean filter(Event newEvent, Context<Event> ctx) throws Exception {
					Map<String, Long> earliestTimestamps = new HashMap<>();

					for (int k = 0; k < thisIndex; k++) {
						String patternKey = patternBaseName + "_" + k;
						Iterable<Event> matchedSoFar = ctx.getEventsForPattern(patternKey);

						for (Event old : matchedSoFar) {
							updateEarliestTimestamps(old, earliestTimestamps);
						}
					}

					updateEarliestTimestamps(newEvent, earliestTimestamps);

					List<List<String>> constraints = q.sequence_constraints.get(num_subquery);
					for (List<String> singleConstraint : constraints) {
						String first = singleConstraint.get(0);   
						String second = singleConstraint.get(1);  

						Long tFirst = earliestTimestamps.get(first);
						Long tSecond = earliestTimestamps.get(second);
						if (tFirst != null && tSecond != null) {
							if (tFirst >= tSecond) {
								return false;
							}
						}
					}
					return true; 
				}
			});
		}

		PatternStream<Event> matchStream = CEP.pattern(input, pattern);

		DataStream<Event> outputStream = matchStream.select(new PatternSelectFunction<Event, Event>() {
			@Override
			public Event select(Map<String, List<Event>> match) {
				Set<String> addedEvents = new HashSet<>();
				ArrayList<SimpleEvent> newEventList = new ArrayList<>();

				for (int i = 0; i < eventTypes.size(); i++) {
					String key = patternBaseName + "_" + i;
					List<Event> matchedList = match.get(key);
					if (matchedList == null) continue;

					for (Event evt : matchedList) {
						for (SimpleEvent contained : evt.getContainedSimpleEvents()) {
							if (q.output_selection.contains(contained.getEventType())) {
								if (addedEvents.add(contained.getID())) {
									newEventList.add(contained);
								}
							}

							if (q.cut_event_type != null
								&& q.cut_event_type.equals(contained.getEventType())) {
								eventMatchCounts.merge(contained.getID(), 1, Integer::sum);
								trackedEventMap.putIfAbsent(contained.getID(), contained);
							}
						}
					}
				}

				long creation_time = (System.nanoTime() / 1000L);
				ComplexEvent new_complex_event =
					new ComplexEvent(creation_time, q.subqueries.get(num_subquery), newEventList);

				return new_complex_event;
			}
		});

		return outputStream;
	}


	private static void updateEarliestTimestamps(Event e, Map<String, Long> earliestTimestamps) {
		for (SimpleEvent se : e.getContainedSimpleEvents()) {
			String etype = se.getEventType();
			Long curr = earliestTimestamps.get(etype);
			if (curr == null || se.getTimestamp() < curr) {
				earliestTimestamps.put(etype, se.getTimestamp());
			}
		}
	}

    public static SimpleCondition<Event> checkEventType(String eventType) {
        return new SimpleCondition<Event>() {
            String latest_eventID = "";
            @Override
            public boolean filter(Event e) {
                String simp_eventID = e.getID();
                if (!simp_eventID.equals(latest_eventID)) {
                    latest_eventID = simp_eventID;
                    timestamp_counter.incrementAndGet();
                }
                return eventType.equals(e.getEventType());
            }
        };
    }

    private static boolean checkConstraints(Event e1, Event e2, Random rand,
                                            QueryInformation.Processing q, final int num_subquery) {
        List<List<String>> sequence_constraints = q.sequence_constraints.get(num_subquery);

        for (List<String> sequence_constraint : sequence_constraints) {
            String first_eventtype = sequence_constraint.get(0);
            String second_eventtype = sequence_constraint.get(1);
            if (e1.getTimestampOf(first_eventtype) != null &&
                e2.getTimestampOf(second_eventtype) != null &&
                e1.getTimestampOf(first_eventtype) >= e2.getTimestampOf(second_eventtype)) {
                return false;
            }
            if (e2.getTimestampOf(first_eventtype) != null &&
                e1.getTimestampOf(second_eventtype) != null &&
                e2.getTimestampOf(first_eventtype) >= e1.getTimestampOf(second_eventtype)) {
                return false;
            }
        }
        return true;
    }

    public static Event createComplexEvent(Event[] events, QueryInformation.Processing q, final int num_subquery) {
        Set<String> addedEvents = new HashSet<>();
        ArrayList<SimpleEvent> newEventList = new ArrayList<>();
        for (Event tmp : events) {
            for (SimpleEvent contained : tmp.getContainedSimpleEvents()) {
                if (q.output_selection.contains(contained.getEventType())) {
                    boolean it_was_new = addedEvents.add(contained.getID());
                    if (it_was_new) {
                        newEventList.add(contained);
                    }
                }
            }
        }
        long creation_time = (LocalTime.now().toNanoOfDay() / 1000L);
        return new ComplexEvent(creation_time, q.subqueries.get(num_subquery), newEventList);
    }

    public static DataStream<SimpleEvent> produceFinalTrackedEventsWithWindow(DataStream<Event> anyStream) {
        return anyStream
            .keyBy(e -> 0)
            .window(GlobalWindows.create())
            .trigger(new EndOfStreamTrigger(2000))
            .process(new FinalWindowProcessFunction())
            .name("ProduceFinalTrackedEventsWithWindow");
    }


    public static DataStream<SimpleEvent> produceSubqueryPartialAggregate(DataStream<Event> anyStream, long idleTimeout) {
        return anyStream
            .keyBy(e -> 0)
            .window(GlobalWindows.create())
            .trigger(new EndOfStreamTrigger(idleTimeout))
            .process(new SubqueryPartialAggregateFunction())
            .name("ProduceSubqueryPartialAggregate");
    }


    public static class EndOfStreamTrigger extends Trigger<Event, GlobalWindow> {
        private static final long serialVersionUID = 1L;
        private final long idleTimeout;
        private final ValueStateDescriptor<Long> lastSeenTimeDescriptor =
            new ValueStateDescriptor<>("lastSeenTime", Long.class);

        public EndOfStreamTrigger(long idleTimeout) {
            this.idleTimeout = idleTimeout;
        }

        @Override
        public TriggerResult onElement(Event element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            if (element.getEventType().contains("end-of-stream")) {
                return TriggerResult.FIRE_AND_PURGE;
            }
            ValueState<Long> lastSeen = ctx.getPartitionedState(lastSeenTimeDescriptor);
            long currentProcessingTime = ctx.getCurrentProcessingTime();
            lastSeen.update(currentProcessingTime);
            ctx.registerProcessingTimeTimer(currentProcessingTime + idleTimeout);
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            ValueState<Long> lastSeen = ctx.getPartitionedState(lastSeenTimeDescriptor);
            Long lastTime = lastSeen.value();
            if (lastTime != null && time >= lastTime + idleTimeout) {
                return TriggerResult.FIRE_AND_PURGE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            ValueState<Long> lastSeen = ctx.getPartitionedState(lastSeenTimeDescriptor);
            lastSeen.clear();
        }
    }

	public static class SubqueryPartialAggregateFunction
			extends ProcessWindowFunction<Event, SimpleEvent, Integer, GlobalWindow> {
		@Override
		public void process(Integer key, Context context, Iterable<Event> elements, Collector<SimpleEvent> out) {
			Map<String, PartialAggregate> aggregates = new HashMap<>();
			for (Event e : elements) {
				for (SimpleEvent se : e.getContainedSimpleEvents()) {
					if (currentCutEventType != null && currentCutEventType.equals(se.getEventType())) {
						PartialAggregate agg = aggregates.get(se.getID());
						if (agg == null) {
							agg = new PartialAggregate(se.getTimestamp(), se.getCount());
							aggregates.put(se.getID(), agg);
						} else {
							agg.count += se.getCount();
						}
					}
				}
			}
			String newType = finalPrefix + "_" + currentCutEventType;
			for (Map.Entry<String, PartialAggregate> entry : aggregates.entrySet()) {
				String eventID = entry.getKey();
				PartialAggregate agg = entry.getValue();
				List<String> attrs = new ArrayList<>();
				SimpleEvent partialAggregateEvent = new SimpleEvent(
						eventID,  
						agg.timestamp,  
						newType,
						agg.count,
						attrs);
				out.collect(partialAggregateEvent);
				log.info("SubqueryPartialAggregateFunction: Emitted partial aggregate for event ID " + eventID +
						 " with count " + agg.count + " and timestamp " + agg.timestamp + LocalTime.now());
			}
		}

		private static class PartialAggregate {
			long timestamp;
			long count;
			PartialAggregate(long timestamp, long count) {
				this.timestamp = timestamp;
				this.count = count;
			}
		}
	}


	public static class FinalWindowProcessFunction extends ProcessWindowFunction<Event, SimpleEvent, Integer, GlobalWindow> {
		@Override
		public void process(Integer key, Context context, Iterable<Event> elements, Collector<SimpleEvent> out) {
			long aggregateSum = 0;
			for (Event match : elements) {
				long product = 1;
				for (SimpleEvent se : match.getContainedSimpleEvents()) {
					product *= se.getCount();
				}
				aggregateSum += product;
			}
			log.info("FinalWindowProcessFunction: Emitted final aggregate value: " + aggregateSum + " current time: " + LocalTime.now());
		}
	}

    public static void printEventMatchCounts() {
        System.out.println("=== Final Event Match Counts ===");
        eventMatchCounts.forEach((eventID, count) ->
            System.out.println("Event ID = " + eventID + " -> " + count + " matches"));
    }

    static {
        System.out.println("PatternFactory_generic loaded, registering shutdown hook.");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Job shutting down. Printing aggregate counts:");
            printEventMatchCounts();
        }));
    }
}
