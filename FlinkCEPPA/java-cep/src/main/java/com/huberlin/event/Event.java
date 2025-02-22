
package com.huberlin.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.lang.*;

public abstract class Event {

    private static final Logger log = LoggerFactory.getLogger(Event.class);
    boolean is_simple;
    public String eventType;


    public boolean isSimple() {
        return this.is_simple;
    }

    public String getEventType() {
        return this.eventType;
    }

    abstract public ArrayList<SimpleEvent> getContainedSimpleEvents();

    /**
     * Get the timestamp used for watermarking
     *
     * @return the timestamp
     */
    abstract public long getTimestamp();

    abstract public long getHighestTimestamp();

    abstract public long getLowestTimestamp();

    abstract public String getEventIdOf(String event_type);

    abstract public Long getTimestampOf(String event_type);

    abstract public String getID();

    abstract public SimpleEvent getEventOfType(String event_type);


    public abstract String toString();


    public static Event parse(String received) {
        String[] receivedParts = received.split("\\|");
        if (receivedParts[0].trim().equals("simple")) {
            ArrayList<String> attributeList = new ArrayList<>();
            if (receivedParts.length > 5)
                attributeList.addAll(Arrays.asList(receivedParts).subList(5, receivedParts.length));

            for (int i = attributeList.size() - 1; i >= 0; i--) {
                String attribute = attributeList.get(i);
                String cleanedAttribute = attribute.trim();

                if (cleanedAttribute.isEmpty()) {
                    attributeList.remove(i);
                } else {
                    attributeList.set(i, cleanedAttribute);
                }
            }
            
            return new SimpleEvent(
                    receivedParts[1].trim(),                               
                    parseTimestamp(receivedParts[2].trim()),               
                    receivedParts[3].trim(),                               
                    Long.parseLong(receivedParts[4].trim()),               
                    attributeList);                                        
        } else if (receivedParts[0].trim().equals("complex")) {
            long timestamp = parseTimestamp(receivedParts[1].trim());
            String eventType = receivedParts[2].trim();
            int numberOfEvents = Integer.parseInt(receivedParts[3].trim());             
            ArrayList<SimpleEvent> eventList = parse_eventlist(receivedParts[5].trim());
            return new ComplexEvent(timestamp, eventType, eventList);
        } else {
            throw new IllegalArgumentException("Received message has wrong type: " + receivedParts[0].trim());
        }
    }

    /**
     * Parse an event list from the serialization format
     *
     * @param event_list transfer encoded primitive-event-list string
     * @return event list as java List of Simple Events
     */
    static ArrayList<SimpleEvent> parse_eventlist(String event_list) {
        String[] seperateEvents = event_list.split(";");
        int numberOfEvents = seperateEvents.length;
        ArrayList<SimpleEvent> events = new ArrayList<>(numberOfEvents);

        for (String event : seperateEvents) {
            event = event.trim(); 
            event = event.substring(1, event.length() - 1); 
            String[] eventParts = event.split(",");  

            List<String> attributeList = new ArrayList<>();
            for (int i = 3; i < eventParts.length; i++) {
                attributeList.add(eventParts[i].trim());
            }


            events.add(new SimpleEvent(
                    eventParts[1].trim(),
                    parseTimestamp(eventParts[0].trim()),
                    eventParts[2].trim(),
                    Long.parseLong(eventParts[3].trim()),
                    attributeList));
        }
        return events;
    }


    final public static DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss:SSSSSS"); 

    public static String formatTimestamp(long timestamp) {
        LocalTime ts = LocalTime.ofNanoOfDay(timestamp * 1000L);
        return TIMESTAMP_FORMATTER.format(ts);
    }

    public static long parseTimestamp2(String serialized_timestamp) {
        LocalTime ts = LocalTime.parse(serialized_timestamp, TIMESTAMP_FORMATTER);
        return ts.toNanoOfDay() / 1000L;
    }

    public static long parseTimestamp(String hhmmssususus) {
        String[] hoursMinutesSecondsMicroseconds = hhmmssususus.split(":");
        long resulting_timestamp = 0;
        assert (hoursMinutesSecondsMicroseconds.length >= 3);

        resulting_timestamp += Long.parseLong(hoursMinutesSecondsMicroseconds[0], 10) * 60 * 60 * 1_000_000L;
        resulting_timestamp += Long.parseLong(hoursMinutesSecondsMicroseconds[1], 10) * 60 * 1_000_000L;
        resulting_timestamp += Long.parseLong(hoursMinutesSecondsMicroseconds[2], 10) * 1_000_000L;

        if (hoursMinutesSecondsMicroseconds.length == 4) {
            resulting_timestamp += Long.parseLong(hoursMinutesSecondsMicroseconds[3], 10);
        }
        return resulting_timestamp;
    }


    public static HashSet<String> getPrimitiveTypes(String complex_event_type) throws IllegalArgumentException {
        return getPrimitiveTypes(complex_event_type, new HashSet<>());
    }

    private static HashSet<String> getPrimitiveTypes(String term, HashSet<String> acc) throws IllegalArgumentException {
        final List<String> OPERATORS = Arrays.asList("AND", "SEQ");
        term = term.trim();

        if (term.length() == 1) {
            acc.add(term);
            return acc;
        }
        for (String operator : OPERATORS) {
            if (term.startsWith(operator + "(") && term.endsWith(")")) {
                List<String> args = new ArrayList<>();
                int paren_depth = 0;
                StringBuilder buf = new StringBuilder();
                for (int i = operator.length() + 1; i < term.length() - 1; i++) {
                    char c = term.charAt(i);
                    if (paren_depth == 0 && c == ',') {
                        args.add(buf.toString());
                        buf.delete(0, buf.length());
                    } else
                        buf.append(c);
                    if (c == ')')
                        paren_depth--;
                    else if (c == '(')
                        paren_depth++;

                    if (paren_depth < 0)
                        throw new IllegalArgumentException("Invalid complex event expression: " + term);
                }
                if (paren_depth > 0)
                    throw new IllegalArgumentException("Invalid complex event expression: " + term);
                args.add(buf.toString());

                for (String arg : args)
                    getPrimitiveTypes(arg, acc);
                return acc;
            }
        }
        throw new IllegalArgumentException("Invalid complex event expression: " + term); 
    }
}

