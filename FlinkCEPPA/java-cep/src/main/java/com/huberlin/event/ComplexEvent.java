package com.huberlin.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ComplexEvent extends Event {
    static private final Logger log = LoggerFactory.getLogger(ComplexEvent.class);
    final ArrayList<SimpleEvent> eventList;

    String eventID;	

    public final HashMap<String, Long> eventTypeToTimestamp;
    private final HashMap<String, String> eventTypeToEventID;
    private final long creation_timestamp;
    private final long highestTimestamp;
    private final long lowestTimestamp;

    public ComplexEvent(long creationTimestamp,
                        String eventType,
                        ArrayList<SimpleEvent> eventList) {
        this.is_simple = false;
        this.creation_timestamp = creationTimestamp;
        this.eventType = eventType;
        this.eventList = eventList;

	
        eventTypeToTimestamp = new HashMap<>();
        eventTypeToEventID = new HashMap<>();
	String event_ID = ""; 
        long highest_timestamp = Long.MIN_VALUE;
        long lowest_timestamp = Long.MAX_VALUE;
        for (SimpleEvent e : eventList) {
            highest_timestamp = Math.max(highest_timestamp, e.timestamp);
            lowest_timestamp = Math.min(lowest_timestamp, e.timestamp);
            eventTypeToTimestamp.put(e.eventType, e.timestamp);
            eventTypeToEventID.put(e.eventType, e.eventID);
            event_ID = event_ID + e.eventID;
        }
	
        this.highestTimestamp = highest_timestamp;
        this.lowestTimestamp = lowest_timestamp;
	this.eventID = event_ID;
    }

    public int getNumberOfEvents(){
        return this.eventList.size();
    }

    public String getID(){
 	return this.eventID;
    }	 
   	
    public String getEventIdOf(String event_type) {
        return eventTypeToEventID.get(event_type);
    }


     @Override
     public SimpleEvent getEventOfType(String event_type) {
         for (SimpleEvent event : this.eventList) {
            if (event.eventType.equals(event_type)) {
                return event;}
        }
        return null;
    }	
	
    /**
     * Return timestamp of given constituent primitive event, by type.
     * @param event_type Primitive type
     * @return Timestamp
     */
    @Override
    public Long getTimestampOf(String event_type) {
        return eventTypeToTimestamp.get(event_type);
    }

    @Override
    public ArrayList<SimpleEvent> getContainedSimpleEvents() {
        return this.eventList;
    }


    @Override
    public long getTimestamp() {
        return this.creation_timestamp;
    }

    @Override
    public long getLowestTimestamp(){
        return this.lowestTimestamp;
    }

    @Override
    public long getHighestTimestamp(){
        return this.highestTimestamp;
    }

    public double getLatencyMs() {
        double creationTime = (double) this.getTimestamp();
        double newestEventTs = (double) getHighestTimestamp();
        return (creationTime - newestEventTs) / 1000;
        
    }


    public String toString() {
        StringBuilder eventString = new StringBuilder(this.isSimple() ? "simple" : "complex");
        eventString.append(" | ").append(formatTimestamp(this.creation_timestamp));
        eventString.append(" | ").append(this.eventType);
        eventString.append(" | ").append(this.getNumberOfEvents());
        eventString.append(" | ");
        for (int i = 0; i< eventList.size(); i++) {
            SimpleEvent e = this.eventList.get(i);
            eventString.append("(")
                    .append(formatTimestamp(e.timestamp))
                    .append(", ")
                    .append(e.eventID)
                    .append(", ")
                    .append(e.eventType)
                    .append(", ")
                    .append(e.count);
                    for (String attributeValue : e.attributeList)
                        eventString.append(", ").append(attributeValue);
                    eventString.append(")");
            if (i < this.getNumberOfEvents() - 1) 
                eventString.append(" ;");
        }
        return eventString.toString();
    }
}

