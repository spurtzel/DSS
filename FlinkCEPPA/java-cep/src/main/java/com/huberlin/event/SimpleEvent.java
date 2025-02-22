package com.huberlin.event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SimpleEvent extends Event {
    String eventID;
    public final long timestamp;
    public final long count;
    public List<String> attributeList;

    public SimpleEvent(String eventID,
                       long timestamp,
                       String eventType,
                       long count,
                       List<String> attributeList) {
        this.is_simple = true;
        this.eventType = eventType;
        this.eventID = eventID;
        this.timestamp = timestamp;
        assert(eventType != null);
        this.count = count;
        this.attributeList = attributeList;
    }

    @Override
    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public long getHighestTimestamp() {
        return this.getTimestamp();
    }

    @Override
    public long getLowestTimestamp() {
        return this.getTimestamp();
    }

    @Override
    public String getEventIdOf(String event_type) {
        if (event_type.equals(this.eventType))
            return this.eventID;
        else
            return null;
    }
    
    public void setEventID(String new_eventID){
        this.eventID = new_eventID;
    }

    @Override
    public SimpleEvent getEventOfType(String event_type) {
        if (event_type.equals(this.eventType))
            return this;
        else
            return null;
    }		
	

    @Override
    public Long getTimestampOf(String event_type) {
        if (event_type.equals(this.eventType))
            return this.timestamp;
        else
            return null;
    }

    public String toString() {
        StringBuilder eventString = new StringBuilder("simple");
        eventString.append(" | ").append(this.eventID);
        eventString.append(" | ").append(formatTimestamp(this.timestamp));
        eventString.append(" | ").append(this.eventType);
        eventString.append(" | ").append(this.count);
        for (String attributeValue : this.attributeList)
            eventString.append(" | ").append(attributeValue);
        return eventString.toString();
    }

    public String getID(){
 	return this.eventID;
    }	 
    
    public long getCount(){
 	return this.count;
    }	 
   
    @Override
    public ArrayList<SimpleEvent> getContainedSimpleEvents() {
        ArrayList<SimpleEvent> result = new ArrayList<>(1);
        result.add(this);
        return result;
    }

    public List<String> getAttributeList() {
        return this.attributeList;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SimpleEvent)) {
            return false;
        } else if (other == this)
            return true;
        else
            return this.eventID.equals(((SimpleEvent) other).eventID);
    }

    @Override
    public int hashCode() {
        return this.eventID.hashCode();
    }
}

