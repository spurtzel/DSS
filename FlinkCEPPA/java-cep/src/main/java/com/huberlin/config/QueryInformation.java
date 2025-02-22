package com.huberlin.config;

import java.io.Serializable;
import java.util.*;

public class QueryInformation implements Serializable {
    public Forwarding forwarding;
    public List<Processing> processing;

    public static class Forwarding implements Serializable {
        public ArrayList<Integer> recipient;
        public int node_id;
        public final HashMap<Integer, String> address_book = new HashMap<>();
        public final ForwardingTable table = new ForwardingTable();
    }

    public static class Processing implements Serializable {
        public String query_name;
        public List<String> subqueries;
        public List<String> output_selection;
        public List<List<String>> inputs;
        public List<List<List<String>>> sequence_constraints;
        public String cut_event_type;
        public boolean final_aggregagte_evaluation;
    }
}
