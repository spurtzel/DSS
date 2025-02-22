package com.huberlin.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class ForwardingTable implements Serializable {
    final static private Logger log = LoggerFactory.getLogger(ForwardingTable.class);
    private final HashMap<String, HashMap<Integer, TreeSet<Integer>>> table = new HashMap<>();


    public SortedSet<Integer> lookup(String event_type, Integer source){
        SortedSet<Integer> result = this.table.getOrDefault(event_type, new HashMap<>()).get(source);
        if (result == null) {

            result = new TreeSet<>();
        }
         return new TreeSet<>(result);
    }


    boolean addAll(String event_type, Integer source_node_id, Collection<Integer> destinations) {
        this.table.putIfAbsent(event_type, new HashMap<>());
        this.table.get(event_type).putIfAbsent(source_node_id, new TreeSet<>());
        return this.table.get(event_type).get(source_node_id).addAll(destinations);
    }


    public SortedSet<Integer> get_all_node_ids() {
        SortedSet<Integer> result = new TreeSet<>();
        result.addAll(this.get_all_sources());
        result.addAll(this.get_all_destinations());
        return result;
    }

    SortedSet<Integer> get_all_sources(){
        return this.table.values()
                .stream()
                .flatMap((src_id_to_dest_map) -> src_id_to_dest_map.keySet().stream())
                .collect(Collectors.toCollection(TreeSet::new));
    }
    SortedSet<Integer> get_all_destinations(){
        return this.table.values()
                .stream()
                .flatMap((src_id_to_dest_map) -> src_id_to_dest_map.values().stream().flatMap((destinations) -> destinations.stream()))
                .collect(Collectors.toCollection(TreeSet::new));
    }

}
