package com.huberlin.config;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JSONQueryParser {
    public static QueryInformation parseJsonFile(String local_config, String global_config) throws IOException {
        try {
            String jsonString = new String(Files.readAllBytes(Paths.get(local_config)));
            JSONObject local = new JSONObject(jsonString);
            String jsonString2 = new String(Files.readAllBytes(Paths.get(global_config)));
            JSONObject global = new JSONObject(jsonString2);

            QueryInformation query_information = new QueryInformation();
            query_information.forwarding = parseForwarding(local, global);
            query_information.processing = parseProcessing(local);
            return query_information;
        } catch (IOException e) {
            System.err.println("Error reading JSON file: " + e.getMessage());
            throw e;
        }
    }

    private static QueryInformation.Forwarding parseForwarding(JSONObject local, JSONObject address_book) {
        JSONObject forwardingObject = local.getJSONObject("forwarding");
        QueryInformation.Forwarding forwarding = new QueryInformation.Forwarding();
        forwarding.node_id = forwardingObject.getInt("node_id");

        JSONArray ft_json = forwardingObject.getJSONArray("forwarding_table");
        for (int i = 0; i < ft_json.length(); i++) {
            JSONArray ft_entry = ft_json.getJSONArray(i); 
            String event_type = ft_entry.getString(0);
            List<Integer> source_node_ids = (List<Integer>)(Object) ft_entry.getJSONArray(1).toList();
            List<Integer> dest_node_ids = (List<Integer>)(Object) ft_entry.getJSONArray(2).toList();

            for (Integer source_node_id : source_node_ids)
                forwarding.table.addAll(event_type, source_node_id, dest_node_ids);
        }

        forwarding.recipient = new ArrayList<>(forwarding.table.get_all_destinations());

        for (String nodeId_str : address_book.keySet()) {
            int nodeId = Integer.parseInt(nodeId_str.trim());
            String addr = (String) address_book.get(nodeId_str);
            forwarding.address_book.put(nodeId, addr);
        }

        System.out.println("Forwarding:");
        System.out.println("  Table: " + forwarding.table);
        System.out.println("  Address book: " + forwarding.address_book);

        return forwarding;
    }

    private static List<QueryInformation.Processing> parseProcessing(JSONObject local) {
        JSONArray processingObjects = local.getJSONArray("processing");
        List<QueryInformation.Processing> processInfos = new ArrayList<>();

        for (Object tmp : processingObjects) {
            JSONObject processing = (JSONObject) tmp;
            QueryInformation.Processing processingInfo = new QueryInformation.Processing();

            processingInfo.query_name = processing.getString("query_name");
            processingInfo.subqueries = jsonArrayToList(processing.getJSONArray("subqueries"));
            processingInfo.output_selection = jsonArrayToList(processing.getJSONArray("output_selection"));

            
            if (processing.has("cut_event_type")) {
                processingInfo.cut_event_type = processing.getString("cut_event_type");
            } else {
                processingInfo.cut_event_type = null;
            }
           
            if (processing.has("final_aggregagte_evaluation")) {
                processingInfo.final_aggregagte_evaluation = processing.getBoolean("final_aggregagte_evaluation");
            } else {
                processingInfo.final_aggregagte_evaluation = false;
            }
          

            JSONArray inputs = processing.getJSONArray("inputs");
            processingInfo.inputs = new ArrayList<>();
            for (int i = 0; i < inputs.length(); i++) {
                processingInfo.inputs.add(jsonArrayToList(inputs.getJSONArray(i)));
            }

            JSONArray sequenceConstraintsArray = processing.getJSONArray("sequence_constraints");
            processingInfo.sequence_constraints = new ArrayList<>();
            for (int i = 0; i < sequenceConstraintsArray.length(); i++) {
                List<List<String>> temp = new ArrayList<>();
                JSONArray sequenceConstraintPerSubquery = sequenceConstraintsArray.getJSONArray(i);
                for (int k = 0; k < sequenceConstraintPerSubquery.length(); k++) {
                    temp.add(jsonArrayToList(sequenceConstraintPerSubquery.getJSONArray(k)));
                }
                processingInfo.sequence_constraints.add(temp);
            }

            print(processingInfo);
            processInfos.add(processingInfo);
        }
        return processInfos;
    }

    private static List<String> jsonArrayToList(JSONArray jsonArray) {
        return jsonArray.toList().stream().map(Object::toString).collect(Collectors.toList());
    }

    private static List<Double> jsonArrayToDoubleList(JSONArray jsonArray) {
        return jsonArray.toList().stream().map(Object::toString).map(Double::parseDouble).collect(Collectors.toList());
    }

    private static void print(QueryInformation.Processing p) {
        System.out.println("Processing:");
        System.out.println("  Query name: " + p.query_name);
        System.out.println("  Output selection: " + p.output_selection);
        System.out.println("  Inputs: " + p.inputs);
        System.out.println("  Sequence constraints: " + p.sequence_constraints);
        System.out.println("  Cut event type: " + p.cut_event_type);
        System.out.println("  Final aggregate evaluation: " + p.final_aggregagte_evaluation);
    }
}
