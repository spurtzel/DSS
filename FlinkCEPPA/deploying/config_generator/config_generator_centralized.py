import json
import os

def parse_aggregator_name(agg_name, known_aggregators):
    result = []
    i = 0
    while i < len(agg_name):
        matched = False
        for length in range(len(agg_name) - i, 0, -1):
            candidate = agg_name[i : i + length]
            if candidate in known_aggregators:
                result.append(candidate)
                i += length
                matched = True
                break
        if not matched:
            result.append(agg_name[i])
            i += 1
    return result

def expand_aggregator(agg_name, known_aggregators):
    components = parse_aggregator_name(agg_name, known_aggregators)
    final_expansion = []
    for comp in components:
        if comp in known_aggregators:
            final_expansion.append(known_aggregators[comp]["forwarded_event"])
        else:
            final_expansion.append(comp)
    return final_expansion

def create_seq_constraints(events):
    seq_constr = []
    for i in range(len(events)):
        for j in range(i + 1, len(events)):
            seq_constr.append([events[i], events[j]])
    return [seq_constr]

def create_processing_obj(query_name, events, is_final, cut_event_type):

    return {
        "query_name": query_name,
        "output_selection": list(events),
        "subqueries": [query_name],
        "inputs": [list(events)],
        "sequence_constraints": create_seq_constraints(events),
        "id_constraints": [[]],
        "final_aggregagte_evaluation": is_final,
        "cut_event_type": cut_event_type
    }

def parse_evaluation_plan(evaluation_plan, eventtype_to_nodes):
    all_nodes = set()
    for et_list in eventtype_to_nodes.values():
        all_nodes.update(et_list)
    all_nodes = sorted(all_nodes)

    node_configs = {
        nid: {
            "forwarding": {
                "node_id": nid,
                "forwarding_table": []
            },
            "processing": []
        }
        for nid in all_nodes
    }

    known_aggregators = {}

    def add_forwarding_entry(node_id, event_name, from_nodes, to_nodes):
        node_configs[node_id]["forwarding"]["forwarding_table"].append(
            [event_name, from_nodes, to_nodes]
        )

    for step in evaluation_plan:

        if isinstance(step, dict):
            step_type = step.get("type")

            if step_type == "push":
                agg = step["aggregate"]
                target_etype = step["targetEtype"]
                sink_type = step["targetSinkType"]
                target_node = step["targetNode"]
                producing_nodes = eventtype_to_nodes.get(agg, [])

                if sink_type == "SiSi":
                    for p in producing_nodes:
                        add_forwarding_entry(p, agg, [p], [target_node])
                else:  # "MuSi"
                    multi_targets = eventtype_to_nodes.get(target_etype, [])
                    for p in producing_nodes:
                        add_forwarding_entry(p, agg, [p], multi_targets)

            elif step_type == "aggr":
                agg_name = step["aggregate"]        
                send_rate_etype = step["sendRateEtype"] 
                source_etype = step["sourceEtype"]
                target_etype = step["targetEtype"]
                sink_type = step["targetSinkType"]
                source_sink_type = step["sourceSinkType"]
                target_node = step["targetNode"]
                source_node = step["sourceNode"]

                forwarded_event = f"{agg_name}_{send_rate_etype}"

                sub_events = expand_aggregator(agg_name, known_aggregators)

                known_aggregators[agg_name] = {
                    "expanded_events": sub_events,
                    "forwarded_event": forwarded_event
                }


                if source_node is not None:
                    aggregator_nodes = [source_node]
                else:
                    if source_sink_type == "SiSi":
                        all_producers = eventtype_to_nodes.get(source_etype, [])
                        if not all_producers:
                            aggregator_nodes = []
                        else:
                            chosen = min(all_producers)
                            aggregator_nodes = [chosen]

                            for p in all_producers:
                                if p != chosen:
                                    add_forwarding_entry(p, source_etype, [p], [chosen])
                    else:
                    
                        aggregator_nodes = eventtype_to_nodes.get(source_etype, [])

                query_name = "SEQ(" + ",".join(sub_events) + ")"
                sub_query_obj = create_processing_obj(
                    query_name=query_name,
                    events=sub_events,
                    is_final=False,
                    cut_event_type=send_rate_etype  
                )

                for nd in aggregator_nodes:
                    node_configs[nd]["processing"].append(sub_query_obj)

                if sink_type == "SiSi":
                    for nd in aggregator_nodes:
                        add_forwarding_entry(nd, forwarded_event, [nd], [target_node])
                else:
                    multi_targets = eventtype_to_nodes.get(target_etype, [])
                    for nd in aggregator_nodes:
                        add_forwarding_entry(nd, forwarded_event, [nd], multi_targets)

        elif isinstance(step, list):
            if len(step) >= 4:
                query_str = step[0]
                is_final = step[1]
                final_etype = step[2]
                cut_event = step[3]

                placement_nodes = eventtype_to_nodes.get(final_etype, [])

                inside_seq = query_str.replace("SEQ(", "").rstrip(")")
                seq_events = [x.strip() for x in inside_seq.split(",")]

                p_obj = create_processing_obj(
                    query_name=query_str,
                    events=seq_events,
                    is_final=is_final,
                    cut_event_type=cut_event
                )

                for nd in placement_nodes:
                    node_configs[nd]["processing"].append(p_obj)

    return node_configs

def write_configs_to_directory(node_configs, output_dir="configs"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for node_id, cfg in node_configs.items():
        fname = os.path.join(output_dir, f"config_{node_id}.json")
        with open(fname, 'w') as f:
            json.dump(cfg, f, indent=2)


if __name__ == "__main__":    
    eventtype_to_nodes = {'A': [2], 'B': [1], 'C': [0, 3], 'D': [0, 2, 4]}
    evaluation_plan = [{'type': 'push', 'aggregate': 'BCD', 'targetEtype': 'A', 'targetSinkType': 'SiSi', 'targetNode': 2}, ['SEQ(A,B,C,D)', True, 'A', 'D']]

    node_configs = parse_evaluation_plan(evaluation_plan, eventtype_to_nodes)
    write_configs_to_directory(node_configs, "configs")

    print("Configurations written to ./configs/.")
