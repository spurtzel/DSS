import json
import copy

class Dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    def __deepcopy__(self, memo=None):
        return Dotdict(copy.deepcopy(dict(self), memo=memo))

### INPUTS
# local rates per node
test_network = [[149, 0, 9, 1, 0, 0, 87], [0, 0, 0, 0, 0, 2, 0], [149, 1, 9, 0, 0, 0, 0], [149, 1, 9, 1, 0, 2, 0], [149, 0, 9, 1, 1, 0, 0], [0, 0, 9, 1, 1, 0, 0], [0, 0, 9, 0, 0, 0, 87], [0, 0, 9, 1, 0, 0, 0], [0, 1, 9, 1, 1, 2, 87], [149, 0, 0, 1, 1, 2, 87], [149, 1, 0, 0, 0, 2, 87], [0, 0, 9, 1, 1, 2, 0], [149, 0, 0, 1, 1, 2, 87], [0, 1, 0, 1, 1, 2, 87], [0, 0, 9, 1, 0, 2, 87], [149, 0, 0, 0, 1, 2, 87], [149, 1, 0, 0, 1, 2, 87], [149, 1, 9, 0, 1, 2, 87], [149, 0, 0, 0, 1, 0, 0], [0, 1, 0, 0, 0, 2, 0]]

# push_step_format: {'type': 'push', 'aggregate': , 'targetEtype': , 'targetSinkType': , 'targetNode': }
# aggr_step_format: {'type': 'aggr', 'aggregate': , 'targetEtype': , 'sendRateEtype': , 'sourceEtype': , 'targetSinkType': , 'sourceSinkType': , 'targetNode': , 'sourceNode': }
# steps of the plan (need subtract 1 from node_ids when plan comes from plan_generator)
test_plan = [{'type': 'push', 'aggregate': 'B', 'targetEtype': 'A', 'targetSinkType': 'MuSi', 'targetNode': None}, {'type': 'push', 'aggregate': 'DEF', 'targetEtype': 'C', 'targetSinkType': 'SiSi', 'targetNode': 17}, {'type': 'aggr', 'aggregate': 'AB', 'targetEtype': 'C', 'sendRateEtype': 'B', 'sourceEtype': 'A', 'targetSinkType': 'SiSi', 'sourceSinkType': 'MuSi', 'targetNode': 17, 'sourceNode': None}, {'type': 'aggr', 'aggregate': 'ABCDEF', 'targetEtype': 'G', 'sendRateEtype': 'F', 'sourceEtype': 'C', 'targetSinkType': 'MuSi', 'sourceSinkType': 'SiSi', 'targetNode': None, 'sourceNode': 17}]

# at which eventtypes which aggregate gets processed (if SiSi also which specific node, MuSi->None instead)
test_aggregate_placement = [('AB', 'A', None, 'B'), ('ABCDEF', 'C', 17, 'F'), ('ABCDEFG', 'G', None, 'G')]


def getsources(network, query):   # returns dict: keys are event types, values are nodes
    letters = query
    my_dict = {}
    for letter in range(len(network[0])):
        my_dict[letters[letter]] = []
    for pos in range(len(network)):
        node = network[pos]
        for letter in range(len(node)):
            if node[letter] > 0:
                my_dict[letters[letter]].append(pos)
    return my_dict

def gettypes(network, query):   # returns dict: keys are nodes, values are event types
    letters = query
    my_dict = {}
    for pos in range(len(network)):
        my_dict[pos] = []
        node = network[pos]
        for letter in range(len(node)):
            if node[letter] > 0:
                my_dict[pos].append(letters[letter])
    return my_dict

def get_aggregate_dict(aggregate_placement, source_dict):
    aggregate_dict = {}
    for aggregate in aggregate_placement:
        if aggregate[2] is None:
            aggregate_dict[aggregate[0]] = source_dict[aggregate[1]]
        else:
            aggregate_dict[aggregate[0]] = [aggregate[2]]
    return aggregate_dict

def get_forwarding(plan, aggregate_placement, aggregate_dict, ):  # write input to forwarding
    forwarding_dict = {}
    for step in plan:
        if step.type == 'push':   # primitive event: aggregate sent to targetEtype
            for eventtype in step.aggregate:
                forwarding_dict[eventtype] = [step.targetEtype, step.targetNode]
        else:   # aggregate to be sent to targetEtype with rate sendRateEtype
            forwarding_dict[step.aggregate] = [step.targetEtype, step.targetNode, step.sourceNode]

        # add forwarding rule for other nodes than the sisi_node
        if step.targetSinkType == "SiSi":
            forwarding_dict[step.targetEtype] = [step.targetEtype, step.targetNode]

    #print(f"forwarding_dict: {forwarding_dict}")
    for aggregate in aggregate_placement:
        if aggregate[0] in forwarding_dict:
            continue
        targetEtype = aggregate[1]
        targetNode = aggregate[2]
        sourceNode = None
        for key in aggregate_dict.keys():
            if aggregate[0] == key:
                if len(aggregate_dict[key]) == 1:
                    sourceNode = aggregate_dict[key][0]
                else:
                    sourceNode = aggregate_dict[key]
        forwarding_dict[aggregate[0]] = [targetEtype, targetNode, sourceNode]

    return forwarding_dict

def get_aggregate(aggregate_placement, eventtype):
    aggregates = [x[0] for x in aggregate_placement]
    my_aggregates = sorted([x for x in aggregates if eventtype in x and not x == eventtype], key=len)
    #print("for eventtype", eventtype ,"my_aggregates:", my_aggregates)
    if my_aggregates:
        return my_aggregates[0]
    else:
        return ''

def get_forwarding_entry(aggregate_placement, node, type_dict, source_dict, forwarding_dict, aggregate_dict, input_dict):
    forwarding_entry = {}
    my_aggregates = [x for x in aggregate_dict.keys() if node in aggregate_dict[x]]
    #print("my_aggregates:", my_aggregates)
    send_rules = []
    #print("--- Adding entries for primitive events ---")
    for eventtype in type_dict[node]:
        if eventtype in forwarding_dict.keys():
            if len(forwarding_dict[eventtype]) == 2:
                if forwarding_dict[eventtype][1] is None:
                    forwarding_entry[eventtype] = [source_dict[forwarding_dict[eventtype][0]], get_aggregate(aggregate_placement, eventtype)]
                    #print("forwarding_entry 1.1:", eventtype, "->", forwarding_entry[eventtype])
                else:
                    forwarding_entry[eventtype] = [[forwarding_dict[eventtype][1]], get_aggregate(aggregate_placement, eventtype)]
                    #print("forwarding_entry 1.2:", eventtype, "->", forwarding_entry[eventtype])
            else:
                print("Something different than expected at point 1 (processing type_dict)")
            send_rules.append(eventtype)
    #print("--- Adding entries for aggregates ---")
    for aggregate in my_aggregates:
        if aggregate in forwarding_dict.keys():
            if len(forwarding_dict[aggregate]) == 2:
                print("Something different than expected at point 2 (processing my_aggregates)")
            else:
                if get_aggregate(aggregate_placement, aggregate) == '':
                    #print("Empty entry for", aggregate)
                    continue
                special_local_entry = forwarding_dict[aggregate][1] is None and isinstance(forwarding_dict[aggregate][2], list)
                if special_local_entry:
                    # None list
                    forwarding_entry[aggregate] = [[node], get_aggregate(aggregate_placement, aggregate)]
                    #print("forwarding_entry 2.1:", aggregate, "->", forwarding_entry[aggregate])
                elif forwarding_dict[aggregate][1] is None:
                    # None None/4
                    forwarding_entry[aggregate] = [source_dict[forwarding_dict[aggregate][0]], get_aggregate(aggregate_placement, aggregate)]
                    #print("forwarding_entry 2.2:", aggregate, "->", forwarding_entry[aggregate])
                else:
                    # 4 None/4
                    forwarding_entry[aggregate] = [[forwarding_dict[aggregate][1]], get_aggregate(aggregate_placement, aggregate)]
                    #print("forwarding_entry 2.3:", aggregate, "->", forwarding_entry[aggregate])
            send_rules.append(aggregate)
    #print("--- Adding leftover forwarding entries ---")
    for aggregate in my_aggregates:
        for inputtype in input_dict[aggregate]:
            #print(inputtype, send_rules, source_dict)
            if not inputtype in send_rules and node in source_dict[inputtype]:
                forwarding_entry[inputtype] = [[node], aggregate]
                #print("forwarding_entry 3.1:", inputtype, "->", [[node], aggregate])
                send_rules.append(inputtype)

    # delete entries for final aggregate that gets sent locally (removed since empty entry of final aggregate gets filtered get filtered)
    # delete_keys = []
    # for key in forwarding_entry:
    #     if forwarding_entry[key][1] == '':
    #         delete_keys.append(key)
    # for key in delete_keys:
    #     del forwarding_entry[key]

    #print("Finished forwarding for node", node), print()
    return forwarding_entry

def get_processing_entry(plan, aggregate_placement, node, source_dict, forwarding_dict, type_dict, all_configs):
    local_processing_entry = {}
    my_aggregates = []
    for eventtype in type_dict[node]:
        my_aggregates += [x[0] for x in aggregate_placement if x[1] == eventtype and (x[2] is None or x[2] == node)]

    for aggregate in my_aggregates:
        inputs = sorted([x for x in list(set(list(source_dict.keys()) + list(forwarding_dict.keys()))) if aggregate == get_aggregate(aggregate_placement, x)], key=lambda x: x[0])
        get_source = [x[1] for x in aggregate_placement if x[0] == aggregate]
        get_rate = [x[3] for x in aggregate_placement if x[0] == aggregate]
        if not get_rate:   # sink
            get_rate = get_source
        # only if input is actually sent

        preds = []
        for i in inputs:
            for config in all_configs:
                forwarding_types = config["forwarding"]
                if i in forwarding_types and node in forwarding_types[i][0]:
                    preds.append(config["id"])

        predecessors = len(preds)   # number of contributing events computing inputs to aggregate
        local_processing_entry[aggregate] = [get_source[0], get_rate[0], inputs, predecessors]
    return local_processing_entry

def old_get_processing_entry(plan, aggregate_placement, node, source_dict, forwarding_dict, type_dict):
    local_processing_entry = {}
    my_aggregates = []
    for eventtype in type_dict[node]:
        my_aggregates += [x[0] for x in aggregate_placement if x[1] == eventtype and (x[2] is None or x[2] == node)]

    for aggregate in my_aggregates:
        inputs = sorted([x for x in list(set(list(source_dict.keys()) + list(forwarding_dict.keys()))) if aggregate == get_aggregate(aggregate_placement, x)], key=lambda x: x[0])
        get_source = [x[1] for x in aggregate_placement if x[0] == aggregate]
        get_rate = [step.sendRateEtype for step in plan if step.aggregate == aggregate]
        if not get_rate:   # sink
            get_rate = get_source
        # only if input is actually sent

        preds = [source_dict[x] for x in [y for y in inputs if y in forwarding_dict.keys()]]
        for i in inputs:
            if node in source_dict[i]:
                preds.append([node])
        predecessors = len(list(set(sum(preds, []))))   # number of distinct nodes computing inputs to aggregate
        local_processing_entry[aggregate] = [get_source[0], get_rate[0], inputs, predecessors]
    return local_processing_entry


# push_step_format: {'type': 'push', 'aggregate': , 'targetEtype': , 'targetSinkType': , 'targetNode': }
# aggr_step_format: {'type': 'aggr', 'aggregate': , 'targetEtype': , 'sendRateEtype': , 'sourceEtype': , 'targetSinkType': , 'sourceSinkType': , 'targetNode': , 'sourceNode': }
def main():
    query = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']   # ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']
    new_test_plan = []
    for step in test_plan:   # change plan into dotdict format so it's easier to work with
        new_test_plan.append(Dotdict(step))

    source_dict = getsources(test_network, query)                                               # dict of every primitive event and its source(s)
    type_dict = gettypes(test_network, query)                                                   # dict of every source and its produced primitive event(s)
    aggregate_dict = get_aggregate_dict(test_aggregate_placement, source_dict)                  # dict of aggregate and on which nodes evaluation happens
    forwarding_dict = get_forwarding(new_test_plan, test_aggregate_placement, aggregate_dict)   # dict of events and aggregates that need to be forwarded and where to forward them to
    # forwarding_dict Note with [targetetype, target, source]:
    #> target/source = None --> target/source is MuSi placement
    #> target/source = id   --> target/source SiSi node
    #> special case: target=None, source=id_list --> processing nodes forward their result to themselves
    print(f"source_dict: {source_dict}")
    print(f"type_dict: {type_dict}")
    print(f"aggregate_dict: {aggregate_dict}")
    print(f"forwarding_dict: {forwarding_dict}"), print()

    input_dict = {}   # dict of aggregates and which partial events they are composed of
    for aggregate in test_aggregate_placement:
        input_dict[aggregate[0]] = sorted([x for x in list(set(list(source_dict.keys()) + list(forwarding_dict.keys()))) if aggregate[0] == get_aggregate(test_aggregate_placement, x)], key=lambda x: x[0])

    # adding the aggregates and on which nodes they are generated
    for aggregate in test_aggregate_placement:
        if aggregate[2] is None:
            source_dict[aggregate[0]] = source_dict[aggregate[1]]
        else:
            source_dict[aggregate[0]] = [aggregate[2]]

    print("input_dict:", input_dict)
    print("source_dict:", source_dict)
    print("--------------------------------------------------"), print()

    all_configs = []
    for node in type_dict.keys():
        my_config = {"id": node,
                     "forwarding": get_forwarding_entry(test_aggregate_placement, node, type_dict, source_dict, forwarding_dict, aggregate_dict, input_dict)
                     }
        all_configs.append(my_config)

    # processing needs forwarding part
    for config in all_configs:
        config["processing"] = get_processing_entry(new_test_plan, test_aggregate_placement, config["id"], source_dict, forwarding_dict, type_dict, all_configs)
        print(config), print()
        with open('plans/config_' + str(config["id"]) + '.json', 'w') as config_file:
            json.dump(config, config_file)

if __name__ == "__main__":
    main()
