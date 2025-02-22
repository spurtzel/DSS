import random
from scipy.stats import zipf


class EventSourcedNetwork:
    def __init__(self, sequence_length, number_of_nodes, zipfian_parameter, event_node_ratio, local_output_rates, stream_size):
        self.sequence_length = sequence_length
        self.number_of_nodes = number_of_nodes
        self.zipfian_parameter = zipfian_parameter
        self.event_node_ratio = event_node_ratio
        self.stream_size = stream_size
        if len(local_output_rates) == 0:
            self.local_output_rates = self.sample_output_rates()
        else:
            self.local_output_rates = local_output_rates

        self.nodes = self.assign_rates_to_nodes()
        self.eventtype_to_nodes = self.map_eventtype_to_nodes()
        self.eventtype_to_local_outputrate = self.map_eventtype_to_local_outputrate()
        self.eventtype_to_global_outputrate = self.calculate_global_outputrate()


    def sample_output_rates(self):
        rates = [int(rate) for rate in zipf.rvs(self.zipfian_parameter, size=self.sequence_length)]
        while not (sum(rates) < self.stream_size+1 and sum(rates) > self.stream_size-1):
            rates = [int(rate) for rate in zipf.rvs(self.zipfian_parameter, size=self.sequence_length)]
        return rates

    def determine_rates_for_nodes(self):
        nodes = []
        for _ in range(self.number_of_nodes):
            node_rates = [self.local_output_rates[event_type] if random.random() < self.event_node_ratio else 0 
                          for event_type in range(self.sequence_length)]
            nodes.append(node_rates)
        
        return nodes

    def assign_rates_to_nodes(self):
        nodes = self.determine_rates_for_nodes()
        
        indices_with_all_zeros = [i for i in range(len(nodes[0])) if all(sublist[i] == 0 for sublist in nodes)]

        # while there are event types which have not been assigned to at least one node, we determine new rates (due to event_node_ratio)
        while len(indices_with_all_zeros) > 0:
            nodes = self.determine_rates_for_nodes()
            indices_with_all_zeros = [i for i in range(len(nodes[0])) if all(sublist[i] == 0 for sublist in nodes)]

        return nodes

    def map_eventtype_to_local_outputrate(self):
        eventtype_to_local_outputrate = {}

        for idx in range(self.sequence_length):
            eventtype_to_local_outputrate[chr(65 + idx)] = self.local_output_rates[idx]

        return eventtype_to_local_outputrate

    def map_eventtype_to_nodes(self):
        eventtype_to_nodes = {chr(65 + event_type): [] for event_type in range(self.sequence_length)}
        for node_index, node in enumerate(self.nodes):
            for event_type, rate in enumerate(node):
                if rate > 0:
                    eventtype_to_nodes[chr(65 + event_type)].append(node_index + 1)
        return eventtype_to_nodes

    def calculate_global_outputrate(self):
        eventtype_to_global_outputrate = {}
        for event_type, nodes_producing in self.eventtype_to_nodes.items():
            if nodes_producing:
                eventtype_to_global_outputrate[event_type] = self.local_output_rates[ord(event_type) - 65] * len(nodes_producing)
            else:
                eventtype_to_global_outputrate[event_type] = 0
        return eventtype_to_global_outputrate

    def nodes_producing_both_eventtypes(self, eventtype1, eventtype2):
        nodes_eventtype1 = set(self.eventtype_to_nodes.get(eventtype1, []))
        nodes_eventtype2 = set(self.eventtype_to_nodes.get(eventtype2, []))
        
        common_nodes = nodes_eventtype1.intersection(nodes_eventtype2)
        
        return len(common_nodes)

    def print_nodes(self):
        for i, node in enumerate(self.nodes):
            print(f"Node {i + 1}: {node}")

    def print_eventtype_to_nodes(self):
        for event_type, producing_nodes in self.eventtype_to_nodes.items():
            print(f"Event Type {event_type}: Nodes {producing_nodes}")

    def print_eventtype_to_global_outputrate(self):
        for event_type, output_rate in self.eventtype_to_global_outputrate.items():
            print(f"Event Type {event_type}: Global Output Rate {output_rate}")

    def print_outputrates(self):
        print(self.local_output_rates)

def main():
    sequence_length = 10
    number_of_nodes = 10
    zipfian_parameter = 1.1
    event_node_ratio = 0.5

    network = EventSourcedNetwork(sequence_length, number_of_nodes, zipfian_parameter, event_node_ratio, [])
    
    network.print_outputrates()
    network.print_nodes()
    network.print_eventtype_to_nodes()
    network.print_eventtype_to_global_outputrate()

if __name__ == "__main__":
    main()
