from event_sourced_network import EventSourcedNetwork
import numpy as np
import math
import statistics
import time
import re
import subprocess
import copy
import csv

from collections import Counter
from itertools import product

############################################### Helper  Methods/Variables ##############################################
use_bf_filtering_for_seq_length_greater = 9

class Dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __deepcopy__(self, memo=None):
        return Dotdict(copy.deepcopy(dict(self), memo=memo))

def print_step(step, end='\n'):
    """print step_dict as list"""
    print(list(step.values()), end=end)

def print_plan_with_dict_steps(plan, index=-1, delim=', ', end='\n'):
    """print plan with steps as dicts as lists (either separated by delimiter in one line or one line per step)"""
    if index != -1:
        print(f"Plan {index:2d}:", end='')
    else:
        print(f"Plan :", end='')
    if delim != '\n':
        print(" [", end='')
        for step in plan[:-1]:
            print(list(step.values()), end=delim)
        print(f" {list(plan[-1].values())}]", end=end)
    else:
        print()
        for idx, step in enumerate(plan):
            print(f"{idx:2d}: {list(step.values())}", end=delim)

class PushPullAggregationPlanner:
    def __init__(self, sequence_pattern, number_of_nodes, zipfian_parameter, event_node_ratio):
        self.sequence_pattern = sequence_pattern
        self.number_of_nodes = number_of_nodes
        self.zipfian_parameter = zipfian_parameter
        self.event_node_ratio = event_node_ratio
        self.event_sourced_network = EventSourcedNetwork(len(self.sequence_pattern), number_of_nodes, zipfian_parameter, event_node_ratio, [])

    def predecessor_is_lower(self, sequence_pattern, idx):
        return self.event_sourced_network.eventtype_to_global_outputrate[sequence_pattern[idx-1]] < self.event_sourced_network.eventtype_to_local_outputrate[sequence_pattern[idx]]

    def successor_is_lower(self, sequence_pattern, idx):
        return self.event_sourced_network.eventtype_to_global_outputrate[sequence_pattern[idx+1]] < self.event_sourced_network.eventtype_to_local_outputrate[sequence_pattern[idx]]

    @staticmethod
    def is_initiator_or_terminator_eventtype(sequence_pattern, eventtype):
        return sequence_pattern[0] == eventtype or sequence_pattern[-1] == eventtype

    def determine_cut_sequence_idx(self, lhs_strong_placement, rhs_strong_placement):
        lhs_strong_placement_idx = self.sequence_pattern.index(lhs_strong_placement)
        rhs_strong_placement_idx = self.sequence_pattern.index(rhs_strong_placement)

        minimal_rate = float('inf')
        minimal_type_idx = ''
        for idx in range(lhs_strong_placement_idx+1, rhs_strong_placement_idx):
            if self.event_sourced_network.eventtype_to_global_outputrate[self.sequence_pattern[idx]] < minimal_rate:
                minimal_rate = self.event_sourced_network.eventtype_to_global_outputrate[self.sequence_pattern[idx]]
                minimal_type_idx = idx
        return minimal_type_idx

    def generate_greedy_plan(self):
        ppa_plan = []
        strong_placements = []
        sequence_length = len(self.sequence_pattern)

        # Determine strong placements
        for idx in range(sequence_length):
            if idx == 0 and self.successor_is_lower(self.sequence_pattern, idx):
                strong_placements.append(self.sequence_pattern[idx])
            elif idx == sequence_length - 1 and self.predecessor_is_lower(self.sequence_pattern, idx):
                strong_placements.append(self.sequence_pattern[idx])
            elif 0 < idx < sequence_length - 1:
                if self.predecessor_is_lower(self.sequence_pattern, idx) and self.successor_is_lower(self.sequence_pattern, idx):
                    strong_placements.append(self.sequence_pattern[idx])


        if len(strong_placements) <= 1:   # Unfortunately, centralized push is optimal
            push_plan = []
            for eventtype in self.event_sourced_network.eventtype_to_global_outputrate:
                push_plan.append((eventtype, eventtype))

            return push_plan

        # Initiator position
        first_strong_placement = strong_placements[0]
        if self.sequence_pattern[0] not in strong_placements:
            step = (self.sequence_pattern[0:self.sequence_pattern.index(first_strong_placement)], first_strong_placement)
            ppa_plan.append(step)

        # Terminator position
        last_strong_placement = strong_placements[-1]
        if self.sequence_pattern[-1] not in strong_placements:
            step = (self.sequence_pattern[self.sequence_pattern.index(last_strong_placement)+1:len(self.sequence_pattern)], last_strong_placement)
            ppa_plan.append(step)

        # There is more than 1 strong placement
        cut_types = []
        for idx in range(len(strong_placements)-1):
            cut_idx = self.determine_cut_sequence_idx(strong_placements[idx], strong_placements[idx+1])
            cut_types.append(self.sequence_pattern[cut_idx])
            lhs_strong_placement_idx = self.sequence_pattern.index(strong_placements[idx])
            rhs_strong_placement_idx = self.sequence_pattern.index(strong_placements[idx+1])
            if not cut_idx-1 == lhs_strong_placement_idx:
                lhs_step = (self.sequence_pattern[lhs_strong_placement_idx+1:cut_idx], strong_placements[idx])
                ppa_plan.append(lhs_step)

            rhs_step = (self.sequence_pattern[cut_idx:rhs_strong_placement_idx], strong_placements[idx+1])
            ppa_plan.append(rhs_step)

        active_sources = {}
        for idx in range(len(ppa_plan)):
            pushed_types = ppa_plan[-1+idx][0]
            target_source = ppa_plan[-1+idx][1]
            if target_source not in active_sources:
                active_sources[target_source] = []
                active_sources[target_source].append(target_source)
            for pushed_type in pushed_types:
                active_sources[target_source].append(pushed_type)

        idx = len(strong_placements)-1
        while idx > 0:
            node_idx = strong_placements[idx]
            projection = ''.join(sorted(active_sources[node_idx], key=self.sequence_pattern.index))
            rate = set(active_sources[node_idx]).intersection(set(cut_types)).pop()
            step = (projection, strong_placements[idx-1], rate, strong_placements[idx])
            ppa_plan.append(step)
            idx -= 1

        return ppa_plan

    @staticmethod
    def merge_PPA_plan_steps(ppa_plan):
        merged_push_steps = {}
        aggregate_steps = []

        for step in ppa_plan:
            if len(step) == 2:
                if step[1] not in merged_push_steps:
                    merged_push_steps[step[1]] = step[0]
                else:
                    merged_push_steps[step[1]] += step[0]
            else:
                aggregate_steps.append(step)

        merged_tuples = [(merged_push_steps[key], key) for key in merged_push_steps]
        return merged_tuples + aggregate_steps

    def determine_PPA_plan_costs_esn_complete_topology(self, ppa_plan):
        """esn -> event-sourced network"""
        if len(ppa_plan) == len(self.sequence_pattern):
            return self.determine_centralized_push_costs()

        push_costs = 0
        aggregation_costs = 0

        for step in ppa_plan:
            if len(step) == 2:
                for etype_to_send in step[0]:
                    target_node_etype = step[1]
                    num_nodes_producing_both_etypes = self.event_sourced_network.nodes_producing_both_eventtypes(etype_to_send, target_node_etype)

                    push_costs += self.event_sourced_network.eventtype_to_global_outputrate[etype_to_send] * len(self.event_sourced_network.eventtype_to_nodes[target_node_etype]) - num_nodes_producing_both_etypes * self.event_sourced_network.eventtype_to_local_outputrate[etype_to_send]
            elif len(step) > 2:
                for etype_to_send in step[2]:
                    target_node_etype = step[1]
                    multi_source_node_etype = step[3]
                    num_nodes_producing_both_etypes = self.event_sourced_network.nodes_producing_both_eventtypes(multi_source_node_etype, target_node_etype)
                    num_multi_sink_placement_nodes  = len(self.event_sourced_network.eventtype_to_nodes[multi_source_node_etype])
                    target_nodes = len(self.event_sourced_network.eventtype_to_nodes[target_node_etype])

                    aggregation_costs += self.event_sourced_network.eventtype_to_global_outputrate[etype_to_send] * num_multi_sink_placement_nodes * target_nodes - num_nodes_producing_both_etypes * self.event_sourced_network.eventtype_to_global_outputrate[etype_to_send]
        return push_costs + aggregation_costs

    def determine_AND_query_costs(self):
        lowest_global_output_rate_eventtype = min(self.event_sourced_network.eventtype_to_global_outputrate, key=self.event_sourced_network.eventtype_to_global_outputrate.get)

        missing_event_types = [char for char in self.sequence_pattern if char != lowest_global_output_rate_eventtype]

        ppa_plan = []
        push_step = (lowest_global_output_rate_eventtype, missing_event_types[0])
        ppa_plan.append(push_step)
        for idx in range(1, len(missing_event_types)):
            aggregation_step = (lowest_global_output_rate_eventtype, missing_event_types[idx], lowest_global_output_rate_eventtype, missing_event_types[idx-1])
            ppa_plan.append(aggregation_step)

        return self.determine_PPA_plan_costs_esn_complete_topology(ppa_plan)

    def determine_centralized_push_costs(self):
        sink_costs = sum(max(self.event_sourced_network.nodes, key=sum))
        total_costs = sum(self.event_sourced_network.eventtype_to_global_outputrate.values())

        return total_costs - sink_costs

    ########################################################################################################################
    ######################################################## Min-Algorithm #################################################
    ########################################################################################################################

    def min_algorithm_cet_composition(self, sequence_pattern):
        cet_composition = ''
        for idx in range(0,len(self.sequence_pattern)-1):
            if self.event_sourced_network.eventtype_to_global_outputrate[sequence_pattern[idx]] < self.event_sourced_network.eventtype_to_global_outputrate[sequence_pattern[idx+1]]:
                cet_composition += sequence_pattern[idx]
            else:
                cet_composition += sequence_pattern[idx+1]

        return cet_composition


    def extract_query_string(self, main_string):
        char_counts = Counter(main_string)
        return ''.join([char for char, count in char_counts.items() if count > 1])

    def generate_substring_sets(self, main_string, query_string):
        result = {}

        for char in query_string:
            substrings = set()

            indices = [i for i, c in enumerate(main_string) if c == char]

            for index in indices:
                substrings.add((main_string[:index+1]))
                substrings.add((main_string[index:]))

            result[char] = substrings

        return result

    def is_valid_tuple(self, t):
        """ Check if the tuple contains overlapping substrings where one is not fully contained in the other."""
        for i, s1 in enumerate(t):
            for j, s2 in enumerate(t):
                if i != j and any(c in s2 for c in s1):
                    if not (s1 in s2 or s2 in s1):
                        return False
        return True

    def get_key(self, subquery,substring_sets):
        for key in substring_sets.keys():
            if subquery in substring_sets[key]:
                return key

    def generate_cross_product(self, substring_sets):
        keys = substring_sets.keys()
        values = substring_sets.values()
        cross_product = [t for t in product(*values) if self.is_valid_tuple(t)]
        correct_decompositions = []
        for decomposition in cross_product:
            decompositions = []
            for subquery in decomposition:
                decompositions.append((subquery, self.get_key(subquery,substring_sets)))
            correct_decompositions.append(decompositions)
        return correct_decompositions

    def find_inclusion_maximal_subsets(self, strings):
        result = {}

        strings = sorted(strings, key=len)
        added = []
        for s in strings:

            result[s] = []
            for candidate in strings:
                if candidate != s and candidate in s and candidate not in added:
                    result[s].append(candidate)
                    added.append(candidate)

        return result

    def generate_placement(self, decomposition, inclusionmap, cet_string):
        placement_map = {}
        strings = sorted(inclusionmap.keys(), key = len)
        for i in strings:
            placement =  set(i).difference(set([x[1] for x in decomposition if x[0] == i])) # not placed at cut event type
            placement =  set(placement).difference(set().union(*inclusionmap[i]))
            placement =  set(placement).difference(set().union(*cet_string))
            if len(placement)> 1:
                max_event = max(placement, key=lambda etype: self.event_sourced_network.eventtype_to_global_outputrate.get(etype, 0))
                placement_map[i] = max_event
            else:
                placement_map[i] = placement
        return placement_map




    def get_cet(self, decomposition, sub_query):
        for i in decomposition:
            if sub_query in i:
                return i[1]

    def generate_evaluationplan(self, decomposition, inclusionmap,placement_map):
        strings = sorted(inclusionmap.keys(), key = len)
        evaluationplan = []
        for i in strings:
            for decomposition_element in inclusionmap[i]:
                mytuple = (decomposition_element, "".join(list(placement_map[i])),self.get_cet(decomposition, decomposition_element), "".join(list(placement_map[decomposition_element])))
                evaluationplan.append(mytuple)

            my_i = set(i).difference(set().union(*inclusionmap[i]))
            my_i = set(my_i).difference(set(placement_map[i]))
            if my_i:
                mytuple = ("".join(list(my_i)),"".join(list(placement_map[i])))
                evaluationplan.append(mytuple)
        return evaluationplan

    def generate_finalplan(self, main_string, cet_string):
        query_string = self.extract_query_string(cet_string)
        substring_sets = self.generate_substring_sets(main_string, query_string)
        cross_product = self.generate_cross_product(substring_sets) # this is the set of decompositions

        query_string = self.extract_query_string(cet_string)
        substring_sets = self.generate_substring_sets(main_string, query_string)
        cross_product = self.generate_cross_product(substring_sets)


        # take main_string and first elements of tuples in cross_products to generate dict
        inputs = [x[0] for x in cross_product[0]]
        inputs.append(main_string)
        inclusionmap = self.find_inclusion_maximal_subsets(inputs)
        placement_map = self.generate_placement(cross_product[0], inclusionmap, cet_string)
        return self.generate_evaluationplan(cross_product[0], inclusionmap,placement_map)

    ########################################################################################################################
    ################################################## MuSi/SiSi  Section ##################################################
    ########################################################################################################################
    @staticmethod
    def prepend_steptype_for_steps_in_pa_plan(pa_plan):
        """
        Note: Afterwards steptype can be differentiated by first element ('push' or 'aggr')

        :param pa_plan: pa_plan with steps (pushEtypes, targetEtype)  and  (aggregate, targetEtype, rateEtype, sourceEtype)
        :return: pa_plan with steps ('push', pushEtypes, targetEtype)  and  ('aggr', aggregate, targetEtype, rateEtype, sourceEtype)
        """
        new_pa_plan = []
        for step in pa_plan:
            if len(step) == 2:
                new_step = ('push', step[0], step[1])
                new_pa_plan.append(new_step)
            if len(step) == 4:
                new_step = ('aggr', step[0], step[1], step[2], step[3])
                new_pa_plan.append(new_step)
        return new_pa_plan

    ####################################################################################################
    def build_all_possible_plans_with_SiSi(self, pa_plan):
        """\n
        :param pa_plan: with steps like ('push', 'ABD', 'C')  and  ('aggr', 'ABCD', 'E', 'D', 'C')  [letters ABCDE are only placeholders]
        :return: - list of every MuSi-SiSi combination for the pa_plan (near optimal nodes are chosen for SiSi-steps) with steps like <br>
            {'type': 'push', 'aggregate': step[1], 'targetEtype': step[2], 'targetSinkType': 'MuSi'/'SuSi', 'targetNode': None/NodeID} <br>
            {'type': 'aggr', 'aggregate': step[1], 'targetEtype': step[2], 'sendRateEtype': step[3], 'sourceEtype': step[4], <br>
             'targetSinkType': 'MuSi'/'SuSi', 'sourceSinkType': 'MuSi'/'SuSi', 'targetNode': None/NodeID, 'sourceNode': None/NodeID} <br>
            Note: each step for all plans of output is a Dotdict and can be used with step.key (example step.type) <br>
            - A dict of predecessors for etypes
        """
        # Input (pa_plan with steps)  :
        # Output (PA_Plans with steps):

        plans = []   # holds every possible plan

        etype_to_plan_layer, etype_to_predecessorsteps, etype_successors = self.new_determine_plan_hierarchy(pa_plan)
        sorted_layer_dict = dict(sorted(etype_to_plan_layer.items(), key=lambda item: item[1]))

        pa_plan, etype_to_predecessorsteps = self.format_pa_plan(pa_plan, sorted_layer_dict, etype_to_predecessorsteps)

        has_splitup = False
        for key in etype_to_predecessorsteps:
            if etype_to_predecessorsteps[key][0] == 'aa' or etype_to_predecessorsteps[key][0] == 'paa':
                has_splitup = True

        num_independent_steps = len(sorted_layer_dict)
        # For every possible Plan: creating labeling -> determine continuous SiSi-step-chains -> find Nodes for continuous subchains
        for index in range(2**num_independent_steps):
            labeling = bin(index)[2:].zfill(num_independent_steps)   # binary string with the length of num_independent_steps
            new_pa_plan = copy.deepcopy(pa_plan)
            new_pa_plan = self.apply_labeling_to_plan(new_pa_plan, labeling, sorted_layer_dict, etype_to_predecessorsteps)
            if not has_splitup:
                # Normal structure: plan_step_chains = [[]*]
                plan_step_chains = self.determine_stepchains_for_one_plan(new_pa_plan, sorted_layer_dict)
            else:
                # new plan structure: plan_step_chains = [[...]*,[[[...],[...]],i,j]?]
                #                                        12   2*,234   4,4   43    2?1
                #   1.layer = independent chains
                #   2.layer = related steps of one chain
                #   3.layer = independent arm of one 'aa'-step-chain
                #   4.layer = left and right chain of 'aa'-chain
                plan_step_chains = self.new_determine_stepchains_for_one_plan(new_pa_plan, sorted_layer_dict, etype_to_predecessorsteps)

            chain_counter = 1
            for chain_idx in range(len(plan_step_chains)):
                current_chain = plan_step_chains[chain_idx]
                # For each chain: find Nodes for continuous subchains
                if type(current_chain[0]) is int:
                    nodes_to_steplists, chain_counter = self.create_node_steplist_pairs_for_one_continuous_chain(new_pa_plan, current_chain, chain_counter)
                else:
                    nodes_to_steplists, chain_counter = self.create_node_steplist_pairs_for_aa_chain(new_pa_plan, current_chain, chain_counter)

                # Assign NodeIDs to steps
                for key in nodes_to_steplists:
                    node_to_steplist = nodes_to_steplists[key]
                    for list_index, step_idx in enumerate(node_to_steplist[1]):
                        step = new_pa_plan[step_idx]
                        if step.targetSinkType == 'SiSi':
                            step.targetNode = node_to_steplist[0]
                        if step.type == 'aggr' and step.sourceSinkType == 'SiSi':
                            prev_step = new_pa_plan[etype_to_predecessorsteps[step.sourceEtype][1][0]]
                            step.sourceNode = prev_step.targetNode
            plans.append(new_pa_plan)
        return plans, etype_to_predecessorsteps

    ####################################################################################################
    @staticmethod
    def new_determine_plan_hierarchy(pa_plan):
        etype_plan_layer = {}   # dict: key=etypes and value=layer
        etype_predecessorsteps = {}   # dict: key=etypes and value=indexes of steps with them as the target_etype
        etype_successors = {}   # dict: key=etypes and value=step index and successor etype

        for index, step in enumerate(pa_plan):
            if step[0] == 'push':
                etype_plan_layer[step[2]] = 1
                for etype in step[1]:
                    etype_successors[etype] = [index, step[2]]
                if step[2] not in etype_predecessorsteps:
                    etype_predecessorsteps[step[2]] = []
                etype_predecessorsteps[step[2]].append(index)
            else:   # step[0] == 'aggr'
                etype_successors[step[4]] = [index, step[2]]
                if step[4] not in etype_plan_layer:
                    etype_plan_layer[step[4]] = 1
                if step[2] not in etype_plan_layer or etype_plan_layer[step[2]] < etype_plan_layer[step[4]] + 1:
                    etype_plan_layer[step[2]] = etype_plan_layer[step[4]] + 1
                if step[2] not in etype_predecessorsteps:
                    etype_predecessorsteps[step[2]] = []
                etype_predecessorsteps[step[2]].append(index)
                target_etype = step[2]
                while target_etype in etype_successors:
                    etype_to_update = etype_successors[target_etype][1]

                    if etype_to_update not in etype_plan_layer:
                        break
                    if etype_plan_layer[etype_to_update] < etype_plan_layer[target_etype] + 1:
                        etype_plan_layer[etype_to_update] = etype_plan_layer[target_etype] + 1
                    target_etype = pa_plan[etype_successors[target_etype][0]][2]

        return etype_plan_layer, etype_predecessorsteps, etype_successors

    ####################################################################################################
    @staticmethod
    def format_pa_plan(pa_plan, sorted_layer_dict, etype_predecessorsteps):
        """
        Dotdict-attributes \n
        push-step: type, aggregate, targetEtype, targetSinkType, targetNode \n
        aggr-step: type, aggregate, targetEtype, sendRateEtype, sourceEtype, targetSinkType, sourceSinkType, targetNode, sourceNode\n
        :param pa_plan: pa_plan with steps
        :param sorted_layer_dict:
        :param etype_predecessorsteps:
        :return: steps as Dotdicts in a new order
        """
        new_pa_plan = []
        new_etype_predecessor_steps = {}
        index_counter = 0
        for index, key in enumerate(sorted_layer_dict):
            new_predecessor_indexes = ['', []]
            for step_index in etype_predecessorsteps[key]:
                step = pa_plan[step_index]
                new_predecessor_indexes[0] += step[0][0]   # 'p' / 'a'
                new_predecessor_indexes[1].append(index_counter)
                index_counter += 1
                if key == step[2]:
                    if step[0] == 'push':
                        new_step = {'type': step[0], 'aggregate': step[1], 'targetEtype': step[2], 'targetSinkType': None, 'targetNode': None}
                    else:
                        new_step = {'type': step[0], 'aggregate': step[1], 'targetEtype': step[2], 'sendRateEtype': step[3], 'sourceEtype': step[4], 'targetSinkType': None, 'sourceSinkType': None, 'targetNode': None, 'sourceNode': None}
                    new_step = Dotdict(new_step)
                    new_pa_plan.append(new_step)
            new_etype_predecessor_steps[key] = new_predecessor_indexes
        return new_pa_plan, new_etype_predecessor_steps

    ####################################################################################################
    @staticmethod
    def apply_labeling_to_plan(pa_plan, labeling, sorted_layer_dict, etype_predecessorsteps):
        """
        Note: Labeling is string of 0s and 1s where 0 = MuSi, 1 = SiSi

        :param pa_plan: steps as Dotdicts
        :param labeling: binary-string
        :param sorted_layer_dict: sorted etype layers for pa_plan
        :param etype_predecessorsteps: predecessor steps for target_etypes
        :return: pa_plan with applied labeling
        """
        for index, key in enumerate(sorted_layer_dict):
            for step_index in etype_predecessorsteps[key][1]:
                step = pa_plan[step_index]
                if key == step.targetEtype:
                    if step.type == 'push':
                        if labeling[index] == '0':
                            step.targetSinkType = 'MuSi'
                        else:
                            step.targetSinkType = 'SiSi'
                    else:
                        if labeling[index] == '0':
                            step.targetSinkType = 'MuSi'
                            prev_step_targetSinkType = pa_plan[etype_predecessorsteps[step.sourceEtype][1][0]].targetSinkType
                            step.sourceSinkType = prev_step_targetSinkType
                        else:
                            step.targetSinkType = 'SiSi'
                            prev_step_targetSinkType = pa_plan[etype_predecessorsteps[step.sourceEtype][1][0]].targetSinkType
                            step.sourceSinkType = prev_step_targetSinkType
        return pa_plan

    ####################################################################################################
    def new_determine_stepchains_for_one_plan(self, pa_plan, sorted_layer_dict, etype_predecessorsteps):
        step_chains = []
        one_chain_steps = []
        highest_key = list(sorted_layer_dict)[-1]

        last_step_steptypes = etype_predecessorsteps[highest_key][0]
        last_step_stepindexes = etype_predecessorsteps[highest_key][1]
        if last_step_steptypes == 'paa':
            left_step_index = last_step_stepindexes[1]
            right_step_index = last_step_stepindexes[2]
        else:
            left_step_index = last_step_stepindexes[0]
            right_step_index = last_step_stepindexes[1]

        left_step = pa_plan[left_step_index]
        left_next_etype = left_step.sourceEtype
        right_step = pa_plan[right_step_index]
        right_next_etype = right_step.sourceEtype

        # Find first step chain including last_steps (if existent)
        if left_step.targetSinkType == 'MuSi':   # Both are XXXX->MuSi: left and right unconnected
            if left_step.sourceSinkType == 'SiSi':   # left is SiSi->MuSi: traversing the left side
                one_chain_steps, left_next_etype = self.determine_chain_from_given_SiSi_etype_downwards(pa_plan, etype_predecessorsteps, left_next_etype)
                one_chain_steps.append(left_step_index)
                step_chains.append(one_chain_steps)
            if right_step.sourceSinkType == 'SiSi':   # right is SiSi->MuSi: traversing the right side
                one_chain_steps, right_next_etype = self.determine_chain_from_given_SiSi_etype_downwards(pa_plan, etype_predecessorsteps, right_next_etype)
                one_chain_steps.append(right_step_index)
                step_chains.append(one_chain_steps)
            # Note: if both are False 1.1 is covered  and  if both are True then 3.2 is covered
        else:   # Both are XXXX->SiSi: left and right connected
            if left_step.sourceSinkType == 'MuSi' and right_step.sourceSinkType == 'MuSi':   # Both are MuSi->SiSi  (Case: 2.1 - connected complete chain)
                one_chain_steps = []
            elif left_step.sourceSinkType == 'MuSi':   # left is MuSi->SiSi, right is SiSi-SiSi: traversing the right side
                one_chain_steps, right_next_etype = self.determine_chain_from_given_SiSi_etype_downwards(pa_plan, etype_predecessorsteps, right_next_etype)
            elif right_step.sourceSinkType == 'MuSi':   # right is MuSi->SiSi, left is SiSi-SiSi: traversing the left side
                one_chain_steps, left_next_etype = self.determine_chain_from_given_SiSi_etype_downwards(pa_plan, etype_predecessorsteps, left_next_etype)
            else:   # both are SiSi->SiSi: traverse both sides and put together as [[leftchain], [rightchain]]
                left_step_chain, left_next_etype = self.determine_chain_from_given_SiSi_etype_downwards(pa_plan, etype_predecessorsteps, left_next_etype)
                right_step_chain, right_next_etype = self.determine_chain_from_given_SiSi_etype_downwards(pa_plan, etype_predecessorsteps, right_next_etype)
                one_chain_steps.append([left_step_chain, right_step_chain])
            one_chain_steps.extend(last_step_stepindexes)   # end step_chain with both stepindexes
            step_chains.append(one_chain_steps)

        # TODO: check traversal correctness
        left_step_steptypes = etype_predecessorsteps[left_next_etype][0]
        left_step_indexes = etype_predecessorsteps[left_next_etype][1]

        if left_step_steptypes != 'p':   # must be 'a' or 'pa' step because nothing to do if only push step (since current step is MuSi-step)
            while sorted_layer_dict[left_next_etype] != 1:
                aggr_step = None
                aggr_step_index = None
                if len(left_step_indexes) > 2:
                    print(f"ERROR: {left_next_etype} has too many predecessors in {etype_predecessorsteps}")
                for index in range(len(left_step_indexes)):
                    step = pa_plan[left_step_indexes[index]]
                    if step.type == 'aggr':
                        aggr_step = step
                        aggr_step_index = left_step_indexes[index]
                left_next_etype = aggr_step.sourceEtype
                left_step_indexes = etype_predecessorsteps[left_next_etype][1]
                while aggr_step.sourceSinkType == 'MuSi':
                    aggr_step = None
                    if len(left_step_indexes) > 2:
                        print(f"ERROR: {left_next_etype} has too many predecessors in {etype_predecessorsteps}")
                    for index in range(len(left_step_indexes)):
                        step = pa_plan[left_step_indexes[index]]
                        if step.type == 'aggr':
                            aggr_step = step
                            aggr_step_index = left_step_indexes[index]
                    if aggr_step is None:   # only push step
                        left_next_etype = None
                        break
                    left_next_etype = aggr_step.sourceEtype
                    left_step_indexes = etype_predecessorsteps[left_next_etype][1]
                if left_next_etype is not None:
                    left_next_etype = aggr_step.sourceEtype
                    left_chain, left_next_etype = self.determine_chain_from_given_SiSi_etype_downwards(pa_plan, etype_predecessorsteps, left_next_etype)
                    left_chain.append(aggr_step_index)
                    step_chains.append(left_chain)
                    if sorted_layer_dict[left_next_etype] == 1:
                        break
                else:
                    break

        right_step_steptypes = etype_predecessorsteps[right_next_etype][0]
        right_step_indexes = etype_predecessorsteps[right_next_etype][1]

        if right_step_steptypes != 'p':   # must be 'a' or 'pa' step because nothing to do if only push step (since current step is MuSi-step)
            while sorted_layer_dict[right_next_etype] != 1:
                aggr_step = None
                aggr_step_index = None
                if len(right_step_indexes) > 2:
                    print(f"ERROR: {right_next_etype} has too many predecessors in {etype_predecessorsteps}")
                for index in range(len(right_step_indexes)):
                    step = pa_plan[right_step_indexes[index]]
                    if step.type == 'aggr':
                        aggr_step = step
                        aggr_step_index = right_step_indexes[index]
                right_next_etype = aggr_step.sourceEtype
                right_step_indexes = etype_predecessorsteps[right_next_etype][1]
                while aggr_step.sourceSinkType == 'MuSi':
                    aggr_step = None
                    if len(right_step_indexes) > 2:
                        print(f"ERROR: {right_next_etype} has too many predecessors in {etype_predecessorsteps}")
                    for index in range(len(right_step_indexes)):
                        step = pa_plan[right_step_indexes[index]]
                        if step.type == 'aggr':
                            aggr_step = step
                            aggr_step_index = right_step_indexes[index]
                    if aggr_step is None:   # only push step
                        right_next_etype = None
                        break
                    right_next_etype = aggr_step.sourceEtype
                    right_step_indexes = etype_predecessorsteps[right_next_etype][1]
                if right_next_etype is not None:
                    right_next_etype = aggr_step.sourceEtype
                    right_chain, right_next_etype = self.determine_chain_from_given_SiSi_etype_downwards(pa_plan, etype_predecessorsteps, right_next_etype)
                    right_chain.append(aggr_step_index)
                    step_chains.append(right_chain)
                    if sorted_layer_dict[right_next_etype] == 1:
                        break
                else:
                    break

        return step_chains

    ####################################################################################################
    @staticmethod
    def determine_chain_from_given_SiSi_etype_downwards(pa_plan, etype_predecessorsteps, end_chain_end_etype):
        # requires end_chain_end_etype to be target_etype of 'SiSi'-step -> targetSinkType == 'SiSi'
        stepchain = []

        last_step_steptypes = etype_predecessorsteps[end_chain_end_etype][0]
        last_step_stepindexes = etype_predecessorsteps[end_chain_end_etype][1]

        aggr_step = None
        if len(last_step_stepindexes) > 2:
            print(f"ERROR: {end_chain_end_etype} has too many predecessors in {etype_predecessorsteps}")
        for index in range(len(last_step_stepindexes)):
            step = pa_plan[last_step_stepindexes[index]]
            if step.targetSinkType != 'SiSi':
                print(f"ERROR: {end_chain_end_etype} is not a SiSi step")
                return [], end_chain_end_etype
            if step.type == 'aggr':
                aggr_step = step

        current_chain_etype = end_chain_end_etype   # just if output is cut short

        if last_step_steptypes == 'p' or (last_step_steptypes == 'a' and aggr_step.sourceSinkType == 'MuSi'):
            # first SiSi-step is also last step of chain (either no step before or step before is MuSi)
            stepchain.append(last_step_stepindexes[0])
            if last_step_steptypes == 'a':
                current_chain_etype = aggr_step.sourceEtype
        elif last_step_steptypes == 'a' or last_step_steptypes == 'pa' or last_step_steptypes == 'ap':
            stepchain.extend(last_step_stepindexes)   # order of push and aggr should not matter here
            current_chain_etype = aggr_step.sourceEtype
            while aggr_step.sourceSinkType == 'SiSi':
                current_chain_etype = aggr_step.sourceEtype
                current_step_stepindexes = etype_predecessorsteps[current_chain_etype][1]
                push_step = None
                aggr_step = None
                if len(current_step_stepindexes) > 2:
                    print(f"ERROR: {end_chain_end_etype} has too many predecessors in {etype_predecessorsteps}")
                for index in range(len(current_step_stepindexes)):
                    step = pa_plan[current_step_stepindexes[index]]
                    if step.type == 'aggr':
                        aggr_step = step
                    else:
                        push_step = step
                if aggr_step is None:
                    if push_step.targetSinkType == 'SiSi':
                        stepchain.append(current_step_stepindexes[0])
                    break
                stepchain.extend(current_step_stepindexes)
                current_chain_etype = aggr_step.sourceEtype
        else:
            print(f"ERROR: Something went wrong during downwards traversal")

        stepchain.reverse()   # traversed downwards -> chain order needed upwards
        return stepchain, current_chain_etype

    ####################################################################################################
    @staticmethod
    def determine_stepchains_for_one_plan(pa_plan, sorted_layer_dict):
        # Input : pa_plan with Dotdict steps; sorted etype layers for pa_plan
        # Output: list of all continuous SiSi step chains in the pa_plan
        step_chains = []
        one_chain_steps = []

        # INFO: step chains need to be separated better
        for key in sorted_layer_dict:
            for idx, step in enumerate(pa_plan):
                if key == step.targetEtype:
                    # Step_chain represented as list of planstep-indexes
                    if step.targetSinkType == 'SiSi':
                        one_chain_steps.append(idx)
                    elif step.type == 'aggr' and len(one_chain_steps) != 0:
                        if step.sourceSinkType == 'SiSi':
                            one_chain_steps.append(idx)
                            step_chains.append(one_chain_steps)
                            one_chain_steps = []
        if len(one_chain_steps) != 0:
            step_chains.append(one_chain_steps)
        return step_chains

    ####################################################################################################
    def create_node_steplist_pairs_for_one_continuous_chain(self, pa_plan, stepindex_chain, chain_counter):
        # Input : pa_plan with Dotdict steps; one continuous chain of stepindex(es); current chain counter for this plan
        # Output: dict where subchain sequence number (= incrementing chain_counter) is the key and list [[nodeID],[included stepindex(es) as list]] is the value
        nodes_to_steplists = {}
        overlapping_nodes = []
        steplist = []
        best_node_for_steplist = []
        send_etypes_for_steplist = {}
        for step_idx in stepindex_chain:
            step = pa_plan[step_idx]
            if not overlapping_nodes:   # first step in subchain (overlapping is empty)
                steplist.append(step_idx)
                overlapping_nodes = self.event_sourced_network.eventtype_to_nodes[step.targetEtype]
                send_etypes_for_steplist = self.update_send_etypes_for_steplist_for_first_step(overlapping_nodes, send_etypes_for_steplist, step)
            else:
                current_possible_nodes = self.event_sourced_network.eventtype_to_nodes[step.targetEtype]
                if step.targetSinkType == "MuSi" and (set(overlapping_nodes) & set(current_possible_nodes)) == set():
                    current_possible_nodes = self.event_sourced_network.eventtype_to_nodes[step.sourceEtype]
                if (set(overlapping_nodes) & set(current_possible_nodes)) == set():
                    best_node = self.find_best_node_from_overlapping_nodes(overlapping_nodes, send_etypes_for_steplist)
                    best_node_for_steplist.append(best_node)
                    best_node_for_steplist.append(steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1
                    overlapping_nodes = current_possible_nodes
                    steplist = [step_idx]
                    send_etypes_for_steplist = {}
                    send_etypes_for_steplist = self.update_send_etypes_for_steplist_for_first_step(overlapping_nodes, send_etypes_for_steplist, step)
                    best_node_for_steplist = []
                else:
                    overlapping_nodes = list(set(overlapping_nodes) & set(current_possible_nodes))
                    steplist.append(step_idx)
                    send_etypes_for_steplist = self.update_send_etypes_for_steplist_for_subsequent_steps(send_etypes_for_steplist, step)


        if steplist:   # if steplist is the entire chain or cutoff was before last step of complete chain
            best_node = self.find_best_node_from_overlapping_nodes(overlapping_nodes, send_etypes_for_steplist)
            best_node_for_steplist.append(best_node)
            best_node_for_steplist.append(steplist)
            nodes_to_steplists[chain_counter] = best_node_for_steplist

        return nodes_to_steplists, chain_counter

    ####################################################################################################
    def create_node_steplist_pairs_for_aa_chain(self, pa_plan, stepindex_chain, chain_counter):
        nodes_to_steplists = {}
        best_node_for_steplist = []

        if len(stepindex_chain) == 4:
            left_aa_stepindex = stepindex_chain[2]
            right_aa_stepindex = stepindex_chain[3]
            left_and_right_stepindex = [stepindex_chain[1], left_aa_stepindex, right_aa_stepindex]
        else:
            left_aa_stepindex = stepindex_chain[1]
            right_aa_stepindex = stepindex_chain[2]
            left_and_right_stepindex = [left_aa_stepindex, right_aa_stepindex]

        left_step_chain = stepindex_chain[0][0]
        right_step_chain = stepindex_chain[0][1]

        left_side_overlapping_nodes = []
        left_steplist = []
        left_send_etypes_for_steplist = {}
        for step_idx in left_step_chain:
            step = pa_plan[step_idx]
            if not left_side_overlapping_nodes:   # first step in subchain (overlapping is empty)
                left_steplist.append(step_idx)
                left_side_overlapping_nodes = self.event_sourced_network.eventtype_to_nodes[step.targetEtype]
                left_send_etypes_for_steplist = self.update_send_etypes_for_steplist_for_first_step(left_side_overlapping_nodes, left_send_etypes_for_steplist, step)
            else:
                current_possible_nodes = self.event_sourced_network.eventtype_to_nodes[step.targetEtype]
                if (set(left_side_overlapping_nodes) & set(current_possible_nodes)) == set():
                    best_node = self.find_best_node_from_overlapping_nodes(left_side_overlapping_nodes, left_send_etypes_for_steplist)
                    best_node_for_steplist.append(best_node)
                    best_node_for_steplist.append(left_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1
                    left_side_overlapping_nodes = current_possible_nodes
                    left_steplist = [step_idx]
                    left_send_etypes_for_steplist = {}
                    left_send_etypes_for_steplist = self.update_send_etypes_for_steplist_for_first_step(left_side_overlapping_nodes, left_send_etypes_for_steplist, step)
                    best_node_for_steplist = []
                else:
                    left_side_overlapping_nodes = list(set(left_side_overlapping_nodes) & set(current_possible_nodes))
                    left_steplist.append(step_idx)
                    left_send_etypes_for_steplist = self.update_send_etypes_for_steplist_for_subsequent_steps(left_send_etypes_for_steplist, step)

        right_side_overlapping_nodes = []
        right_steplist = []
        right_send_etypes_for_steplist = {}
        for step_idx in right_step_chain:
            step = pa_plan[step_idx]
            if not right_side_overlapping_nodes:   # first step in subchain (overlapping is empty)
                right_steplist.append(step_idx)
                right_side_overlapping_nodes = self.event_sourced_network.eventtype_to_nodes[step.targetEtype]
                right_send_etypes_for_steplist = self.update_send_etypes_for_steplist_for_first_step(right_side_overlapping_nodes, right_send_etypes_for_steplist, step)
            else:
                current_possible_nodes = self.event_sourced_network.eventtype_to_nodes[step.targetEtype]
                if (set(right_side_overlapping_nodes) & set(current_possible_nodes)) == set():
                    best_node = self.find_best_node_from_overlapping_nodes(right_side_overlapping_nodes, right_send_etypes_for_steplist)
                    best_node_for_steplist.append(best_node)
                    best_node_for_steplist.append(right_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1
                    right_side_overlapping_nodes = current_possible_nodes
                    right_steplist = [step_idx]
                    right_send_etypes_for_steplist = {}
                    right_send_etypes_for_steplist = self.update_send_etypes_for_steplist_for_first_step(right_side_overlapping_nodes, right_send_etypes_for_steplist, step)
                    best_node_for_steplist = []
                else:
                    right_side_overlapping_nodes = list(set(right_side_overlapping_nodes) & set(current_possible_nodes))
                    right_steplist.append(step_idx)
                    right_send_etypes_for_steplist = self.update_send_etypes_for_steplist_for_subsequent_steps(right_send_etypes_for_steplist, step)

        aa_step_target_etype = pa_plan[left_aa_stepindex].targetEtype   # right_aa_step and left_aa_step have same targetEtype
        aa_step_nodes = self.event_sourced_network.eventtype_to_nodes[aa_step_target_etype]
        # TODO: for aa step that doesnt have a step before it in the chain: need to consider sourceEtype too
        # For now ignore aa-step and just consider left and right

        # calculate for overlap of left with aa
        left_overlap = list(set(left_side_overlapping_nodes) & set(aa_step_nodes))
        # calculate for overlap of right with aa
        right_overlap = list(set(right_side_overlapping_nodes) & set(aa_step_nodes))

        # TODO: Cleanup
        if (set(left_side_overlapping_nodes) & set(right_side_overlapping_nodes)) == set():   # left and right part at aa-step have no overlap
            #print(f"Entering Case 1")
            if left_overlap != [] and right_overlap != []:   # both have no overlap with aa-step
                # -> choose the best node of both and other side chooses the best node without aa-step
                best_left_node = self.find_best_node_from_overlapping_nodes(left_overlap, left_send_etypes_for_steplist)
                best_right_node = self.find_best_node_from_overlapping_nodes(right_overlap, right_send_etypes_for_steplist)
                left_savings = self.calculate_saving_for_node_and_etypelist(best_left_node, left_send_etypes_for_steplist)
                right_savings = self.calculate_saving_for_node_and_etypelist(best_right_node, right_send_etypes_for_steplist)
                if left_savings > right_savings:
                    new_right_node = self.find_best_node_from_overlapping_nodes(right_side_overlapping_nodes, right_send_etypes_for_steplist)
                    best_node_for_steplist.append(new_right_node)
                    best_node_for_steplist.append(right_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1
                    best_node_for_steplist = []

                    left_steplist.extend(left_and_right_stepindex)
                    best_node_for_steplist.append(best_left_node)
                    best_node_for_steplist.append(left_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1

                else:
                    new_left_node = self.find_best_node_from_overlapping_nodes(left_side_overlapping_nodes, left_send_etypes_for_steplist)
                    best_node_for_steplist.append(new_left_node)
                    best_node_for_steplist.append(left_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1
                    best_node_for_steplist = []

                    right_steplist.extend(left_and_right_stepindex)
                    best_node_for_steplist.append(best_right_node)
                    best_node_for_steplist.append(right_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1

            elif left_overlap:   # only left has overlap with aa
                best_right_node = self.find_best_node_from_overlapping_nodes(right_side_overlapping_nodes, right_send_etypes_for_steplist)
                best_node_for_steplist.append(best_right_node)
                best_node_for_steplist.append(right_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1
                best_node_for_steplist = []

                best_left_node = self.find_best_node_from_overlapping_nodes(left_overlap, left_send_etypes_for_steplist)
                left_steplist.extend(left_and_right_stepindex)
                best_node_for_steplist.append(best_left_node)
                best_node_for_steplist.append(left_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1

            elif right_overlap:   # only right has overlap with aa
                best_left_node = self.find_best_node_from_overlapping_nodes(left_side_overlapping_nodes, left_send_etypes_for_steplist)
                best_node_for_steplist.append(best_left_node)
                best_node_for_steplist.append(left_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1
                best_node_for_steplist = []

                best_right_node = self.find_best_node_from_overlapping_nodes(right_overlap, right_send_etypes_for_steplist)
                right_steplist.extend(left_and_right_stepindex)
                best_node_for_steplist.append(best_right_node)
                best_node_for_steplist.append(right_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1

            else:   # neither has overlap with aa-step -> 3 separate chains with individual nodes
                best_left_node = self.find_best_node_from_overlapping_nodes(left_side_overlapping_nodes, left_send_etypes_for_steplist)
                best_node_for_steplist.append(best_left_node)
                best_node_for_steplist.append(left_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1
                best_node_for_steplist = []

                best_right_node = self.find_best_node_from_overlapping_nodes(right_side_overlapping_nodes, right_send_etypes_for_steplist)
                best_node_for_steplist.append(best_right_node)
                best_node_for_steplist.append(right_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1
                best_node_for_steplist = []

                best_aa_node = aa_step_nodes[0]   # for now just choose first node in the list (since no saving is possible here?)
                best_node_for_steplist.append(best_aa_node)
                best_node_for_steplist.append(left_and_right_stepindex)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1

        else:   # left and right have overlap
            if (set(left_side_overlapping_nodes) & set(right_side_overlapping_nodes) & set(aa_step_nodes)) != set():   # left-right-aa overlap exists
                all_steps_overlapping_nodes = list(set(left_side_overlapping_nodes) & set(right_side_overlapping_nodes) & set(aa_step_nodes))
                all_send_etypes = dict(left_send_etypes_for_steplist)
                for key in right_send_etypes_for_steplist:
                    if key in left_send_etypes_for_steplist:
                        all_send_etypes[key] += right_send_etypes_for_steplist[key]
                    else:
                        all_send_etypes[key] = right_send_etypes_for_steplist[key]
                best_aa_node = self.find_best_node_from_overlapping_nodes(all_steps_overlapping_nodes, all_send_etypes)
                best_node_for_steplist.append(best_aa_node)
                steplist = left_steplist + right_steplist + left_and_right_stepindex
                best_node_for_steplist.append(steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1

            elif left_overlap != [] and right_overlap != []:   # both have overlap with aa-step
                best_left_node = self.find_best_node_from_overlapping_nodes(left_overlap, left_send_etypes_for_steplist)
                best_right_node = self.find_best_node_from_overlapping_nodes(right_overlap, right_send_etypes_for_steplist)
                left_savings = self.calculate_saving_for_node_and_etypelist(best_left_node, left_send_etypes_for_steplist)
                right_savings = self.calculate_saving_for_node_and_etypelist(best_right_node, right_send_etypes_for_steplist)
                if left_savings > right_savings:
                    new_right_node = self.find_best_node_from_overlapping_nodes(right_side_overlapping_nodes, right_send_etypes_for_steplist)
                    best_node_for_steplist.append(new_right_node)
                    best_node_for_steplist.append(right_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1
                    best_node_for_steplist = []

                    left_steplist.extend(left_and_right_stepindex)
                    best_node_for_steplist.append(best_left_node)
                    best_node_for_steplist.append(left_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1

                else:
                    new_left_node = self.find_best_node_from_overlapping_nodes(left_side_overlapping_nodes, left_send_etypes_for_steplist)
                    best_node_for_steplist.append(new_left_node)
                    best_node_for_steplist.append(left_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1
                    best_node_for_steplist = []

                    right_steplist.extend(left_and_right_stepindex)
                    best_node_for_steplist.append(best_right_node)
                    best_node_for_steplist.append(right_steplist)
                    nodes_to_steplists[chain_counter] = best_node_for_steplist
                    chain_counter += 1

            elif left_overlap:   # only left has overlap with aa
                best_right_node = self.find_best_node_from_overlapping_nodes(right_side_overlapping_nodes, right_send_etypes_for_steplist)
                best_node_for_steplist.append(best_right_node)
                best_node_for_steplist.append(right_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1
                best_node_for_steplist = []

                best_left_node = self.find_best_node_from_overlapping_nodes(left_overlap, left_send_etypes_for_steplist)
                left_steplist.extend(left_and_right_stepindex)
                best_node_for_steplist.append(best_left_node)
                best_node_for_steplist.append(left_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1

            elif right_overlap:   # only right has overlap with aa
                best_left_node = self.find_best_node_from_overlapping_nodes(left_side_overlapping_nodes, left_send_etypes_for_steplist)
                best_node_for_steplist.append(best_left_node)
                best_node_for_steplist.append(left_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1
                best_node_for_steplist = []

                best_right_node = self.find_best_node_from_overlapping_nodes(right_overlap, right_send_etypes_for_steplist)
                right_steplist.extend(left_and_right_stepindex)
                best_node_for_steplist.append(best_right_node)
                best_node_for_steplist.append(right_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1

            else:   # neither has overlap with aa-step -> 3 separate chains with individual nodes
                best_left_node = self.find_best_node_from_overlapping_nodes(left_side_overlapping_nodes, left_send_etypes_for_steplist)
                best_node_for_steplist.append(best_left_node)
                best_node_for_steplist.append(left_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1
                best_node_for_steplist = []

                best_right_node = self.find_best_node_from_overlapping_nodes(right_side_overlapping_nodes, right_send_etypes_for_steplist)
                best_node_for_steplist.append(best_right_node)
                best_node_for_steplist.append(right_steplist)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1
                best_node_for_steplist = []

                best_aa_node = aa_step_nodes[0]   # for now just choose first node in the list (since no saving is possible here?)
                best_node_for_steplist.append(best_aa_node)
                best_node_for_steplist.append(left_and_right_stepindex)
                nodes_to_steplists[chain_counter] = best_node_for_steplist
                chain_counter += 1

        return nodes_to_steplists, chain_counter

    ####################################################################################################
    def find_best_node_from_overlapping_nodes(self, overlapping_nodes, send_etypes_for_steplist):
        if len(overlapping_nodes) > 1:
            best_node = 0
            best_saving = 0
            for node in overlapping_nodes:
                savings = 0
                node_rates = self.event_sourced_network.nodes[node-1]
                for key_etype in send_etypes_for_steplist:
                    if key_etype == 'push':
                        for etype in send_etypes_for_steplist[key_etype]:
                            savings += node_rates[(ord(etype)-65)]
                    elif node_rates[(ord(key_etype)-65)] != 0:
                        for etype in send_etypes_for_steplist[key_etype]:
                            savings += self.event_sourced_network.eventtype_to_local_outputrate[etype]
                if savings > best_saving:
                    best_saving = savings
                    best_node = node
            send_etypes_for_steplist = {}
        else:
            best_node = overlapping_nodes[0]
        return best_node

    ####################################################################################################
    def calculate_saving_for_node_and_etypelist(self, node, etypes):
        saving = 0
        if type(node) is not int:
            print(f"ERROR: 'calculate_saving_for_node_and_etypelist' did not get an int NodeID: {node}")
        node_rates = self.event_sourced_network.nodes[node-1]
        for key_etype in etypes:
            if key_etype == 'push':
                for etype in etypes[key_etype]:
                    saving += node_rates[(ord(etype)-65)]
            elif node_rates[(ord(key_etype)-65)] != 0:
                for etype in etypes[key_etype]:
                    saving += self.event_sourced_network.eventtype_to_local_outputrate[etype]
        return saving

    ####################################################################################################
    def update_send_etypes_for_steplist_for_first_step(self, overlapping_nodes, send_etypes_for_steplist, step):
        if step.type == 'push':   # has to be SiSi-Step to be included here
            send_etypes_for_steplist['push'] = step.aggregate
        else:   # == 'aggr'
            nodes_for_source_etype = self.event_sourced_network.eventtype_to_nodes[step.sourceEtype]
            if (set(overlapping_nodes) & set(nodes_for_source_etype)) == set():
                overlapping_nodes = [overlapping_nodes[0]]   # INFO: for this further optimization could be done here
            overlapping_nodes = list(set(overlapping_nodes) & set(nodes_for_source_etype))
            if step.currSinkType == 'MuSi':   # SiSi -> MuSi (last MuSi step of a SiSi-Chain)
                num_send_etype_nodes = len(self.event_sourced_network.eventtype_to_nodes[step.sendRateEtype])
                send_etypes_for_steplist[step.sourceEtype] = num_send_etype_nodes * step.sendRateEtype   # = global rate send_etype
            else:
                if step.prevSinkType == 'MuSi':   # MuSi -> SiSi
                    if step.sendRateEtype != step.sourceEtype:
                        num_send_etype_nodes = len(self.event_sourced_network.eventtype_to_nodes[step.sendRateEtype])
                        send_etypes_for_steplist[step.sourceEtype] = num_send_etype_nodes * step.sendRateEtype   # = global rate send_etype
                    else:
                        send_etypes_for_steplist[step.sourceEtype] = step.sendRateEtype
                else:   # SiSi -> SiSi
                    num_send_etype_nodes = len(self.event_sourced_network.eventtype_to_nodes[step.sendRateEtype])
                    send_etypes_for_steplist[step.sourceEtype] = num_send_etype_nodes * step.sendRateEtype
        return send_etypes_for_steplist

    ####################################################################################################
    def update_send_etypes_for_steplist_for_subsequent_steps(self, send_etypes_for_steplist, step):
        if step.type == 'push':   # has to be SiSi-Step to be included here
            if 'push' in send_etypes_for_steplist:
                send_etypes_for_steplist['push'] += step.aggregate
            else:
                send_etypes_for_steplist['push'] = step.aggregate
        else:
            if step.currSinkType == 'MuSi':   # SiSi -> MuSi (last MuSi step of a SiSi-Chain)
                num_send_etype_nodes = len(self.event_sourced_network.eventtype_to_nodes[step.sendRateEtype])
                if step.sourceEtype in send_etypes_for_steplist:
                    send_etypes_for_steplist[step.sourceEtype] += num_send_etype_nodes * step.sendRateEtype
                else:
                    send_etypes_for_steplist[step.sourceEtype] = num_send_etype_nodes * step.sendRateEtype
            else:
                if step.prevSinkType == 'MuSi':   # MuSi -> SiSi
                    if step.sendRateEtype != step.sourceEtype:
                        num_send_etype_nodes = len(self.event_sourced_network.eventtype_to_nodes[step.sendRateEtype])
                        if step.sourceEtype in send_etypes_for_steplist:
                            send_etypes_for_steplist[step.sourceEtype] += num_send_etype_nodes * step.sendRateEtype
                        else:
                            send_etypes_for_steplist[step.sourceEtype] = num_send_etype_nodes * step.sendRateEtype
                    else:
                        if step.sourceEtype in send_etypes_for_steplist:
                            send_etypes_for_steplist[step.sourceEtype] += step.sendRateEtype
                        else:
                            send_etypes_for_steplist[step.sourceEtype] = step.sendRateEtype
                else:   # SiSi -> SiSi
                    num_send_etype_nodes = len(self.event_sourced_network.eventtype_to_nodes[step.sendRateEtype])
                    if step.sourceEtype in send_etypes_for_steplist:
                        send_etypes_for_steplist[step.sourceEtype] += num_send_etype_nodes * step.sendRateEtype
                    else:
                        send_etypes_for_steplist[step.sourceEtype] = num_send_etype_nodes * step.sendRateEtype
        return send_etypes_for_steplist

    ####################################################################################################
    def determine_PA_Plan_wSiSi_costs_of_entire_esn(self, pa_plan, etype_to_predecessorsteps):
        push_costs = 0
        aggregation_costs = 0
        is_first_aa_step = True
        for step in pa_plan:
            debug_cost = 0
            ############################################################################################################
            if step.type == 'push':
                num_target_nodes = len(self.event_sourced_network.eventtype_to_nodes[step.targetEtype])
                ########################################################################################################
                if step.targetSinkType == 'MuSi':
                    # Costfunction += R(sendEtype) * |f(targetEtype)| - r(sendEtype) * |f(sendEtype,targetEtype)|   {for all sendEtype in step.aggregate}
                    for etype_to_send in step.aggregate:
                        num_nodes_producing_both_etypes = self.event_sourced_network.nodes_producing_both_eventtypes(etype_to_send, step.targetEtype)
                        global_rate_send_etype          = self.event_sourced_network.eventtype_to_global_outputrate[etype_to_send]
                        local_rate_send_etype           = self.event_sourced_network.eventtype_to_local_outputrate[etype_to_send]
                        push_costs += global_rate_send_etype * num_target_nodes - num_nodes_producing_both_etypes * local_rate_send_etype
                        debug_cost += global_rate_send_etype * num_target_nodes - num_nodes_producing_both_etypes * local_rate_send_etype
                ########################################################################################################
                elif step.targetSinkType == 'SiSi':
                    # Costfunction += Sum(R(X)) - Sum(r(X))
                    #       {Sum(R(X)) = Sum of global rates, Sum(r(X)) = Sum of local rates | for all X in step.aggregate on targetNode}
                    # Costfunction += (|f(targetEtype)| - 1) * r(targetEtype)
                    #       {add aggregation_cost for targetEtype only for first push_step; for all other steps this cost is included in the aggr_step}
                    target_node_rates = self.event_sourced_network.nodes[step.targetNode-1]
                    for etype_to_send in step.aggregate:
                        global_rate_send_etype = self.event_sourced_network.eventtype_to_global_outputrate[etype_to_send]
                        push_costs += global_rate_send_etype - target_node_rates[(ord(etype_to_send)-65)]
                        debug_cost += global_rate_send_etype - target_node_rates[(ord(etype_to_send)-65)]
                        #print(f"push_costs += {debug_cost} = {global_rate_send_etype} - {target_node_rates[(ord(etype_to_send)-65)]}")
                    if etype_to_predecessorsteps[step.targetEtype][0] == 'p':
                        local_rate_target_etype  = self.event_sourced_network.eventtype_to_local_outputrate[step.targetEtype]
                        push_costs += (num_target_nodes - 1) * local_rate_target_etype
                        debug_cost += (num_target_nodes - 1) * local_rate_target_etype
                #step.cost = debug_cost
            ############################################################################################################
            elif step.type == 'aggr':
                num_target_nodes = len(self.event_sourced_network.eventtype_to_nodes[step.targetEtype])
                num_source_nodes = len(self.event_sourced_network.eventtype_to_nodes[step.sourceEtype])
                ########################################################################################################
                if step.targetSinkType == 'MuSi':
                    ####################################################################################################
                    if step.sourceSinkType == 'MuSi':   # MuSi->MuSi
                        # Costfunction = R(sendEtype) * |f(targetEtype)| * |f(sourceEtype)| - |f(targetEtype,sourceEtype)| * R(sendEtype)   ; if sendEtype != sourceEtype
                        #      or
                        # Costfunction = r(sendEtype) * |f(targetEtype)| * |f(sourceEtype)| - |f(targetEtype,sourceEtype)| * r(sendEtype)   ; if sendEtype = sourceEtype
                        num_nodes_producing_both_etypes = self.event_sourced_network.nodes_producing_both_eventtypes(step.sourceEtype, step.targetEtype)
                        if step.sendRateEtype != step.sourceEtype:
                            global_rate_send_etype = self.event_sourced_network.eventtype_to_global_outputrate[step.sendRateEtype]
                            aggregation_costs += global_rate_send_etype * num_source_nodes * num_target_nodes - num_nodes_producing_both_etypes * global_rate_send_etype
                            debug_cost += global_rate_send_etype * num_source_nodes * num_target_nodes - num_nodes_producing_both_etypes * global_rate_send_etype
                        else:
                            local_rate_send_etype = self.event_sourced_network.eventtype_to_local_outputrate[step.sendRateEtype]
                            aggregation_costs += local_rate_send_etype * num_source_nodes * num_target_nodes - num_nodes_producing_both_etypes * local_rate_send_etype
                            debug_cost += local_rate_send_etype * num_source_nodes * num_target_nodes - num_nodes_producing_both_etypes * local_rate_send_etype
                    ####################################################################################################
                    elif step.sourceSinkType == 'SiSi':   # SiSi->MuSi:
                        # Costfunction = R(sendEtype) * |f(targetEtype)| [ - R(sendEtype) | if f(targetEtype) != 0 on sourceNode]
                        all_rates_for_sisi_node = self.event_sourced_network.nodes[step.sourceNode-1]
                        global_rate_send_etype  = self.event_sourced_network.eventtype_to_global_outputrate[step.sendRateEtype]
                        aggregation_costs += global_rate_send_etype * num_target_nodes
                        debug_cost += global_rate_send_etype * num_target_nodes
                        if all_rates_for_sisi_node[(ord(step.targetEtype) - 65)] != 0:
                            aggregation_costs += - global_rate_send_etype
                            debug_cost += - global_rate_send_etype
                ########################################################################################################
                elif step.targetSinkType == 'SiSi':
                    local_rate_target_node_etype = self.event_sourced_network.eventtype_to_local_outputrate[step.targetEtype]
                    ####################################################################################################
                    if step.sourceSinkType == 'MuSi':   # MuSi->SiSi
                        # Costfunction += R(sendEtype) * |f(sourceEtype)| [ - R(sendEtype) | if f(sourceEtype) != 0 on targetNode]   ; if sendEtype != sourceEtype
                        #      or
                        # Costfunction += R(sendEtype)                    [ - r(sendEtype) | if f(sourceEtype) != 0 on targetNode]   ; if sendEtype = sourceEtype
                        # Costfunction += (|f(targetEtype)| - 1) * r(targetEtype)
                        target_node_rates = self.event_sourced_network.nodes[step.targetNode-1]
                        global_rate_send_etype = self.event_sourced_network.eventtype_to_global_outputrate[step.sendRateEtype]
                        local_rate_send_etype = self.event_sourced_network.eventtype_to_local_outputrate[step.sendRateEtype]
                        if step.sourceEtype != step.sendRateEtype:
                            aggregation_costs += global_rate_send_etype * num_source_nodes
                            debug_cost += global_rate_send_etype * num_source_nodes
                            if target_node_rates[(ord(step.sourceEtype) - 65)] != 0:
                                aggregation_costs += - global_rate_send_etype
                                debug_cost += - global_rate_send_etype
                        else:
                            aggregation_costs += global_rate_send_etype
                            debug_cost += global_rate_send_etype
                            if target_node_rates[(ord(step.sourceEtype) - 65)] != 0:
                                aggregation_costs += - local_rate_send_etype
                                debug_cost += - local_rate_send_etype
                        aggregation_costs += (num_target_nodes - 1) * local_rate_target_node_etype
                        debug_cost += (num_target_nodes - 1) * local_rate_target_node_etype
                    ####################################################################################################
                    elif step.sourceSinkType == 'SiSi':   # SiSi->SiSi
                        # Costfunction += R(sendEtype) + (|f(targetEtype)| - 1) * r(targetEtype) [ - R(sendEtype) | if sourceNode == targetNode]
                        global_rate_send_etype       = self.event_sourced_network.eventtype_to_global_outputrate[step.sendRateEtype]
                        aggregation_costs += global_rate_send_etype + (num_target_nodes - 1) * local_rate_target_node_etype
                        debug_cost += global_rate_send_etype + (num_target_nodes - 1) * local_rate_target_node_etype
                        if step.targetNode == step.sourceNode:
                            aggregation_costs += - global_rate_send_etype
                            debug_cost += - global_rate_send_etype
                    if is_first_aa_step and (etype_to_predecessorsteps[step.targetEtype][0] == 'aa' or etype_to_predecessorsteps[step.targetEtype][0] == 'paa'):
                        aggregation_costs += - (num_target_nodes - 1) * local_rate_target_node_etype
                        debug_cost += - (num_target_nodes - 1) * local_rate_target_node_etype
                        is_first_aa_step = False
            #step.cost = debug_cost
            ############################################################################################################
        return push_costs + aggregation_costs

    #############################################cd#########################################################################
    ################################################# Stevens Code Section #################################################
    ########################################################################################################################

    def partition_by_magnitude(self):
        partitioned = {}

        for value in self.event_sourced_network.eventtype_to_global_outputrate.values():
            magnitude = int(math.log10(value))

            if magnitude not in partitioned:
                partitioned[magnitude] = []
            partitioned[magnitude].append(value)

        for magnitude in partitioned:
            partitioned[magnitude].sort()


        for magnitude in sorted(partitioned.keys()):
            print(f"Order of magnitude {magnitude}:")
            for value in partitioned[magnitude]:
                print(f"  {value}")
            print()

        return partitioned

    def determine_different_strong_placement_types(self, oom_threshold):
        def find_adjacent_strong_placements(strong_placements, sequence):
            strong_placements = list(dict.fromkeys(strong_placements))

            char_to_index = {char: index for index, char in enumerate(sequence)}

            result = []
            current_substring = []

            for char in strong_placements:
                if not current_substring:
                    current_substring.append(char)
                elif char_to_index[char] == char_to_index[current_substring[-1]] + 1:
                    current_substring.append(char)
                else:
                    if len(current_substring) > 1:
                        result.append(''.join(current_substring))
                    current_substring = [char]

            if len(current_substring) > 1:
                result.append(''.join(current_substring))

            return result

        def sort_strong_placements_by_sequence_order(strong_placements_list, order_str):
            order_dict = {char: idx for idx, char in enumerate(order_str)}

            def sort_key(string):
                return [order_dict.get(char, len(order_str)) for char in string]

            sorted_list = ''.join(sorted(strong_placements_list, key=sort_key))
            return sorted_list

        def determine_strong_placements_through_inequality():
            inequality_strong_placements = ''
            for idx in range(len(self.sequence_pattern)):
                if idx == 0 and self.successor_is_lower(self.sequence_pattern, idx):
                    inequality_strong_placements += self.sequence_pattern[idx]
                elif idx == len(self.sequence_pattern) - 1 and self.predecessor_is_lower(self.sequence_pattern, idx):
                    inequality_strong_placements += self.sequence_pattern[idx]
                elif 0 < idx < len(self.sequence_pattern) - 1:
                    if self.predecessor_is_lower(self.sequence_pattern, idx) and self.successor_is_lower(self.sequence_pattern, idx):
                        inequality_strong_placements += self.sequence_pattern[idx]

            return inequality_strong_placements

        median_global_outputrate = statistics.median(self.event_sourced_network.eventtype_to_global_outputrate.values())
        median_magnitude = int(math.log10(median_global_outputrate))

        strong_placements_median = ''
        for symbol in self.sequence_pattern:
            if int(math.log10(self.event_sourced_network.eventtype_to_global_outputrate[symbol])) >= median_magnitude + oom_threshold:
                strong_placements_median += symbol

        adjacent_strong_placements = find_adjacent_strong_placements(strong_placements_median, self.sequence_pattern)

        for adjacent_strong_placement in adjacent_strong_placements:
            curr_max = 0
            peak_symbol = ''
            for symbol in adjacent_strong_placement:
                if self.event_sourced_network.eventtype_to_global_outputrate[symbol] > curr_max:
                    curr_max = self.event_sourced_network.eventtype_to_global_outputrate[symbol]
                    peak_symbol = symbol

            strong_placements_to_remove = adjacent_strong_placement.replace(peak_symbol, '')

            for strong_placement_to_remove in strong_placements_to_remove:
                strong_placements_median = strong_placements_median.replace(strong_placement_to_remove, '')


        inequality_strong_placements = determine_strong_placements_through_inequality()

        #print(sort_strong_placements_by_sequence_order(inequality_strong_placements,self.sequence_pattern))

        combined_strong_placements = sort_strong_placements_by_sequence_order(''.join(list(set(strong_placements_median + inequality_strong_placements))), self.sequence_pattern)

        #print("strong_placements_median:",strong_placements_median)
        #print("inequality_strong_placements:",inequality_strong_placements)
        #print("combined_strong_placements:",combined_strong_placements)

        return strong_placements_median, inequality_strong_placements, combined_strong_placements

    @staticmethod
    def process_output(input_string):
        lines = input_string.strip().split('\n')
        result = []

        for line in lines:
            if not line.strip():
                continue

            line = line.strip()[1:-1]
            tuples = re.findall(r'\([^)]+\)', line)

            processed_tuples = []
            for t in tuples:
                elements = [elem.strip() for elem in t[1:-1].split(',')]
                elements = [elem.strip("'") for elem in elements]
                processed_tuples.append(tuple(elements))

            result.append(sorted(processed_tuples, key=len))

        return result

    def enumerate_all_PA_plans(self, sequence, strong_placements="", kleene_types="", negation_types=""):
        executable_path = r"./enumerate_plans"
        result = subprocess.run([executable_path, sequence, strong_placements, kleene_types, negation_types], capture_output=True, text=True)
        return self.process_output(result.stdout)

    def determine_best_MuSi_and_MuSiSi_plan(self, all_pa_plans):
        best_musi_plan = {}
        best_musisi_plan = {}
        best_musi_plan_costs = float('inf')
        best_musisi_plan_costs = float('inf')

        for plan_idx, pa_plan in enumerate(all_pa_plans):
            formated_pa_plan = self.prepend_steptype_for_steps_in_pa_plan(pa_plan)
            all_musisi_labelings, etype_to_predecessorsteps = self.build_all_possible_plans_with_SiSi(formated_pa_plan)
            min_musisi_costs = float('inf')
            best_sink_labeling_index = 0
            musi_plan_costs = float('inf')

            for sink_labeling_idx, plan in enumerate(all_musisi_labelings):
                plan_costs = self.determine_PA_Plan_wSiSi_costs_of_entire_esn(plan, etype_to_predecessorsteps)
                if sink_labeling_idx == 0:
                    musi_plan_costs = plan_costs

                if plan_costs <= min_musisi_costs:
                    min_musisi_costs = plan_costs
                    best_sink_labeling_index = sink_labeling_idx

            if musi_plan_costs < best_musi_plan_costs:
                best_musi_plan_costs = musi_plan_costs
                #best_musi_plan = all_musisi_labelings[0]

            if min_musisi_costs < best_musisi_plan_costs:
                best_musisi_plan_costs = min_musisi_costs
                #best_musisi_plan = all_musisi_labelings[best_sink_labeling_index]

        return best_musi_plan_costs, best_musisi_plan_costs

    def determine_best_new_MuSiSi_plan(self, new_musisi_plan):
        if new_musisi_plan == []:
            return float('inf')

        best_new_musisi_plan = {}
        best_new_musisi_plan_costs = float('inf')

        formated_pa_plan = self.prepend_steptype_for_steps_in_pa_plan(new_musisi_plan)
        all_musisi_labelings, etype_to_predecessorsteps = self.build_all_possible_plans_with_SiSi(formated_pa_plan)
        min_musisi_costs = float('inf')
        best_sink_labeling_index = 0

        for sink_labeling_idx, plan in enumerate(all_musisi_labelings):
            plan_costs = self.determine_PA_Plan_wSiSi_costs_of_entire_esn(plan, etype_to_predecessorsteps)

            if plan_costs <= min_musisi_costs:
                min_musisi_costs = plan_costs
                best_sink_labeling_index = sink_labeling_idx

        return min_musisi_costs

########################################################################################################################
################################################## Experiment Section ##################################################
########################################################################################################################

    def run_event_node_ratio_experiment(self):
        results_folder = "results/"
        file_raw_costs = results_folder + "eventratio_experiment_rawcosts.csv"
        header1 = 'eventratio,method,cost'
        file_ratios = results_folder + "eventratio_experiment_tratios.csv"
        header2 = 'eventratio,comparison,ratio'
        with open(file_raw_costs, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header1.split(','))
        with open(file_ratios, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header2.split(','))

        event_ratios = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
        runs = 100

        if len(self.sequence_pattern) <= use_bf_filtering_for_seq_length_greater:
            all_bruteforce_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern)

        print(f"Running run_event_node_ratio_experiment for Sequence of length {len(self.sequence_pattern)}: {self.sequence_pattern}")
        for ratio in event_ratios:
            print(f"Current ratio: {ratio}")
            for _ in range(runs):
                self.event_sourced_network = EventSourcedNetwork(len(self.sequence_pattern), self.number_of_nodes, self.zipfian_parameter, ratio, [])
                if len(self.sequence_pattern) > use_bf_filtering_for_seq_length_greater:
                    sp1, sp2, sp3 = self.determine_different_strong_placement_types(1)
                    filtered_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern, sp3)

                centralized_push_costs = self.determine_centralized_push_costs()

                greedy_pa_plan = self.generate_greedy_plan()
                greedy_pa_plan = self.merge_PPA_plan_steps(greedy_pa_plan)
                greedy_musi_plan_costs = self.determine_PPA_plan_costs_esn_complete_topology(greedy_pa_plan)

                greedy_musisi_plan_costs = centralized_push_costs
                if len(greedy_pa_plan) != len(self.sequence_pattern):
                    formatted_greedy_pa_plan = self.prepend_steptype_for_steps_in_pa_plan(greedy_pa_plan)
                    all_possible_greedy_plans, etype_to_predecessorsteps = self.build_all_possible_plans_with_SiSi(formatted_greedy_pa_plan)
                    greedy_musisi_plan_costs = float('inf')
                    for idx, plan in enumerate(all_possible_greedy_plans):
                        plan_costs = self.determine_PA_Plan_wSiSi_costs_of_entire_esn(plan, etype_to_predecessorsteps)
                        if plan_costs < greedy_musisi_plan_costs:
                            greedy_musisi_plan_costs = plan_costs

                if len(self.sequence_pattern) <= use_bf_filtering_for_seq_length_greater:
                    bf_musi_plan_costs, bf_musisi_plan_costs = self.determine_best_MuSi_and_MuSiSi_plan(all_bruteforce_pa_plans)
                else:
                    bf_musi_plan_costs, bf_musisi_plan_costs = self.determine_best_MuSi_and_MuSiSi_plan(filtered_pa_plans)

                cet_composition = self.min_algorithm_cet_composition(self.sequence_pattern)
                new_musisi_plan = sorted(self.generate_finalplan(self.sequence_pattern,cet_composition), key=len)
                new_musisi_plans_costs = self.determine_best_new_MuSiSi_plan(new_musisi_plan)

                # Failsave section for costs
                if greedy_musi_plan_costs > centralized_push_costs:
                    greedy_musi_plan_costs = centralized_push_costs
                if bf_musi_plan_costs > centralized_push_costs:
                    bf_musi_plan_costs = centralized_push_costs
                if new_musisi_plans_costs > centralized_push_costs:
                    new_musisi_plans_costs = centralized_push_costs

                # save individual costs
                with open(file_raw_costs, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    central = str(ratio) + ",central," + str(centralized_push_costs)
                    writer.writerow(central.split(','))
                    greedy_musi = str(ratio)+ ",greedy_musi," + str(greedy_musi_plan_costs)
                    writer.writerow(greedy_musi.split(','))
                    bf_musi = str(ratio) + ",bf_musi," + str(bf_musi_plan_costs)
                    writer.writerow(bf_musi.split(','))
                    greedy_musisi = str(ratio) + ",greedy_musisi," + str(greedy_musisi_plan_costs)
                    writer.writerow(greedy_musisi.split(','))
                    bf_musisi = str(ratio) + ",bf_musisi," + str(bf_musisi_plan_costs)
                    writer.writerow(bf_musisi.split(','))
                    new_musisi = str(ratio) + ",new_musisi," + str(new_musisi_plans_costs)
                    writer.writerow(new_musisi.split(','))

                # save transmission ratios
                transmission_ratio_greedy_musisi_vs_greedy_musi = greedy_musisi_plan_costs / greedy_musi_plan_costs
                transmission_ratio_bf_musisi_vs_greedy_musi = bf_musisi_plan_costs / greedy_musi_plan_costs
                transmission_ratio_bf_musisi_vs_central = bf_musisi_plan_costs / centralized_push_costs
                transmission_ratio_greedy_musi_vs_central = greedy_musi_plan_costs / centralized_push_costs
                transmission_ratio_bf_musi_vs_central = bf_musi_plan_costs / centralized_push_costs
                transmission_ratio_new_musisi_vs_central = new_musisi_plans_costs / centralized_push_costs
                with open(file_ratios, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    greedy_musisi_greedy_musi = str(ratio) + ',greedymusisi_greedymusi,' + str(min(transmission_ratio_greedy_musisi_vs_greedy_musi, 1.0))
                    writer.writerow(greedy_musisi_greedy_musi.split(','))
                    bfmusisi_greedymusi = str(ratio) + ',bfmusisi_greedymusi,' + str(min(transmission_ratio_bf_musisi_vs_greedy_musi, 1.0))
                    writer.writerow(bfmusisi_greedymusi.split(','))
                    bfmusisi_central = str(ratio) + ',bfmusisi_central,' + str(min(transmission_ratio_bf_musisi_vs_central, 1.0))
                    writer.writerow(bfmusisi_central.split(','))
                    greedymusi_central = str(ratio) + ',greedymusi_central,' + str(min(transmission_ratio_greedy_musi_vs_central, 1.0))
                    writer.writerow(greedymusi_central.split(','))
                    bfmusi_central = str(ratio) + ',bfmusisi_greedymusisi,' + str(min(transmission_ratio_bf_musi_vs_central, 1.0))
                    writer.writerow(bfmusi_central.split(','))
                    newmusisi_central = str(ratio) + ',newmusisi_central,' + str(min(transmission_ratio_new_musisi_vs_central, 1.0))
                    writer.writerow(newmusisi_central.split(','))
                    
                    
                    

    def run_network_sizes_experiment(self):
        results_folder = "results/"
        file_raw_costs = results_folder + "networksize_experiment_rawcosts.csv"
        header1 = 'network_size,method,cost'
        file_ratios = results_folder + "networksize_experiment_tratios.csv"
        header2 = 'network_size,comparison,ratio'
        with open(file_raw_costs, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header1.split(','))
        with open(file_ratios, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header2.split(','))

        network_sizes = [5, 10, 20, 50, 100, 200, 500]
        runs = 100

        if len(self.sequence_pattern) <= use_bf_filtering_for_seq_length_greater:
            all_bruteforce_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern)

        print(f"Running run_network_sizes_experiment for Sequence of length {len(self.sequence_pattern)}: {self.sequence_pattern}")
        for i in range(runs):
            print(f"Current run: {i:3d}")
            self.event_sourced_network = EventSourcedNetwork(len(self.sequence_pattern), 5, self.zipfian_parameter, self.event_node_ratio, [])
            first_rates = self.event_sourced_network.local_output_rates
            if len(self.sequence_pattern) > use_bf_filtering_for_seq_length_greater:
                sp1, sp2, sp3 = self.determine_different_strong_placement_types(1)
                filtered_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern, sp3)

            for size in network_sizes:
                self.number_of_nodes = size
                self.event_sourced_network = EventSourcedNetwork(len(self.sequence_pattern), size, self.zipfian_parameter, self.event_node_ratio, first_rates)

                centralized_push_costs = self.determine_centralized_push_costs()

                greedy_pa_plan = self.generate_greedy_plan()
                greedy_pa_plan = self.merge_PPA_plan_steps(greedy_pa_plan)
                greedy_musi_plan_costs = self.determine_PPA_plan_costs_esn_complete_topology(greedy_pa_plan)

                greedy_musisi_plan_costs = centralized_push_costs
                if len(greedy_pa_plan) != len(self.sequence_pattern):
                    formatted_greedy_pa_plan = self.prepend_steptype_for_steps_in_pa_plan(greedy_pa_plan)
                    all_possible_greedy_plans, etype_to_predecessorsteps = self.build_all_possible_plans_with_SiSi(formatted_greedy_pa_plan)
                    greedy_musisi_plan_costs = float('inf')
                    for idx, plan in enumerate(all_possible_greedy_plans):
                        plan_costs = self.determine_PA_Plan_wSiSi_costs_of_entire_esn(plan, etype_to_predecessorsteps)
                        if plan_costs < greedy_musisi_plan_costs:
                            greedy_musisi_plan_costs = plan_costs

                if len(self.sequence_pattern) <= use_bf_filtering_for_seq_length_greater:
                    bf_musi_plan_costs, bf_musisi_plan_costs = self.determine_best_MuSi_and_MuSiSi_plan(all_bruteforce_pa_plans)
                else:
                    bf_musi_plan_costs, bf_musisi_plan_costs = self.determine_best_MuSi_and_MuSiSi_plan(filtered_pa_plans)

                cet_composition = self.min_algorithm_cet_composition(self.sequence_pattern)
                new_musisi_plan = sorted(self.generate_finalplan(self.sequence_pattern,cet_composition), key=len)
                new_musisi_plans_costs = self.determine_best_new_MuSiSi_plan(new_musisi_plan)

                # Failsave section for costs
                if greedy_musi_plan_costs > centralized_push_costs:
                    greedy_musi_plan_costs = centralized_push_costs
                if bf_musi_plan_costs > centralized_push_costs:
                    bf_musi_plan_costs = centralized_push_costs
                if new_musisi_plans_costs > centralized_push_costs:
                    new_musisi_plans_costs = centralized_push_costs

                # save individual costs
                with open(file_raw_costs, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    central = str(size) + ",central," + str(centralized_push_costs)
                    writer.writerow(central.split(','))
                    greedy_musi = str(size)+ ",greedy_musi," + str(greedy_musi_plan_costs)
                    writer.writerow(greedy_musi.split(','))
                    bf_musi = str(size) + ",bf_musi," + str(bf_musi_plan_costs)
                    writer.writerow(bf_musi.split(','))
                    greedy_musisi = str(size) + ",greedy_musisi," + str(greedy_musisi_plan_costs)
                    writer.writerow(greedy_musisi.split(','))
                    bf_musisi = str(size) + ",bf_musisi," + str(bf_musisi_plan_costs)
                    writer.writerow(bf_musisi.split(','))
                    new_musisi = str(size) + ",new_musisi," + str(new_musisi_plans_costs)
                    writer.writerow(new_musisi.split(','))

                # save transmission ratios
                transmission_ratio_greedy_musisi_vs_greedy_musi = greedy_musisi_plan_costs / greedy_musi_plan_costs
                transmission_ratio_bf_musisi_vs_greedy_musi = bf_musisi_plan_costs / greedy_musi_plan_costs
                transmission_ratio_bf_musisi_vs_central = bf_musisi_plan_costs / centralized_push_costs
                transmission_ratio_greedy_musi_vs_central = greedy_musi_plan_costs / centralized_push_costs
                transmission_ratio_bf_musi_vs_central = bf_musi_plan_costs / centralized_push_costs
                transmission_ratio_new_musisi_vs_central = new_musisi_plans_costs / centralized_push_costs
                with open(file_ratios, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    greedy_musisi_greedy_musi = str(size) + ',greedymusisi_greedymusi,' + str(min(transmission_ratio_greedy_musisi_vs_greedy_musi, 1.0))
                    writer.writerow(greedy_musisi_greedy_musi.split(','))
                    bfmusisi_greedymusi = str(size) + ',bfmusisi_greedymusi,' + str(min(transmission_ratio_bf_musisi_vs_greedy_musi, 1.0))
                    writer.writerow(bfmusisi_greedymusi.split(','))
                    bfmusisi_central = str(size) + ',bfmusisi_central,' + str(min(transmission_ratio_bf_musisi_vs_central, 1.0))
                    writer.writerow(bfmusisi_central.split(','))
                    greedymusi_central = str(size) + ',greedymusi_central,' + str(min(transmission_ratio_greedy_musi_vs_central, 1.0))
                    writer.writerow(greedymusi_central.split(','))
                    bfmusi_central = str(size) + ',bfmusisi_greedymusisi,' + str(min(transmission_ratio_bf_musi_vs_central, 1.0))
                    writer.writerow(bfmusi_central.split(','))
                    newmusisi_central = str(size) + ',newmusisi_central,' + str(min(transmission_ratio_new_musisi_vs_central, 1.0))
                    writer.writerow(newmusisi_central.split(','))                    


    def run_sequence_length_experiment(self):
        results_folder = "results/"
        file_raw_costs = results_folder + "seqlen_experiment_rawcosts.csv"
        header1 = 'seqlen,method,cost'
        file_ratios = results_folder + "seqlen_experiment_tratios.csv"
        header2 = 'seqlen,comparison,ratio'
        with open(file_raw_costs, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header1.split(','))
        with open(file_ratios, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header2.split(','))

        sequence = self.sequence_pattern
        print(f"Running run_sequence_length_experiment up to Sequence of length {len(sequence)}: {sequence}")
        for length in range(3, len(sequence)+1):
            print(f"Current length: {length:2d}")
            if length <= use_bf_filtering_for_seq_length_greater:
                print("Using complete bruteforce")
                all_bruteforce_pa_plans = self.enumerate_all_PA_plans(sequence[0:length])
            else:
                print("Using filtered bruteforce")

            for _ in range(100):
                self.sequence_pattern = sequence[0:length]
                self.event_sourced_network = EventSourcedNetwork(length, self.number_of_nodes, self.zipfian_parameter, self.event_node_ratio, [])
                if length > use_bf_filtering_for_seq_length_greater:
                    sp1, sp2, sp3 = self.determine_different_strong_placement_types(1)
                    filtered_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern, sp3)

                centralized_push_costs = self.determine_centralized_push_costs()

                greedy_pa_plan = self.generate_greedy_plan()
                greedy_pa_plan = self.merge_PPA_plan_steps(greedy_pa_plan)
                greedy_musi_plan_costs = self.determine_PPA_plan_costs_esn_complete_topology(greedy_pa_plan)

                greedy_musisi_plan_costs = centralized_push_costs
                if len(greedy_pa_plan) != len(self.sequence_pattern):
                    formatted_greedy_pa_plan = self.prepend_steptype_for_steps_in_pa_plan(greedy_pa_plan)
                    all_possible_greedy_plans, etype_to_predecessorsteps = self.build_all_possible_plans_with_SiSi(formatted_greedy_pa_plan)
                    greedy_musisi_plan_costs = float('inf')
                    for idx, plan in enumerate(all_possible_greedy_plans):
                        plan_costs = self.determine_PA_Plan_wSiSi_costs_of_entire_esn(plan, etype_to_predecessorsteps)
                        if plan_costs < greedy_musisi_plan_costs:
                            greedy_musisi_plan_costs = plan_costs

                if len(self.sequence_pattern) <= use_bf_filtering_for_seq_length_greater:
                    bf_musi_plan_costs, bf_musisi_plan_costs = self.determine_best_MuSi_and_MuSiSi_plan(all_bruteforce_pa_plans)
                else:
                    bf_musi_plan_costs, bf_musisi_plan_costs = self.determine_best_MuSi_and_MuSiSi_plan(filtered_pa_plans)

                cet_composition = self.min_algorithm_cet_composition(self.sequence_pattern)
                new_musisi_plan = sorted(self.generate_finalplan(self.sequence_pattern,cet_composition), key=len)
                new_musisi_plans_costs = self.determine_best_new_MuSiSi_plan(new_musisi_plan)

                # Failsave section for costs
                if greedy_musi_plan_costs > centralized_push_costs:
                    greedy_musi_plan_costs = centralized_push_costs
                if bf_musi_plan_costs > centralized_push_costs:
                    bf_musi_plan_costs = centralized_push_costs
                if new_musisi_plans_costs > centralized_push_costs:
                    new_musisi_plans_costs = centralized_push_costs

                # save individual costs
                with open(file_raw_costs, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    central = str(length) + ",central," + str(centralized_push_costs)
                    writer.writerow(central.split(','))
                    greedy_musi = str(length)+ ",greedy_musi," + str(greedy_musi_plan_costs)
                    writer.writerow(greedy_musi.split(','))
                    bf_musi = str(length) + ",bf_musi," + str(bf_musi_plan_costs)
                    writer.writerow(bf_musi.split(','))
                    greedy_musisi = str(length) + ",greedy_musisi," + str(greedy_musisi_plan_costs)
                    writer.writerow(greedy_musisi.split(','))
                    bf_musisi = str(length) + ",bf_musisi," + str(bf_musisi_plan_costs)
                    writer.writerow(bf_musisi.split(','))
                    new_musisi = str(length) + ",new_musisi," + str(new_musisi_plans_costs)
                    writer.writerow(new_musisi.split(','))

                # save transmission ratios
                transmission_ratio_greedy_musisi_vs_greedy_musi = greedy_musisi_plan_costs / greedy_musi_plan_costs
                transmission_ratio_bf_musisi_vs_greedy_musi = bf_musisi_plan_costs / greedy_musi_plan_costs
                transmission_ratio_bf_musisi_vs_central = bf_musisi_plan_costs / centralized_push_costs
                transmission_ratio_greedy_musi_vs_central = greedy_musi_plan_costs / centralized_push_costs
                transmission_ratio_bf_musi_vs_central = bf_musi_plan_costs / centralized_push_costs
                transmission_ratio_new_musisi_vs_central = new_musisi_plans_costs / centralized_push_costs
                with open(file_ratios, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    greedy_musisi_greedy_musi = str(length) + ',greedymusisi_greedymusi,' + str(min(transmission_ratio_greedy_musisi_vs_greedy_musi, 1.0))
                    writer.writerow(greedy_musisi_greedy_musi.split(','))
                    bfmusisi_greedymusi = str(length) + ',bfmusisi_greedymusi,' + str(min(transmission_ratio_bf_musisi_vs_greedy_musi, 1.0))
                    writer.writerow(bfmusisi_greedymusi.split(','))
                    bfmusisi_central = str(length) + ',bfmusisi_central,' + str(min(transmission_ratio_bf_musisi_vs_central, 1.0))
                    writer.writerow(bfmusisi_central.split(','))
                    greedymusi_central = str(length) + ',greedymusi_central,' + str(min(transmission_ratio_greedy_musi_vs_central, 1.0))
                    writer.writerow(greedymusi_central.split(','))
                    bfmusi_central = str(length) + ',bfmusisi_greedymusisi,' + str(min(transmission_ratio_bf_musi_vs_central, 1.0))
                    writer.writerow(bfmusi_central.split(','))
                    newmusisi_central = str(length) + ',newmusisi_central,' + str(min(transmission_ratio_new_musisi_vs_central, 1.0))
                    writer.writerow(newmusisi_central.split(','))


    def run_event_skew_experiment(self):
        results_folder = "results/"
        file_raw_costs = results_folder + "skew_experiment_rawcosts.csv"
        header1 = 'skew,method,cost'
        file_ratios = results_folder + "skew_experiment_tratios.csv"
        header2 = 'skew,comparison,ratio'
        with open(file_raw_costs, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header1.split(','))
        with open(file_ratios, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header2.split(','))

        skews = np.arange(1.1, 2.01, 0.1)
        for i, skew in enumerate(skews):
            skews[i] = round(skew, 1)

        if len(self.sequence_pattern) <= use_bf_filtering_for_seq_length_greater:
            all_bruteforce_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern)

        print(f"Running run_event_skew_experiment for Sequence of length {len(self.sequence_pattern)}: {self.sequence_pattern}")
        for skew in skews:
            print(f"Current skew: {round(skew, 1)}")
            for i in range(100):
                self.event_sourced_network = EventSourcedNetwork(len(self.sequence_pattern), self.number_of_nodes, skew, self.event_node_ratio, [])
                if len(self.sequence_pattern) > use_bf_filtering_for_seq_length_greater:
                    sp1, sp2, sp3 = self.determine_different_strong_placement_types(1)
                    filtered_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern, sp3)

                centralized_push_costs = self.determine_centralized_push_costs()

                greedy_pa_plan = self.generate_greedy_plan()
                greedy_pa_plan = self.merge_PPA_plan_steps(greedy_pa_plan)
                greedy_musi_plan_costs = self.determine_PPA_plan_costs_esn_complete_topology(greedy_pa_plan)

                greedy_musisi_plan_costs = centralized_push_costs
                if len(greedy_pa_plan) != len(self.sequence_pattern):
                    formatted_greedy_pa_plan = self.prepend_steptype_for_steps_in_pa_plan(greedy_pa_plan)
                    all_possible_greedy_plans, etype_to_predecessorsteps = self.build_all_possible_plans_with_SiSi(formatted_greedy_pa_plan)
                    greedy_musisi_plan_costs = float('inf')
                    for idx, plan in enumerate(all_possible_greedy_plans):
                        plan_costs = self.determine_PA_Plan_wSiSi_costs_of_entire_esn(plan, etype_to_predecessorsteps)
                        if plan_costs < greedy_musisi_plan_costs:
                            greedy_musisi_plan_costs = plan_costs

                if len(self.sequence_pattern) <= use_bf_filtering_for_seq_length_greater:
                    bf_musi_plan_costs, bf_musisi_plan_costs = self.determine_best_MuSi_and_MuSiSi_plan(all_bruteforce_pa_plans)
                else:
                    bf_musi_plan_costs, bf_musisi_plan_costs = self.determine_best_MuSi_and_MuSiSi_plan(filtered_pa_plans)

                cet_composition = self.min_algorithm_cet_composition(self.sequence_pattern)
                new_musisi_plan = sorted(self.generate_finalplan(self.sequence_pattern,cet_composition), key=len)
                new_musisi_plans_costs = self.determine_best_new_MuSiSi_plan(new_musisi_plan)

                # Failsave section for costs
                if greedy_musi_plan_costs > centralized_push_costs:
                    greedy_musi_plan_costs = centralized_push_costs
                if bf_musi_plan_costs > centralized_push_costs:
                    bf_musi_plan_costs = centralized_push_costs
                if new_musisi_plans_costs > centralized_push_costs:
                    new_musisi_plans_costs = centralized_push_costs

                # save individual costs
                with open(file_raw_costs, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    central = str(skew) + ",central," + str(centralized_push_costs)
                    writer.writerow(central.split(','))
                    greedy_musi = str(skew)+ ",greedy_musi," + str(greedy_musi_plan_costs)
                    writer.writerow(greedy_musi.split(','))
                    bf_musi = str(skew) + ",bf_musi," + str(bf_musi_plan_costs)
                    writer.writerow(bf_musi.split(','))
                    greedy_musisi = str(skew) + ",greedy_musisi," + str(greedy_musisi_plan_costs)
                    writer.writerow(greedy_musisi.split(','))
                    bf_musisi = str(skew) + ",bf_musisi," + str(bf_musisi_plan_costs)
                    writer.writerow(bf_musisi.split(','))
                    new_musisi = str(skew) + ",new_musisi," + str(new_musisi_plans_costs)
                    writer.writerow(new_musisi.split(','))

                # save transmission ratios
                transmission_ratio_greedy_musisi_vs_greedy_musi = greedy_musisi_plan_costs / greedy_musi_plan_costs
                transmission_ratio_bf_musisi_vs_greedy_musi = bf_musisi_plan_costs / greedy_musi_plan_costs
                transmission_ratio_bf_musisi_vs_central = bf_musisi_plan_costs / centralized_push_costs
                transmission_ratio_greedy_musi_vs_central = greedy_musi_plan_costs / centralized_push_costs
                transmission_ratio_bf_musi_vs_central = bf_musi_plan_costs / centralized_push_costs
                transmission_ratio_new_musisi_vs_central = new_musisi_plans_costs / centralized_push_costs
                with open(file_ratios, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    greedy_musisi_greedy_musi = str(skew) + ',greedymusisi_greedymusi,' + str(min(transmission_ratio_greedy_musisi_vs_greedy_musi, 1.0))
                    writer.writerow(greedy_musisi_greedy_musi.split(','))
                    bfmusisi_greedymusi = str(skew) + ',bfmusisi_greedymusi,' + str(min(transmission_ratio_bf_musisi_vs_greedy_musi, 1.0))
                    writer.writerow(bfmusisi_greedymusi.split(','))
                    bfmusisi_central = str(skew) + ',bfmusisi_central,' + str(min(transmission_ratio_bf_musisi_vs_central, 1.0))
                    writer.writerow(bfmusisi_central.split(','))
                    greedymusi_central = str(skew) + ',greedymusi_central,' + str(min(transmission_ratio_greedy_musi_vs_central, 1.0))
                    writer.writerow(greedymusi_central.split(','))
                    bfmusi_central = str(skew) + ',bfmusisi_greedymusisi,' + str(min(transmission_ratio_bf_musi_vs_central, 1.0))
                    writer.writerow(bfmusi_central.split(','))
                    newmusisi_central = str(skew) + ',newmusisi_central,' + str(min(transmission_ratio_new_musisi_vs_central, 1.0))
                    writer.writerow(newmusisi_central.split(','))


########################################################################################################################
##################################################### Main  Method #####################################################
########################################################################################################################

def main():
    sequence_length = 8
    sequence = ''
    for idx in range(sequence_length):
        sequence += chr(65+idx)
    number_of_nodes = 50
    # The next 2 parameters are just to pregenerate a network
    zipfian_parameter = 1.3
    event_node_ratio = 0.5

    pa_plan_generator = PushPullAggregationPlanner(sequence, number_of_nodes, zipfian_parameter, event_node_ratio)
    #pa_plan_generator.run_event_node_ratio_experiment()
    #pa_plan_generator.run_network_sizes_experiment()
    #pa_plan_generator.run_sequence_length_experiment()
    pa_plan_generator.run_event_skew_experiment()

if __name__ == "__main__":
    main()
