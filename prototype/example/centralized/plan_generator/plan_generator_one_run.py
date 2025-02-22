from event_sourced_network import EventSourcedNetwork
import re
import subprocess
import math
import statistics
import copy
import sys
import os

#################################################### Helper Methods ####################################################

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

########################################################################################################################
################################################## MuSi_only  Section ##################################################
########################################################################################################################

class PushPullAggregationPlanner:
    def __init__(self, sequence_pattern, number_of_nodes, zipfian_parameter, event_node_ratio, stream_size):
        self.sequence_pattern = sequence_pattern
        self.number_of_nodes = number_of_nodes
        self.zipfian_parameter = zipfian_parameter
        self.event_node_ratio = event_node_ratio
        self.stream_size = stream_size
        self.event_sourced_network = EventSourcedNetwork(len(self.sequence_pattern), number_of_nodes, zipfian_parameter, event_node_ratio, [], stream_size)

    
    def min_algorithm_cet_composition(self):
        cet_composition = ''
        for idx in range(0,len(self.sequence_pattern)-1):
            if self.event_sourced_network.eventtype_to_global_outputrate[self.sequence_pattern[idx]] < self.event_sourced_network.eventtype_to_local_outputrate[self.sequence_pattern[idx+1]]:
                cet_composition += self.sequence_pattern[idx]
            else:
                cet_composition += self.sequence_pattern[idx+1]
                
        return cet_composition

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

        # TODO: here is still some problem with the plan creation (Placements and Aggregates)

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
    ################################################## MuSi/SiSi  Section ##################################################
    ########################################################################################################################
    # TODO: Documentation
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
            #print_plan_with_dict_steps(new_pa_plan)
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

    #############################################cd###########################################################################
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

    def enumerate_all_PA_plans(self, sequence, strong_placements=""):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        executable_path = os.path.join(script_dir, "enumerate_plans")
        #executable_path = r"/home/spurtzel/phd/distributed_summaries/sebastian_code/plan_generator/enumerate_plans"
        result = subprocess.run([executable_path, sequence, strong_placements], capture_output=True, text=True)
        return self.process_output(result.stdout)

    def determine_best_MuSiSi_plan(self, all_pa_plans):
        best_pa_plan = {}
        best_pa_plan_costs = float('inf')
        best_musisi_labeling_for_every_plan = []
        for plan_idx, pa_plan in enumerate(all_pa_plans):
            pa_plan = self.prepend_steptype_for_steps_in_pa_plan(pa_plan)
            all_possible_plans, etype_to_predecessorsteps = self.build_all_possible_plans_with_SiSi(pa_plan)
            min_plan_costs = float('inf')
            best_sink_labeling_index = 0
            for sink_labeling_idx, plan in enumerate(all_possible_plans):
                plan_costs = self.determine_PA_Plan_wSiSi_costs_of_entire_esn(plan, etype_to_predecessorsteps)
                if plan_costs <= min_plan_costs:
                    min_plan_costs = plan_costs
                    best_sink_labeling_index = sink_labeling_idx

            best_musisi_labeling_for_every_plan.append((all_possible_plans[best_sink_labeling_index], min_plan_costs))
            #print("--------------------------------------------------------------------------------")
            
            #print("For plan:",pa_plan,"we have cost:",min_plan_costs)
            #print("The evaluation plan is:")
            #for k in all_possible_plans[best_sink_labeling_index]:
                #print(k)
            if min_plan_costs < best_pa_plan_costs:
                best_pa_plan_costs = min_plan_costs
                best_pa_plan = all_possible_plans[best_sink_labeling_index]
            #print("--------------------------------------------------------------------------------")
        # print(best_musisi_labeling_for_every_plan)
        # best_musisi_labeling_for_every_plan = sorted(best_musisi_labeling_for_every_plan, key=lambda tup:tup[1])
        # for plan in best_musisi_labeling_for_every_plan[:10]:
        #     print_plan_with_dict_steps(plan[0], plan[1], delim='\n')
        # print()
        # if len(best_musisi_labeling_for_every_plan) > 5:
        #     print(best_musisi_labeling_for_every_plan[4][1], ":\n", best_musisi_labeling_for_every_plan[4][0])
        return best_pa_plan, best_pa_plan_costs

    def determine_best_MuSi_plan(self, all_pa_plans):
        best_pa_plan = {}
        best_pa_plan_costs = float('inf')
        every_plan = []
        for plan_idx, pa_plan in enumerate(all_pa_plans):
            greedy_pa_plan_musi_only_costs = self.determine_PPA_plan_costs_esn_complete_topology(pa_plan)

            every_plan.append((pa_plan, greedy_pa_plan_musi_only_costs))
            if greedy_pa_plan_musi_only_costs < best_pa_plan_costs:
                best_pa_plan_costs = greedy_pa_plan_musi_only_costs
                best_pa_plan = pa_plan

        # print(every_plan)
        # best_musi_plans = sorted(every_plan, key=lambda tup:tup[1])
        # for plan in best_musi_plans[:20]:
        #     print(plan[1], ":", plan[0])
        # print()

        return best_pa_plan, best_pa_plan_costs

    ########################################################################################################################
    ################################################## Experiment Section ##################################################
    ########################################################################################################################
    
    def plan_modification(self, plan):
        for step in plan:
            if step['targetSinkType'] == 'SiSi':
                step['targetNode'] -= 1
            
            if 'sourceSinkType' in step:
                if step['sourceSinkType'] == 'SiSi':
                    step['sourceNode'] -= 1
    
    def determine_aggregate_evaluation(self, plan):
        placements = []
        longest_aggr = None

        # Process individual aggregation steps.
        for item in plan:
            if item.get('type') == 'aggr':
                placements.append((
                    item['aggregate'],
                    item['sourceEtype'],
                    item['sourceNode'],
                    item['sendRateEtype']
                ))
                # Track the aggr step with the longest aggregate.
                if longest_aggr is None or len(item['aggregate']) > len(longest_aggr['aggregate']):
                    longest_aggr = item

        if longest_aggr:
            # Save targetEtype and targetNode from the longest aggregation step.
            final_target_etype = longest_aggr['targetEtype']
            final_target_node = longest_aggr['targetNode']

            # Collect aggregates from all steps (push and aggr) with matching targetEtype.
            # Also include the final targetEtype itself.
            collected = final_target_etype
            for item in plan:
                if item.get('targetEtype') == final_target_etype and 'aggregate' in item:
                    collected += item['aggregate']

            # Sort the concatenated characters.
            final_aggregate = ''.join(sorted(collected))

            # Final tuple: (final aggregate, targetEtype, targetNode, targetEtype)
            placements.append((final_aggregate, final_target_etype, final_target_node, final_target_etype))
            
        return str(placements)
    

    def generate_plan(self):
        print()
        print_network = True

        if print_network:
            print(self.event_sourced_network.eventtype_to_local_outputrate)
            print(self.event_sourced_network.eventtype_to_global_outputrate)
            print(self.event_sourced_network.nodes)
            print(self.event_sourced_network.eventtype_to_nodes)
            print()

        centralized_push_costs = self.determine_centralized_push_costs()
        '''
        # Greedy MuSi-Only
        greedy_pa_plan = self.generate_greedy_plan()
        greedy_pa_plan = self.merge_PPA_plan_steps(greedy_pa_plan)
        greedy_pa_plan_musi_only_costs = self.determine_PPA_plan_costs_esn_complete_topology(greedy_pa_plan)

        # Greedy MuSi-SiSi
        #greedy_pa_plan = [('B', 'A'), ('D', 'E'), ('AB', 'C', 'B', 'A'), ('DE', 'C', 'D', 'E')]
        formatted_greedy_pa_plan = self.prepend_steptype_for_steps_in_pa_plan(greedy_pa_plan)
        best_greedy_musisi_plan = formatted_greedy_pa_plan
        if len(greedy_pa_plan) == len(self.sequence_pattern):
            greedy_pa_plan_musisi_costs = centralized_push_costs
        else:
            all_possible_greedy_plans, etype_to_predecessorsteps = self.build_all_possible_plans_with_SiSi(formatted_greedy_pa_plan)
            greedy_pa_plan_musisi_costs = float('inf')
            for idx, plan in enumerate(all_possible_greedy_plans):
                plan_costs = self.determine_PA_Plan_wSiSi_costs_of_entire_esn(plan, etype_to_predecessorsteps)
                if plan_costs <= greedy_pa_plan_musisi_costs:
                    greedy_pa_plan_musisi_costs = plan_costs
                    best_greedy_musisi_plan = all_possible_greedy_plans[idx]
            print(), print_plan_with_dict_steps(best_greedy_musisi_plan, greedy_pa_plan_musisi_costs, delim='\n')'''

        # Filtered Bruteforce
        #sp1, sp2, sp3 = self.determine_different_strong_placement_types(1)
        #filtered_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern, sp3)
        #best_bf_filtered_musisi_plan, bf_filtered_pa_plan_musisi_costs = self.determine_best_MuSiSi_plan(filtered_pa_plans)

        # Complete Bruteforce
        all_bruteforce_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern)
        best_bruteforce_musisi_plan, bruteforce_pa_plan_musisi_costs = self.determine_best_MuSiSi_plan(all_bruteforce_pa_plans)
        print(best_bruteforce_musisi_plan)
        self.plan_modification(best_bruteforce_musisi_plan)
        # Bruteforce MuSi only
        #best_bf_musi_plan, best_bf_musi_plan_cost = self.determine_best_MuSi_plan(all_bruteforce_pa_plans)

        if print_network:
            #print(f"Central          : {centralized_push_costs}")
            #print(f"Greedy MuSi_only : {greedy_pa_plan_musi_only_costs}")
            #print(f"Greedy MuSiSi    : {greedy_pa_plan_musisi_costs}")
            #print(f"Bruteforce MuSi  : {best_bf_musi_plan_cost}")
            #print(f"B-Filtered MuSiSi: {bf_filtered_pa_plan_musisi_costs}")
            print(f"Bruteforce MuSiSi: {bruteforce_pa_plan_musisi_costs}")
            print()
            #print(f"Greedy MuSi {greedy_pa_plan_musi_only_costs}:"), [print(f" {idx}: {list(step)}") for idx, step in enumerate(greedy_pa_plan)]
            #print("Greedy MuSiSi", end=''), print_plan_with_dict_steps(best_greedy_musisi_plan, index=greedy_pa_plan_musisi_costs, delim='\n') if isinstance(best_greedy_musisi_plan[0], Dotdict) else (print(f" {greedy_pa_plan_musisi_costs}:"), [print(f" {idx}: {list(step)}") for idx, step in enumerate(greedy_pa_plan)])
            #print(f"Bruteforce MuSi {best_bf_musi_plan_cost}:"), [print(f" {idx}: {list(step)}") for idx, step in enumerate(best_bf_musi_plan)]
            #print("Bf Filtered", end=''), print_plan_with_dict_steps(best_bf_filtered_musisi_plan, index=bf_filtered_pa_plan_musisi_costs, delim='\n')
            print("Bruteforce", end=''), print_plan_with_dict_steps(best_bruteforce_musisi_plan, index=bruteforce_pa_plan_musisi_costs, delim='\n')
            print("Aggregate Placement:", self.determine_aggregate_evaluation(best_bruteforce_musisi_plan))
        with open("generated_network.txt", mode='w') as file:
            file.write(f"query = \"{self.sequence_pattern}\"\n")
            file.write(f"local_rate = {self.event_sourced_network.eventtype_to_local_outputrate}\n")
            file.write(f"global_rate = {self.event_sourced_network.eventtype_to_global_outputrate}\n")
            file.write(f"network = {self.event_sourced_network.nodes}\n")
            for e in self.event_sourced_network.eventtype_to_nodes:
                self.event_sourced_network.eventtype_to_nodes[e] = [x - 1 for x in self.event_sourced_network.eventtype_to_nodes[e]]
            file.write(f"eventtype_to_nodes = {self.event_sourced_network.eventtype_to_nodes}\n\n")

            file.write(f"Central cost({centralized_push_costs})\n\n")
            #file.write(f"Greedy MuSi cost({greedy_pa_plan_musi_only_costs}):\n {greedy_pa_plan}\n\n")
            #file.write(f"Greedy MuSiSi cost({greedy_pa_plan_musisi_costs}):\n {best_greedy_musisi_plan}\n\n")
            #file.write(f"Bruteforce MuSi cost({best_bf_musi_plan_cost}):\n {best_bf_musi_plan}\n\n")
            #file.write(f"Bf Filtered cost({bf_filtered_pa_plan_musisi_costs}):\n {best_bf_filtered_musisi_plan}\n\n")
            file.write(f"Bruteforce cost({bruteforce_pa_plan_musisi_costs}):\n {best_bruteforce_musisi_plan}\n\n")
            file.write("Aggregate Placement: " + self.determine_aggregate_evaluation(best_bruteforce_musisi_plan))
            
    def filter_plans(self, query, plans, cut_event_type_composition):
        cet_sorted = sorted(cut_event_type_composition)
        filtered = []
        for plan in plans:
            cut_event_types = []
            for tup in plan:
                if len(tup) == 2:
                    cut_event_type = tup[0]  # for 2-tuple, first component is the cut event type
                elif len(tup) == 4:
                    cut_event_type = tup[2]  # for 4-tuple, third component is the cut event type
                else:
                    continue
                cut_event_types.append(cut_event_type)
                
            total_events = sorted("".join(cut_event_types))
            if total_events == cet_sorted:
                filtered.append(plan)
        
        return filtered
            
    def generate_plan2(self, cut_event_type_composition):
        '''print()
        print_network = True

        if print_network:
            print(self.event_sourced_network.eventtype_to_local_outputrate)
            print(self.event_sourced_network.eventtype_to_global_outputrate)
            print(self.event_sourced_network.nodes)
            print(self.event_sourced_network.eventtype_to_nodes)
            print()

        centralized_push_costs = self.determine_centralized_push_costs()

        # Greedy MuSi-Only
        greedy_pa_plan = self.generate_greedy_plan()
        greedy_pa_plan = self.merge_PPA_plan_steps(greedy_pa_plan)
        greedy_pa_plan_musi_only_costs = self.determine_PPA_plan_costs_esn_complete_topology(greedy_pa_plan)

        # Greedy MuSi-SiSi
        #greedy_pa_plan = [('B', 'A'), ('D', 'E'), ('AB', 'C', 'B', 'A'), ('DE', 'C', 'D', 'E')]
        formatted_greedy_pa_plan = self.prepend_steptype_for_steps_in_pa_plan(greedy_pa_plan)
        best_greedy_musisi_plan = formatted_greedy_pa_plan
        if len(greedy_pa_plan) == len(self.sequence_pattern):
            greedy_pa_plan_musisi_costs = centralized_push_costs
        else:
            all_possible_greedy_plans, etype_to_predecessorsteps = self.build_all_possible_plans_with_SiSi(formatted_greedy_pa_plan)
            greedy_pa_plan_musisi_costs = float('inf')
            for idx, plan in enumerate(all_possible_greedy_plans):
                plan_costs = self.determine_PA_Plan_wSiSi_costs_of_entire_esn(plan, etype_to_predecessorsteps)
                if plan_costs <= greedy_pa_plan_musisi_costs:
                    greedy_pa_plan_musisi_costs = plan_costs
                    best_greedy_musisi_plan = all_possible_greedy_plans[idx]
            print(), print_plan_with_dict_steps(best_greedy_musisi_plan, greedy_pa_plan_musisi_costs, delim='\n')

        # Filtered Bruteforce
        sp1, sp2, sp3 = self.determine_different_strong_placement_types(1)
        filtered_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern, sp3)
        best_bf_filtered_musisi_plan, bf_filtered_pa_plan_musisi_costs = self.determine_best_MuSiSi_plan(filtered_pa_plans)

        # Complete Bruteforce
        all_bruteforce_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern)
        best_bruteforce_musisi_plan, bruteforce_pa_plan_musisi_costs = self.determine_best_MuSiSi_plan(all_bruteforce_pa_plans)'''
		
        all_bruteforce_pa_plans = self.enumerate_all_PA_plans(self.sequence_pattern)
        cet_filtered_plans = self.filter_plans(self.sequence_pattern, all_bruteforce_pa_plans, cut_event_type_composition)
        best_cet_filtered_plan, cost = self.determine_best_MuSiSi_plan(cet_filtered_plans)
        #print("~~~~~~~~~~~~~~~~~~~~~~~")
        #print("These are all filtered plans:")
        #for p in cet_filtered_plans:
        #    print(p)
        #print("~~~~~~~~~~~~~~~~~~~~~~~")

        '''if print_network:
            print(f"Central          : {centralized_push_costs}")
            print(f"Greedy MuSi_only : {greedy_pa_plan_musi_only_costs}")
            print(f"Greedy MuSiSi    : {greedy_pa_plan_musisi_costs}")
            print(f"B-Filtered MuSiSi: {bf_filtered_pa_plan_musisi_costs}")
            print(f"Bruteforce MuSiSi: {bruteforce_pa_plan_musisi_costs}")
            print()
            print(f"Greedy MuSi {greedy_pa_plan_musi_only_costs}:"), [print(f" {idx}: {list(step)}") for idx, step in enumerate(greedy_pa_plan)]
            print("Greedy MuSiSi", end=''), print_plan_with_dict_steps(best_greedy_musisi_plan, index=greedy_pa_plan_musisi_costs, delim='\n') if isinstance(best_greedy_musisi_plan[0], Dotdict) else (print(f" {greedy_pa_plan_musisi_costs}:"), [print(f" {idx}: {list(step)}") for idx, step in enumerate(greedy_pa_plan)])
            print("Bf Filtered", end=''), print_plan_with_dict_steps(best_bf_filtered_musisi_plan, index=bf_filtered_pa_plan_musisi_costs, delim='\n')
            print("Bruteforce", end=''), print_plan_with_dict_steps(best_bruteforce_musisi_plan, index=bruteforce_pa_plan_musisi_costs, delim='\n')

        with open("generated_network.txt", mode='w') as file:
            file.write(f"query = \"{self.sequence_pattern}\"\n")
            file.write(f"local_rate = {self.event_sourced_network.eventtype_to_local_outputrate}\n")
            file.write(f"global_rate = {self.event_sourced_network.eventtype_to_global_outputrate}\n")
            file.write(f"network = {self.event_sourced_network.nodes}\n")
            file.write(f"eventtype_to_nodes = {self.event_sourced_network.eventtype_to_nodes}\n\n")

            file.write(f"Central cost({centralized_push_costs})\n\n")
            file.write(f"Greedy MuSi cost({greedy_pa_plan_musi_only_costs}):\n {greedy_pa_plan}\n\n")
            file.write(f"Greedy MuSiSi cost({greedy_pa_plan_musisi_costs}):\n {best_greedy_musisi_plan}\n\n")
            file.write(f"Bruteforce MuSi cost({best_bf_musi_plan_cost}):\n {best_bf_musi_plan}\n\n")
            file.write(f"Bf Filtered cost({bf_filtered_pa_plan_musisi_costs}):\n {best_bf_filtered_musisi_plan}\n\n")
            file.write(f"Bruteforce cost({bruteforce_pa_plan_musisi_costs}):\n {best_bruteforce_musisi_plan}\n\n")'''
            

    def query1_5(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 173, 'B': 6860, 'C': 22, 'D': 1778, 'E': 957, 'F': 98}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 346, 'B': 13720, 'C': 44, 'D': 3556, 'E': 1914, 'F': 196}
        self.event_sourced_network.nodes = [[173, 0, 0, 1778, 0, 98], [0, 6860, 22, 0, 0, 0], [0, 6860, 0, 0, 0, 0], [0, 0, 0, 0, 957, 0], [173, 0, 22, 1778, 957, 98]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 5], 'B': [2, 3], 'C': [2, 5], 'D': [1, 5], 'E': [4, 5], 'F': [1, 5]}

    def query1_10(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 70, 'B': 2744, 'C': 9, 'D': 712, 'E': 383, 'F': 40}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 350, 'B': 13720, 'C': 45, 'D': 3560, 'E': 1915, 'F': 200}
        self.event_sourced_network.nodes = [[70, 0, 9, 712, 0, 40], [70, 2744, 9, 0, 0, 0], [0, 2744, 0, 0, 0, 40], [0, 2744, 0, 712, 383, 40], [70, 2744, 0, 0, 383, 40], [0, 0, 0, 712, 0, 40], [70, 0, 9, 712, 383, 0], [0, 0, 9, 0, 383, 0], [0, 2744, 0, 712, 383, 0], [70, 0, 9, 0, 0, 0]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 2, 5, 7, 10], 'B': [2, 3, 4, 5, 9], 'C': [1, 2, 7, 8, 10], 'D': [1, 4, 6, 7, 9], 'E': [4, 5, 7, 8, 9], 'F': [1, 3, 4, 5, 6]}

    def query1_25(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 29, 'B': 1144, 'C': 4, 'D': 297, 'E': 160, 'F': 17}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 348, 'B': 13728, 'C': 48, 'D': 3564, 'E': 1920, 'F': 204}
        self.event_sourced_network.nodes = [[29, 1144, 4, 0, 0, 0], [0, 0, 4, 297, 160, 0], [29, 1144, 0, 0, 160, 0], [29, 0, 4, 297, 0, 17], [29, 0, 4, 0, 0, 17], [0, 0, 4, 0, 0, 17], [0, 1144, 4, 0, 160, 17], [29, 1144, 0, 0, 160, 17], [29, 0, 4, 297, 0, 17], [0, 1144, 0, 0, 160, 0], [0, 0, 4, 0, 0, 17], [0, 0, 0, 297, 160, 17], [0, 0, 0, 297, 0, 17], [29, 1144, 4, 0, 160, 0], [29, 0, 0, 297, 0, 17], [0, 1144, 0, 0, 0, 0], [0, 1144, 0, 0, 0, 0], [0, 1144, 4, 297, 160, 17], [0, 0, 0, 0, 160, 0], [0, 1144, 0, 297, 160, 17], [29, 0, 4, 297, 160, 0], [0, 0, 0, 297, 0, 0], [29, 1144, 0, 0, 0, 0], [29, 1144, 0, 297, 0, 0], [29, 0, 4, 297, 160, 0]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 3, 4, 5, 8, 9, 14, 15, 21, 23, 24, 25], 'B': [1, 3, 7, 8, 10, 14, 16, 17, 18, 20, 23, 24], 'C': [1, 2, 4, 5, 6, 7, 9, 11, 14, 18, 21, 25], 'D': [2, 4, 9, 12, 13, 15, 18, 20, 21, 22, 24, 25], 'E': [2, 3, 7, 8, 10, 12, 14, 18, 19, 20, 21, 25], 'F': [4, 5, 6, 7, 8, 9, 11, 12, 13, 15, 18, 20]}

    def query1_50(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 14, 'B': 549, 'C': 2, 'D': 143, 'E': 77, 'F': 8}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 350, 'B': 13725, 'C': 50, 'D': 3575, 'E': 1925, 'F': 200}
        self.event_sourced_network.nodes = [[0, 549, 0, 0, 0, 8], [14, 549, 0, 143, 0, 0], [14, 0, 2, 0, 77, 0], [0, 0, 0, 0, 0, 8], [0, 0, 2, 143, 77, 8], [14, 549, 2, 0, 0, 8], [14, 549, 2, 0, 0, 0], [14, 0, 2, 143, 77, 8], [14, 549, 2, 0, 77, 0], [0, 549, 0, 0, 77, 8], [0, 549, 0, 143, 0, 8], [0, 0, 2, 0, 77, 0], [0, 0, 2, 0, 0, 0], [14, 549, 0, 143, 77, 8], [14, 549, 2, 143, 0, 8], [14, 0, 0, 143, 77, 0], [0, 0, 0, 0, 77, 8], [14, 549, 0, 143, 77, 8], [0, 0, 2, 0, 0, 0], [0, 549, 0, 0, 0, 0], [14, 0, 2, 143, 77, 0], [0, 549, 0, 0, 0, 8], [14, 549, 0, 143, 0, 0], [0, 549, 2, 143, 0, 0], [0, 549, 2, 143, 0, 8], [0, 549, 0, 0, 77, 0], [0, 549, 2, 143, 0, 0], [14, 549, 0, 143, 77, 8], [14, 549, 0, 0, 0, 0], [0, 0, 0, 143, 77, 8], [14, 0, 0, 0, 0, 0], [0, 0, 0, 0, 77, 0], [14, 549, 0, 0, 0, 0], [0, 0, 2, 0, 77, 8], [14, 0, 2, 143, 77, 8], [0, 0, 2, 143, 0, 0], [14, 549, 2, 0, 77, 0], [14, 549, 0, 0, 0, 0], [0, 0, 0, 143, 0, 8], [0, 0, 2, 143, 77, 8], [14, 0, 2, 143, 0, 8], [0, 549, 0, 143, 77, 0], [14, 0, 0, 143, 0, 0], [0, 0, 2, 143, 0, 8], [14, 0, 2, 143, 77, 0], [0, 549, 0, 143, 77, 0], [14, 0, 2, 0, 77, 8], [14, 0, 2, 0, 0, 8], [0, 549, 0, 0, 77, 8], [14, 0, 2, 0, 77, 8]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [2, 3, 6, 7, 8, 9, 14, 15, 16, 18, 21, 23, 28, 29, 31, 33, 35, 37, 38, 41, 43, 45, 47, 48, 50], 'B': [1, 2, 6, 7, 9, 10, 11, 14, 15, 18, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 37, 38, 42, 46, 49], 'C': [3, 5, 6, 7, 8, 9, 12, 13, 15, 19, 21, 24, 25, 27, 34, 35, 36, 37, 40, 41, 44, 45, 47, 48, 50], 'D': [2, 5, 8, 11, 14, 15, 16, 18, 21, 23, 24, 25, 27, 28, 30, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46], 'E': [3, 5, 8, 9, 10, 12, 14, 16, 17, 18, 21, 26, 28, 30, 32, 34, 35, 37, 40, 42, 45, 46, 47, 49, 50], 'F': [1, 4, 5, 6, 8, 10, 11, 14, 15, 17, 18, 22, 25, 28, 30, 34, 35, 39, 40, 41, 44, 47, 48, 49, 50]}

    def query2_5(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 6860, 'B': 98, 'C': 3584, 'D': 98, 'E': 1121, 'F': 44, 'G': 1778, 'H': 51}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 13720, 'B': 196, 'C': 7168, 'D': 196, 'E': 2242, 'F': 88, 'G': 3556, 'H': 102}
        self.event_sourced_network.nodes = [[6860, 0, 0, 98, 0, 44, 1778, 51], [0, 98, 3584, 0, 0, 0, 1778, 51], [0, 98, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 1121, 0, 0, 0], [6860, 0, 3584, 98, 1121, 44, 0, 0]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 5], 'B': [2, 3], 'C': [2, 5], 'D': [1, 5], 'E': [4, 5], 'F': [1, 5], 'G': [1, 2], 'H': [1, 2]}

    def query2_10(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 2744, 'B': 40, 'C': 1434, 'D': 39, 'E': 449, 'F': 18, 'G': 712, 'H': 21}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 13720, 'B': 200, 'C': 7170, 'D': 195, 'E': 2245, 'F': 90, 'G': 3560, 'H': 105}
        self.event_sourced_network.nodes = [[2744, 0, 1434, 39, 0, 18, 0, 21], [2744, 40, 1434, 0, 0, 0, 712, 21], [0, 40, 0, 0, 0, 18, 712, 21], [0, 40, 0, 39, 449, 18, 0, 0], [2744, 40, 0, 0, 449, 18, 712, 0], [0, 0, 0, 39, 0, 18, 712, 0], [2744, 0, 1434, 39, 449, 0, 0, 21], [0, 0, 1434, 0, 449, 0, 712, 0], [0, 40, 0, 39, 449, 0, 0, 0], [2744, 0, 1434, 0, 0, 0, 0, 21]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 2, 5, 7, 10], 'B': [2, 3, 4, 5, 9], 'C': [1, 2, 7, 8, 10], 'D': [1, 4, 6, 7, 9], 'E': [4, 5, 7, 8, 9], 'F': [1, 3, 4, 5, 6], 'G': [2, 3, 5, 6, 8], 'H': [1, 2, 3, 7, 10]}

    def query2_25(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 1144, 'B': 17, 'C': 598, 'D': 17, 'E': 187, 'F': 8, 'G': 297, 'H': 9}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 13728, 'B': 204, 'C': 7176, 'D': 204, 'E': 2244, 'F': 96, 'G': 3564, 'H': 108}
        self.event_sourced_network.nodes = [[1144, 17, 598, 0, 0, 0, 0, 0], [0, 0, 598, 17, 187, 0, 0, 9], [1144, 17, 0, 0, 187, 0, 297, 0], [1144, 0, 598, 17, 0, 8, 0, 0], [1144, 0, 598, 0, 0, 8, 297, 9], [0, 0, 598, 0, 0, 8, 297, 0], [0, 17, 598, 0, 187, 8, 297, 0], [1144, 17, 0, 0, 187, 8, 297, 9], [1144, 0, 598, 17, 0, 8, 0, 0], [0, 17, 0, 0, 187, 0, 0, 0], [0, 0, 598, 0, 0, 8, 0, 9], [0, 0, 0, 17, 187, 8, 0, 0], [0, 0, 0, 17, 0, 8, 0, 9], [1144, 17, 598, 0, 187, 0, 0, 0], [1144, 0, 0, 17, 0, 8, 297, 0], [0, 17, 0, 0, 0, 0, 0, 0], [0, 17, 0, 0, 0, 0, 0, 0], [0, 17, 598, 17, 187, 8, 297, 9], [0, 0, 0, 0, 187, 0, 0, 0], [0, 17, 0, 17, 187, 8, 297, 9], [1144, 0, 598, 17, 187, 0, 297, 9], [0, 0, 0, 17, 0, 0, 297, 9], [1144, 17, 0, 0, 0, 0, 297, 9], [1144, 17, 0, 17, 0, 0, 0, 9], [1144, 0, 598, 17, 187, 0, 297, 9]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 3, 4, 5, 8, 9, 14, 15, 21, 23, 24, 25], 'B': [1, 3, 7, 8, 10, 14, 16, 17, 18, 20, 23, 24], 'C': [1, 2, 4, 5, 6, 7, 9, 11, 14, 18, 21, 25], 'D': [2, 4, 9, 12, 13, 15, 18, 20, 21, 22, 24, 25], 'E': [2, 3, 7, 8, 10, 12, 14, 18, 19, 20, 21, 25], 'F': [4, 5, 6, 7, 8, 9, 11, 12, 13, 15, 18, 20], 'G': [3, 5, 6, 7, 8, 15, 18, 20, 21, 22, 23, 25], 'H': [2, 5, 8, 11, 13, 18, 20, 21, 22, 23, 24, 25]}

    def query2_50(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 549, 'B': 8, 'C': 287, 'D': 8, 'E': 90, 'F': 4, 'G': 143, 'H': 5}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 13725, 'B': 200, 'C': 7175, 'D': 200, 'E': 2250, 'F': 100, 'G': 3575, 'H': 125}
        self.event_sourced_network.nodes = [[0, 8, 0, 0, 0, 4, 143, 0], [549, 8, 0, 8, 0, 0, 0, 0], [549, 0, 287, 0, 90, 0, 0, 0], [0, 0, 0, 0, 0, 4, 143, 5], [0, 0, 287, 8, 90, 4, 0, 5], [549, 8, 287, 0, 0, 4, 0, 5], [549, 8, 287, 0, 0, 0, 143, 5], [549, 0, 287, 8, 90, 4, 0, 0], [549, 8, 287, 0, 90, 0, 0, 5], [0, 8, 0, 0, 90, 4, 143, 0], [0, 8, 0, 8, 0, 4, 143, 5], [0, 0, 287, 0, 90, 0, 143, 0], [0, 0, 287, 0, 0, 0, 143, 0], [549, 8, 0, 8, 90, 4, 0, 5], [549, 8, 287, 8, 0, 4, 0, 0], [549, 0, 0, 8, 90, 0, 143, 5], [0, 0, 0, 0, 90, 4, 143, 5], [549, 8, 0, 8, 90, 4, 0, 5], [0, 0, 287, 0, 0, 0, 0, 0], [0, 8, 0, 0, 0, 0, 143, 5], [549, 0, 287, 8, 90, 0, 143, 0], [0, 8, 0, 0, 0, 4, 0, 0], [549, 8, 0, 8, 0, 0, 0, 5], [0, 8, 287, 8, 0, 0, 143, 5], [0, 8, 287, 8, 0, 4, 0, 5], [0, 8, 0, 0, 90, 0, 143, 0], [0, 8, 287, 8, 0, 0, 143, 0], [549, 8, 0, 8, 90, 4, 0, 5], [549, 8, 0, 0, 0, 0, 0, 0], [0, 0, 0, 8, 90, 4, 0, 5], [549, 0, 0, 0, 0, 0, 0, 5], [0, 0, 0, 0, 90, 0, 0, 5], [549, 8, 0, 0, 0, 0, 143, 0], [0, 0, 287, 0, 90, 4, 143, 5], [549, 0, 287, 8, 90, 4, 0, 5], [0, 0, 287, 8, 0, 0, 0, 5], [549, 8, 287, 0, 90, 0, 0, 5], [549, 8, 0, 0, 0, 0, 0, 0], [0, 0, 0, 8, 0, 4, 143, 0], [0, 0, 287, 8, 90, 4, 143, 5], [549, 0, 287, 8, 0, 4, 143, 0], [0, 8, 0, 8, 90, 0, 0, 0], [549, 0, 0, 8, 0, 0, 143, 0], [0, 0, 287, 8, 0, 4, 0, 0], [549, 0, 287, 8, 90, 0, 143, 5], [0, 8, 0, 8, 90, 0, 143, 0], [549, 0, 287, 0, 90, 4, 143, 0], [549, 0, 287, 0, 0, 4, 0, 0], [0, 8, 0, 0, 90, 4, 143, 5], [549, 0, 287, 0, 90, 4, 143, 0]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [2, 3, 6, 7, 8, 9, 14, 15, 16, 18, 21, 23, 28, 29, 31, 33, 35, 37, 38, 41, 43, 45, 47, 48, 50], 'B': [1, 2, 6, 7, 9, 10, 11, 14, 15, 18, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 37, 38, 42, 46, 49], 'C': [3, 5, 6, 7, 8, 9, 12, 13, 15, 19, 21, 24, 25, 27, 34, 35, 36, 37, 40, 41, 44, 45, 47, 48, 50], 'D': [2, 5, 8, 11, 14, 15, 16, 18, 21, 23, 24, 25, 27, 28, 30, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46], 'E': [3, 5, 8, 9, 10, 12, 14, 16, 17, 18, 21, 26, 28, 30, 32, 34, 35, 37, 40, 42, 45, 46, 47, 49, 50], 'F': [1, 4, 5, 6, 8, 10, 11, 14, 15, 17, 18, 22, 25, 28, 30, 34, 35, 39, 40, 41, 44, 47, 48, 49, 50], 'G': [1, 4, 7, 10, 11, 12, 13, 16, 17, 20, 21, 24, 26, 27, 33, 34, 39, 40, 41, 43, 45, 46, 47, 49, 50], 'H': [4, 5, 6, 7, 9, 11, 14, 16, 17, 18, 20, 23, 24, 25, 28, 30, 31, 32, 34, 35, 36, 37, 40, 45, 49]}

    def query3_5(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 40, 'B': 2722, 'C': 836, 'D': 3244, 'E': 957, 'F': 1184, 'G': 381, 'H': 1286, 'I': 271, 'J': 6860}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 80, 'B': 5444, 'C': 1672, 'D': 6488, 'E': 1914, 'F': 2368, 'G': 762, 'H': 2572, 'I': 542, 'J': 13720}
        self.event_sourced_network.nodes = [[40, 0, 0, 3244, 0, 1184, 381, 1286, 0, 0], [0, 2722, 836, 0, 0, 0, 381, 1286, 271, 0], [0, 2722, 0, 0, 0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 957, 0, 0, 0, 0, 6860], [40, 0, 836, 3244, 957, 1184, 0, 0, 271, 6860]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 5], 'B': [2, 3], 'C': [2, 5], 'D': [1, 5], 'E': [4, 5], 'F': [1, 5], 'G': [1, 2], 'H': [1, 2], 'I': [2, 5], 'J': [4, 5]}

    def query3_10(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 16, 'B': 1089, 'C': 335, 'D': 1298, 'E': 383, 'F': 474, 'G': 153, 'H': 515, 'I': 109, 'J': 2744}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 80, 'B': 5445, 'C': 1675, 'D': 6490, 'E': 1915, 'F': 2370, 'G': 765, 'H': 2575, 'I': 545, 'J': 13720}
        self.event_sourced_network.nodes = [[16, 0, 335, 1298, 0, 474, 0, 515, 109, 2744], [16, 1089, 335, 0, 0, 0, 153, 515, 0, 2744], [0, 1089, 0, 0, 0, 474, 153, 515, 0, 0], [0, 1089, 0, 1298, 383, 474, 0, 0, 109, 0], [16, 1089, 0, 0, 383, 474, 153, 0, 109, 2744], [0, 0, 0, 1298, 0, 474, 153, 0, 109, 0], [16, 0, 335, 1298, 383, 0, 0, 515, 0, 2744], [0, 0, 335, 0, 383, 0, 153, 0, 0, 0], [0, 1089, 0, 1298, 383, 0, 0, 0, 0, 2744], [16, 0, 335, 0, 0, 0, 0, 515, 109, 0]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 2, 5, 7, 10], 'B': [2, 3, 4, 5, 9], 'C': [1, 2, 7, 8, 10], 'D': [1, 4, 6, 7, 9], 'E': [4, 5, 7, 8, 9], 'F': [1, 3, 4, 5, 6], 'G': [2, 3, 5, 6, 8], 'H': [1, 2, 3, 7, 10], 'I': [1, 4, 5, 6, 10], 'J': [1, 2, 5, 7, 9]}

    def query3_25(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 7, 'B': 454, 'C': 140, 'D': 541, 'E': 160, 'F': 198, 'G': 64, 'H': 215, 'I': 46, 'J': 1144}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 84, 'B': 5448, 'C': 1680, 'D': 6492, 'E': 1920, 'F': 2376, 'G': 768, 'H': 2580, 'I': 552, 'J': 13728}
        self.event_sourced_network.nodes = [[7, 454, 140, 0, 0, 0, 0, 0, 0, 0], [0, 0, 140, 541, 160, 0, 0, 215, 0, 0], [7, 454, 0, 0, 160, 0, 64, 0, 46, 1144], [7, 0, 140, 541, 0, 198, 0, 0, 46, 0], [7, 0, 140, 0, 0, 198, 64, 215, 46, 0], [0, 0, 140, 0, 0, 198, 64, 0, 0, 0], [0, 454, 140, 0, 160, 198, 64, 0, 46, 0], [7, 454, 0, 0, 160, 198, 64, 215, 0, 1144], [7, 0, 140, 541, 0, 198, 0, 0, 46, 1144], [0, 454, 0, 0, 160, 0, 0, 0, 0, 0], [0, 0, 140, 0, 0, 198, 0, 215, 46, 0], [0, 0, 0, 541, 160, 198, 0, 0, 0, 1144], [0, 0, 0, 541, 0, 198, 0, 215, 46, 1144], [7, 454, 140, 0, 160, 0, 0, 0, 0, 1144], [7, 0, 0, 541, 0, 198, 64, 0, 46, 0], [0, 454, 0, 0, 0, 0, 0, 0, 46, 0], [0, 454, 0, 0, 0, 0, 0, 0, 0, 0], [0, 454, 140, 541, 160, 198, 64, 215, 0, 1144], [0, 0, 0, 0, 160, 0, 0, 0, 46, 1144], [0, 454, 0, 541, 160, 198, 64, 215, 0, 0], [7, 0, 140, 541, 160, 0, 64, 215, 0, 1144], [0, 0, 0, 541, 0, 0, 64, 215, 0, 1144], [7, 454, 0, 0, 0, 0, 64, 215, 46, 0], [7, 454, 0, 541, 0, 0, 0, 215, 0, 1144], [7, 0, 140, 541, 160, 0, 64, 215, 46, 1144]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 3, 4, 5, 8, 9, 14, 15, 21, 23, 24, 25], 'B': [1, 3, 7, 8, 10, 14, 16, 17, 18, 20, 23, 24], 'C': [1, 2, 4, 5, 6, 7, 9, 11, 14, 18, 21, 25], 'D': [2, 4, 9, 12, 13, 15, 18, 20, 21, 22, 24, 25], 'E': [2, 3, 7, 8, 10, 12, 14, 18, 19, 20, 21, 25], 'F': [4, 5, 6, 7, 8, 9, 11, 12, 13, 15, 18, 20], 'G': [3, 5, 6, 7, 8, 15, 18, 20, 21, 22, 23, 25], 'H': [2, 5, 8, 11, 13, 18, 20, 21, 22, 23, 24, 25], 'I': [3, 4, 5, 7, 9, 11, 13, 15, 16, 19, 23, 25], 'J': [3, 8, 9, 12, 13, 14, 18, 19, 21, 22, 24, 25]}

    def query3_50(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 4, 'B': 218, 'C': 67, 'D': 260, 'E': 77, 'F': 95, 'G': 31, 'H': 103, 'I': 22, 'J': 549}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 100, 'B': 5450, 'C': 1675, 'D': 6500, 'E': 1925, 'F': 2375, 'G': 775, 'H': 2575, 'I': 550, 'J': 13725}
        self.event_sourced_network.nodes = [[0, 218, 0, 0, 0, 95, 31, 0, 22, 0], [4, 218, 0, 260, 0, 0, 0, 0, 22, 0], [4, 0, 67, 0, 77, 0, 0, 0, 22, 0], [0, 0, 0, 0, 0, 95, 31, 103, 0, 0], [0, 0, 67, 260, 77, 95, 0, 103, 22, 549], [4, 218, 67, 0, 0, 95, 0, 103, 0, 0], [4, 218, 67, 0, 0, 0, 31, 103, 0, 549], [4, 0, 67, 260, 77, 95, 0, 0, 22, 0], [4, 218, 67, 0, 77, 0, 0, 103, 0, 549], [0, 218, 0, 0, 77, 95, 31, 0, 0, 0], [0, 218, 0, 260, 0, 95, 31, 103, 0, 0], [0, 0, 67, 0, 77, 0, 31, 0, 0, 549], [0, 0, 67, 0, 0, 0, 31, 0, 0, 549], [4, 218, 0, 260, 77, 95, 0, 103, 0, 549], [4, 218, 67, 260, 0, 95, 0, 0, 22, 549], [4, 0, 0, 260, 77, 0, 31, 103, 22, 549], [0, 0, 0, 0, 77, 95, 31, 103, 0, 0], [4, 218, 0, 260, 77, 95, 0, 103, 0, 549], [0, 0, 67, 0, 0, 0, 0, 0, 0, 0], [0, 218, 0, 0, 0, 0, 31, 103, 22, 0], [4, 0, 67, 260, 77, 0, 31, 0, 22, 0], [0, 218, 0, 0, 0, 95, 0, 0, 22, 549], [4, 218, 0, 260, 0, 0, 0, 103, 22, 0], [0, 218, 67, 260, 0, 0, 31, 103, 22, 0], [0, 218, 67, 260, 0, 95, 0, 103, 0, 0], [0, 218, 0, 0, 77, 0, 31, 0, 22, 0], [0, 218, 67, 260, 0, 0, 31, 0, 0, 549], [4, 218, 0, 260, 77, 95, 0, 103, 0, 549], [4, 218, 0, 0, 0, 0, 0, 0, 22, 0], [0, 0, 0, 260, 77, 95, 0, 103, 0, 549], [4, 0, 0, 0, 0, 0, 0, 103, 0, 549], [0, 0, 0, 0, 77, 0, 0, 103, 0, 549], [4, 218, 0, 0, 0, 0, 31, 0, 0, 549], [0, 0, 67, 0, 77, 95, 31, 103, 22, 0], [4, 0, 67, 260, 77, 95, 0, 103, 22, 549], [0, 0, 67, 260, 0, 0, 0, 103, 22, 0], [4, 218, 67, 0, 77, 0, 0, 103, 0, 549], [4, 218, 0, 0, 0, 0, 0, 0, 22, 549], [0, 0, 0, 260, 0, 95, 31, 0, 22, 0], [0, 0, 67, 260, 77, 95, 31, 103, 0, 549], [4, 0, 67, 260, 0, 95, 31, 0, 22, 0], [0, 218, 0, 260, 77, 0, 0, 0, 22, 0], [4, 0, 0, 260, 0, 0, 31, 0, 22, 549], [0, 0, 67, 260, 0, 95, 0, 0, 22, 0], [4, 0, 67, 260, 77, 0, 31, 103, 0, 549], [0, 218, 0, 260, 77, 0, 31, 0, 22, 0], [4, 0, 67, 0, 77, 95, 31, 0, 0, 549], [4, 0, 67, 0, 0, 95, 0, 0, 0, 549], [0, 218, 0, 0, 77, 95, 31, 103, 0, 0], [4, 0, 67, 0, 77, 95, 31, 0, 22, 549]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [2, 3, 6, 7, 8, 9, 14, 15, 16, 18, 21, 23, 28, 29, 31, 33, 35, 37, 38, 41, 43, 45, 47, 48, 50], 'B': [1, 2, 6, 7, 9, 10, 11, 14, 15, 18, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 37, 38, 42, 46, 49], 'C': [3, 5, 6, 7, 8, 9, 12, 13, 15, 19, 21, 24, 25, 27, 34, 35, 36, 37, 40, 41, 44, 45, 47, 48, 50], 'D': [2, 5, 8, 11, 14, 15, 16, 18, 21, 23, 24, 25, 27, 28, 30, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46], 'E': [3, 5, 8, 9, 10, 12, 14, 16, 17, 18, 21, 26, 28, 30, 32, 34, 35, 37, 40, 42, 45, 46, 47, 49, 50], 'F': [1, 4, 5, 6, 8, 10, 11, 14, 15, 17, 18, 22, 25, 28, 30, 34, 35, 39, 40, 41, 44, 47, 48, 49, 50], 'G': [1, 4, 7, 10, 11, 12, 13, 16, 17, 20, 21, 24, 26, 27, 33, 34, 39, 40, 41, 43, 45, 46, 47, 49, 50], 'H': [4, 5, 6, 7, 9, 11, 14, 16, 17, 18, 20, 23, 24, 25, 28, 30, 31, 32, 34, 35, 36, 37, 40, 45, 49], 'I': [1, 2, 3, 5, 8, 15, 16, 20, 21, 22, 23, 24, 26, 29, 34, 35, 36, 38, 39, 41, 42, 43, 44, 46, 50], 'J': [5, 7, 9, 12, 13, 14, 15, 16, 18, 22, 27, 28, 30, 31, 32, 33, 35, 37, 38, 40, 43, 45, 47, 48, 50]}

    # def example_query(self):
    #     self.event_sourced_network.eventtype_to_local_outputrate = {'A': 85000, 'B': 8270000, 'C': 342000, 'D': 1560000000, 'E': 10}
    #     self.event_sourced_network.eventtype_to_global_outputrate = {'A': 2550000, 'B': 215020000, 'C': 7182000, 'D': 35880000000, 'E': 250}
    #     self.event_sourced_network.nodes = [[0, 8270000, 0, 0, 10], [85000, 8270000, 0, 1560000000, 0], [85000, 8270000, 0, 0, 0], [85000, 8270000, 342000, 1560000000, 10], [0, 0, 342000, 0, 10], [85000, 8270000, 0, 0, 0], [85000, 8270000, 342000, 0, 10], [85000, 0, 342000, 1560000000, 0], [85000, 8270000, 0, 0, 0], [0, 8270000, 342000, 0, 0], [85000, 8270000, 0, 0, 0], [85000, 8270000, 0, 0, 10], [85000, 8270000, 0, 1560000000, 0], [85000, 0, 0, 0, 10], [85000, 0, 0, 1560000000, 10], [85000, 0, 342000, 1560000000, 0], [0, 0, 0, 0, 0], [85000, 0, 342000, 1560000000, 0], [85000, 8270000, 0, 0, 10], [0, 0, 342000, 1560000000, 0], [0, 0, 0, 1560000000, 10], [85000, 0, 342000, 0, 10], [85000, 8270000, 0, 0, 10], [0, 8270000, 0, 1560000000, 10], [0, 0, 0, 1560000000, 0], [85000, 0, 342000, 1560000000, 10], [0, 0, 0, 1560000000, 10], [85000, 8270000, 342000, 0, 10], [0, 8270000, 0, 1560000000, 10], [85000, 0, 0, 1560000000, 0], [0, 8270000, 342000, 0, 10], [85000, 0, 0, 1560000000, 10], [85000, 8270000, 342000, 1560000000, 0], [0, 8270000, 0, 0, 0], [85000, 0, 0, 1560000000, 0], [85000, 0, 342000, 1560000000, 10], [0, 8270000, 0, 1560000000, 0], [0, 0, 0, 0, 10], [0, 8270000, 342000, 1560000000, 10], [0, 0, 0, 1560000000, 10], [85000, 0, 0, 0, 10], [85000, 8270000, 342000, 0, 0], [0, 8270000, 342000, 0, 0], [0, 0, 342000, 0, 0], [0, 8270000, 342000, 1560000000, 0], [85000, 8270000, 0, 0, 0], [85000, 0, 342000, 0, 10], [85000, 8270000, 0, 0, 0], [0, 0, 342000, 0, 0], [85000, 0, 0, 0, 10]]
    #     self.event_sourced_network.eventtype_to_nodes = {'A': [2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 18, 19, 22, 23, 26, 28, 30, 32, 33, 35, 36, 41, 42, 46, 47, 48, 50], 'B': [1, 2, 3, 4, 6, 7, 9, 10, 11, 12, 13, 19, 23, 24, 28, 29, 31, 33, 34, 37, 39, 42, 43, 45, 46, 48], 'C': [4, 5, 7, 8, 10, 16, 18, 20, 22, 26, 28, 31, 33, 36, 39, 42, 43, 44, 45, 47, 49], 'D': [2, 4, 8, 13, 15, 16, 18, 20, 21, 24, 25, 26, 27, 29, 30, 32, 33, 35, 36, 37, 39, 40, 45], 'E': [1, 4, 5, 7, 12, 14, 15, 19, 21, 22, 23, 24, 26, 27, 28, 29, 31, 32, 36, 38, 39, 40, 41, 47, 50]}

    def example_query(self):
        self.event_sourced_network.eventtype_to_local_outputrate = {'A': 588000, 'B': 2, 'C': 390, 'D': 70, 'E': 60, 'F': 1400}
        self.event_sourced_network.eventtype_to_global_outputrate = {'A': 11760000, 'B': 48, 'C': 11310, 'D': 1610, 'E': 1380, 'F': 30800}
        self.event_sourced_network.nodes = [[588000, 2, 0, 0, 60, 0], [0, 0, 390, 0, 0, 1400], [0, 2, 0, 70, 0, 0], [588000, 2, 390, 0, 0, 0], [0, 0, 390, 70, 0, 1400], [0, 0, 390, 70, 0, 1400], [588000, 0, 0, 0, 60, 0], [0, 0, 0, 0, 60, 1400], [588000, 0, 0, 0, 60, 1400], [588000, 0, 0, 0, 60, 1400], [588000, 0, 390, 0, 0, 0], [0, 0, 390, 70, 0, 0], [588000, 0, 390, 0, 60, 0], [0, 0, 0, 0, 0, 1400], [0, 2, 390, 0, 60, 1400], [0, 2, 0, 0, 0, 0], [0, 0, 390, 70, 0, 0], [588000, 2, 390, 0, 60, 0], [0, 0, 390, 0, 60, 1400], [0, 2, 390, 0, 60, 0], [588000, 2, 390, 70, 60, 1400], [588000, 2, 390, 70, 0, 1400], [0, 2, 0, 0, 60, 0], [0, 0, 0, 70, 0, 0], [588000, 2, 390, 70, 60, 0], [588000, 2, 0, 0, 60, 1400], [0, 0, 390, 0, 0, 0], [0, 2, 0, 70, 60, 1400], [0, 2, 0, 70, 0, 1400], [0, 0, 390, 0, 0, 0], [588000, 0, 0, 0, 60, 0], [0, 2, 390, 0, 0, 0], [0, 2, 390, 0, 0, 1400], [0, 0, 390, 70, 0, 1400], [0, 0, 390, 0, 0, 1400], [0, 2, 0, 70, 0, 0], [0, 2, 0, 70, 60, 0], [588000, 0, 0, 70, 0, 0], [0, 0, 390, 70, 0, 0], [0, 2, 390, 70, 60, 0], [0, 2, 0, 70, 60, 0], [0, 0, 390, 0, 60, 1400], [0, 0, 0, 70, 0, 1400], [588000, 2, 0, 70, 0, 0], [588000, 2, 390, 0, 0, 1400], [588000, 2, 0, 70, 0, 1400], [588000, 0, 390, 70, 60, 1400], [0, 0, 390, 70, 0, 0], [588000, 0, 390, 0, 60, 0], [588000, 2, 390, 0, 60, 0]]
        self.event_sourced_network.eventtype_to_nodes = {'A': [1, 4, 7, 9, 10, 11, 13, 18, 21, 22, 25, 26, 31, 38, 44, 45, 46, 47, 49, 50], 'B': [1, 3, 4, 15, 16, 18, 20, 21, 22, 23, 25, 26, 28, 29, 32, 33, 36, 37, 40, 41, 44, 45, 46, 50], 'C': [2, 4, 5, 6, 11, 12, 13, 15, 17, 18, 19, 20, 21, 22, 25, 27, 30, 32, 33, 34, 35, 39, 40, 42, 45, 47, 48, 49, 50], 'D': [3, 5, 6, 12, 17, 21, 22, 24, 25, 28, 29, 34, 36, 37, 38, 39, 40, 41, 43, 44, 46, 47, 48], 'E': [1, 7, 8, 9, 10, 13, 15, 18, 19, 20, 21, 23, 25, 26, 28, 31, 37, 40, 41, 42, 47, 49, 50], 'F': [2, 5, 6, 8, 9, 10, 14, 15, 19, 21, 22, 26, 28, 29, 33, 34, 35, 42, 43, 45, 46, 47]}


########################################################################################################################
##################################################### Main  Method #####################################################
########################################################################################################################

def main():

    sequence = "ABCDEFG"

    number_of_nodes = 20
    # The next 2 parameters are just to pregenerate a network
    zipfian_parameter = 1.3
    event_node_ratio = 0.5
    stream_size = 250
    
    pa_plan_generator = PushPullAggregationPlanner(sequence, number_of_nodes, zipfian_parameter, event_node_ratio, stream_size)
    #pa_plan_generator.example_query()   # this is used to build a plan for a specific network
    cet_composition = pa_plan_generator.min_algorithm_cet_composition()
    #("The cut event type composition is:",cet_composition)
    pa_plan_generator.generate_plan()
    

if __name__ == "__main__":
    main()
