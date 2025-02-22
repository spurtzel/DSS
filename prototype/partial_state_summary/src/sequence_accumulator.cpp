#include "event.hpp"
#include "execution_state_counter.hpp"
#include "regex.hpp"

#include <boost/multiprecision/cpp_int.hpp>

#include <cxxopts.hpp>

#include <fmt/color.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <iostream>
#include <string>
#include <vector>

using counter_type = boost::multiprecision::uint256_t;
using timestamp_type = std::size_t;

template <> struct fmt::formatter<counter_type>: fmt::ostream_formatter {};

struct accumulated_event
{	
	suse::event<timestamp_type> event;
	counter_type count;
	std::string accumulation;

	friend std::istream& operator>>(std::istream& in, accumulated_event& e)
	{
		return in>>e.event>>e.count>>e.accumulation;
	}
};

int main(int argc, char* argv[]) try
{
	cxxopts::Options options("sequence_accumulator", "Computes accumulated matches of a sequence query");
	options.add_options()
		("regex,r","Regex to evaluate",cxxopts::value<std::string>())
		("type,t","The event type you are interested in", cxxopts::value<char>())
		("help,h","Display this help meassage");

	options.parse_positional("regex");
	options.positional_help("regex");

	const auto parsed_args = options.parse(argc,argv);

	if(parsed_args.count("help")>0 || argc<2)
	{
		fmt::print("{}",options.help());
		return 0;
	}

	for(auto required: {"regex","type"})
	{
		if(parsed_args.count(required)==0)
		{
			fmt::print(stderr,"{} is a required argument\n",required);
			return 1;
		}
	}

	const auto relevant_type = parsed_args["type"].template as<char>();
	const auto nfa = suse::parse_regex(parsed_args["regex"].template as<std::string>());
        const auto regex = parsed_args["regex"].template as<std::string>();

	const auto count_matches = [&](const suse::execution_state_counter<counter_type>& counters)
	{
		counter_type total = 0;

		for(std::size_t i=0; i<counters.size(); ++i)
		{
			if(nfa.states()[i].is_final)
				total+=counters[i];
		}

		return total;
	};

	const auto is_complete = [&](const suse::execution_state_counter<counter_type>& counters)
	{
		for(std::size_t i=0; i<counters.size(); ++i)
		{
			if(counters[i]>0 && !nfa.states()[i].transitions.empty())
				return false;
		}
		return true;
	};
	
	suse::execution_state_counter<counter_type> active_counters_per_state{nfa.number_of_states()};
	active_counters_per_state[nfa.initial_state_id()] = 1;

	struct counted_event_data
	{
		timestamp_type timestamp;
		suse::execution_state_counter<counter_type> counters_per_state;
	};
	std::vector<counted_event_data> per_event_counters;

	for(accumulated_event next_event; std::cin>>next_event;)
	{
		const auto counter_update_for = [&](auto counters)
		{
			for(auto single_event: next_event.accumulation)
				counters = advance(counters, nfa, single_event);

			return counters*next_event.count;
		};
		
		for(auto& e: per_event_counters)
			e.counters_per_state+=counter_update_for(e.counters_per_state);

		const auto active_update = counter_update_for(active_counters_per_state);
		active_counters_per_state+=active_update;

		if(next_event.event.type==relevant_type)
		{
			if(is_complete(active_update))
				fmt::print("{} {} {} {} ",relevant_type,next_event.event.timestamp,count_matches(active_update),regex);
			else
				per_event_counters.push_back({ .timestamp = next_event.event.timestamp, .counters_per_state = active_update});
		}
	}

	for(const auto& e: per_event_counters)
		fmt::print("{} {} {} {} ",relevant_type,e.timestamp,count_matches(e.counters_per_state),regex);

	return 0;
}
catch(const cxxopts::exceptions::exception& e)
{
	fmt::print(stderr,"Error parsing arguments: {}\n", e.what());
	return 1;
}
