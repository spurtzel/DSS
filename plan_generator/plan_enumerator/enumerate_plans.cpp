#include <algorithm>
#include <iostream>
#include <numeric>
#include <ranges>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

template <typename result_fun_t>
void enumerate_length_combinations(std::size_t remaining_length, std::vector<std::size_t>& current_result, result_fun_t result_fun)
{
	if(remaining_length==0)
	{
		result_fun(current_result);
		return;
	}
	
	for(std::size_t i=1; i<=remaining_length; ++i)
	{
		current_result.push_back(i);
			enumerate_length_combinations(remaining_length - i, current_result, result_fun);
		current_result.pop_back();
	}
}

template <typename result_fun_t>
void enumerate_length_combinations(std::size_t remaining_length, result_fun_t result_fun)
{
	std::vector<std::size_t> result;
	enumerate_length_combinations(remaining_length, result, result_fun);
}

template <typename result_fun_t>
void enumerate_combinations_with_placements(std::size_t length, const std::unordered_set<std::size_t>& allowed_placements, std::size_t offset, result_fun_t result_fun)
{
	for(std::size_t sink = offset; sink < offset+length; ++sink)
	{
		const auto relative_idx = sink - offset;
		
		enumerate_length_combinations(relative_idx,[&](const auto& left)
		{
			enumerate_length_combinations(length-relative_idx-1,[&](const auto& right)
			{
				std::size_t idx = offset;
				for(auto left_length: left)
				{
					if (left_length == 1 && allowed_placements.contains(idx))
						return;

					idx += left_length;
				}
				++idx;
				for(auto right_length: right)
				{
					if (right_length == 1 && allowed_placements.contains(idx))
						return;

					idx += right_length;
				}

				result_fun(left,right,relative_idx);
			});
		});
	}
}

struct plan_step
{
	std::size_t offset, sink;
	std::vector<std::size_t> left, right;
};

struct plan_step_application
{
	std::string_view sequence;
	const plan_step& step;
};

std::ostream& operator<<(std::ostream& out, const plan_step_application& app)
{
	for(std::size_t idx = app.step.offset; const auto& l: app.step.left)
	{
		out<<app.sequence.substr(idx,l)<<' ';
		idx+=l;
	}
	if(!app.step.left.empty()) out<<"=> ";
	out<<app.sequence[app.step.offset + app.step.sink];
	if(!app.step.right.empty()) out<<" <= ";
	for(std::size_t idx = app.step.offset + app.step.sink + 1; const auto& r: app.step.right)
	{
		out<<app.sequence.substr(idx,r)<<' ';
		idx+=r;
	}

	return out;
}

enum class forward_type
{
	nowhere, left, right
};

struct restriction_info
{
	std::unordered_set<std::size_t> allowed_placements, kleene_indices, negated_indices;
};

std::vector<std::vector<plan_step>> enumerate_plans_of_length(std::size_t offset, std::size_t length, const restriction_info& restrictions, forward_type forward_to)
{
	std::vector<std::vector<plan_step>> result;
	
	enumerate_combinations_with_placements(length, restrictions.allowed_placements, offset, [&](const auto& left, const auto& right, std::size_t sink)
	{
		const auto greater1 = [](auto length){ return length>1; };
		if(forward_to==forward_type::right && std::ranges::any_of(right,greater1))
			return;
		if(forward_to==forward_type::left && std::ranges::any_of(left,greater1))
			return;

		if(left.size()>1 && std::any_of(std::next(left.begin()),left.end(),greater1))
			return;

		if(right.size()>1 && std::any_of(right.begin(),std::prev(right.end()),greater1))
			return;

		if (forward_to == forward_type::right && right.empty() && restrictions.allowed_placements.contains(offset+sink)) 
			return;
		if (forward_to == forward_type::left && left.empty() && restrictions.allowed_placements.contains(offset+sink))
			return;

		const auto leftmost = offset;
		const auto rightmost = offset + sink + std::accumulate(right.begin(), right.end(), std::size_t{0});

		if(restrictions.negated_indices.contains(leftmost)) return;
		if(restrictions.negated_indices.contains(rightmost)) return;

		if(forward_to==forward_type::right && restrictions.kleene_indices.contains(rightmost)) return;
		if(forward_to==forward_type::left && restrictions.kleene_indices.contains(leftmost)) return;

		struct subplan_info
		{
			std::vector<std::vector<plan_step>> plans;
		};
		
		std::vector<subplan_info> required_subplans;
		
		for(std::size_t idx = offset; const auto& subsize: left)
		{
			if(subsize>1)
			{
				auto subplans = enumerate_plans_of_length(idx, subsize, restrictions, forward_type::right);
				required_subplans.emplace_back(std::move(subplans));
			}
			idx+=subsize;
		}
		
		for(std::size_t idx = offset + sink + 1; const auto& subsize: right)
		{
			if(subsize>1)
			{
				auto subplans = enumerate_plans_of_length(idx, subsize, restrictions, forward_type::left);
				required_subplans.emplace_back(std::move(subplans));
			}
			idx+=subsize;
		}

		std::vector<plan_step> current_plan;
		current_plan.push_back({offset, sink,left,right});
		
		const auto generate_plans = [&](std::size_t idx, std::size_t inner_idx, auto rec)
		{
			if(idx>=required_subplans.size())
			{
				result.push_back(current_plan);
				return;
			}
			
			if(inner_idx>=required_subplans[idx].plans.size())
				return;
				
			const auto old_size = current_plan.size();
			for(const auto& step: required_subplans[idx].plans[inner_idx])
				current_plan.push_back(step);

			rec(idx+1,0,rec);
			current_plan.resize(old_size);

			rec(idx, inner_idx+1, rec);
		};

		generate_plans(0,0,generate_plans);
	});

	return result;
}


void print_in_format(const std::vector<plan_step>& plan, std::string_view sequence)
{
	struct package
	{
		std::string_view sequence;
		char view;
	};
	
	std::unordered_map<char,std::vector<package>> send_to;
	std::unordered_map<std::string_view,char> created_on;
	for(const auto& step: plan)
	{
		const auto sink = sequence[step.offset + step.sink];
		
		std::size_t total_length = 0;
		for(std::size_t idx = step.offset; const auto& l: step.left)
		{
			const auto result = sequence.substr(idx,l);
			send_to[sink].emplace_back(result,result.back());
			idx+=l;
			total_length+=l;
		}
		for(std::size_t idx = step.offset + step.sink + 1; const auto& r: step.right)
		{
			const auto result = sequence.substr(idx,r);
			send_to[sink].emplace_back(result,result.front());
			idx+=r;
			total_length+=r;
		}

		++total_length;
		const auto sub_result = sequence.substr(step.offset, total_length);
		created_on[sub_result] = sink;
	}

	std::cout<<'{';
	for(bool first = true; const auto& [target,parts]: send_to)
	{
		std::string singles;
		
		for(const auto& part: parts)
		{
			if(part.sequence.size()==1)
				singles+=part.sequence;
			else
			{
				std::cout<<(first?"":", ")<<'('<<part.sequence<<", "<< target << ", " << part.view << ", "<<created_on[part.sequence]<<')';
				first = false;
			}
		}

		if(!singles.empty())
		{
			std::cout<<(first?"":", ")<<'('<<singles<<", "<<target<<')';
			first = false;
		}
	}
	std::cout<<"}\n";
}

int main(int argc, char* argv[])
{
	if(argc<5)
	{
		std::cerr<<"Usage:\n\t./plan_enumerator sequence placements kleene_types negated_types\n";
		return 1;
	}

	const auto sequence = std::string_view{argv[1]};
	const auto event_to_index = std::views::transform([&](char c)
	{
		return sequence.find(c);
	});
	
	const auto placement_indices = std::string_view{argv[2]} | event_to_index;
	const auto kleene_indices = std::string_view{argv[3]} | event_to_index;
	const auto negated_indices = std::string_view{argv[4]} | event_to_index;

	const restriction_info restrictions = 
	{
		.allowed_placements = {std::ranges::begin(placement_indices), std::ranges::end(placement_indices)},
		.kleene_indices = {std::ranges::begin(kleene_indices), std::ranges::end(kleene_indices)},
		.negated_indices = {std::ranges::begin(negated_indices), std::ranges::end(negated_indices)}
	};
	
	const auto possible_plans = enumerate_plans_of_length(0, sequence.size(), restrictions, forward_type::nowhere);

	for(const auto& plan: possible_plans)
	{
		print_in_format(plan, sequence);

	}
	
	std::cerr<<"There are "<<possible_plans.size()<<" different plans\n";
}
