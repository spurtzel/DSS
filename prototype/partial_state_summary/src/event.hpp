#ifndef SUSE_EVENT_HPP
#define SUSE_EVENT_HPP

#include <iostream>

#include <cstddef>

namespace suse
{
	template <typename timestamp_type>
	struct event
	{
		char type;
		timestamp_type timestamp;

		friend constexpr auto operator<=>(const event&, const event&) = default;
	};

	template <typename timestamp_type>
	std::istream& operator>>(std::istream& in, event<timestamp_type>& e)
	{
		return in>>e.type>>e.timestamp;
	}
}

#endif
