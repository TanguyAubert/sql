#ifndef FILTERS_H
#define FILTERS_H

#include "filter.h"
#include "container.h"
#include "sql.h"

namespace SQL
{
    class Filters : public Container<Filter>
    {
        public:

            template <typename... Args>
            void add(Args && ... args)
            {
                values.emplace_back(args...);
            }

            std::string render() const
            {
                std::string output;

                for (std::size_t i = 0 ; i < values.size() ; ++i)
                {
                    if (i > 0) output += " AND ";
                    
                    output += values[i].render();
                }

                return output;
            }

            bool empty() const
            {
                return values.empty();
            }

            Names get_dependencies() const
            {
                Names output;

                for (auto && filter : values)
                {
                    output.add(filter.get_dependencies());
                }

                return output;
            }

            bool is_same_as(const Filters & other) const
            {
                return Utils::are_same(values, other.values);
            }
            
            void rename_column(const std::string & old_name, const std::string & new_name)
            {
                for (auto && filter : values)
                {
                    filter.rename_column(old_name, new_name);
                }
            }
    };
}

#endif // FILTERS_H