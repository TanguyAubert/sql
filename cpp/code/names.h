#ifndef NAMES_H
#define NAMES_H

#include <set>
#include <string>
#include <algorithm>
#include "container.h"
#include "sql.h"

namespace SQL
{
    class Names : public Container<std::string>
    {
        private:

            struct is_less
            {
                bool operator() (const std::string & x, const std::string & y) const
                {
                    return Utils::to_upper(x) < Utils::to_upper(y);
                }
            };

            std::set<std::string, is_less> uniques;

        public:

            Names() = default;

            template <typename... Args>
            Names(Args ... args)
            {
                for (auto && value : Utils::flatten(args...))
                {
                    add(value);
                }
            }

            void add(const std::string & value)
            {
                values.emplace_back(value);
                uniques.insert(value);
            }

            void add(const Names & other)
            {
                for (auto && value : other)
                {
                    add(value);
                }
            }

            bool contains(const std::string & value) const
            {
                return uniques.find(value) != uniques.end();
            }

            Names substract(const Names & other) const
            {
                Names output;

                for (auto && name : values)
                {
                    if (!other.contains(name))
                    {
                        output.add(name);
                    }
                }

                return output;
            }

            Names intersect(const Names & other) const
            {
                Names output;

                for (auto && name : values)
                {
                    if (other.contains(name))
                    {
                        output.add(name);
                    }
                }

                return output;
            }

            std::string collapse(const std::string & separator) const
            {
                return Utils::collapse(values, separator);
            }

            void rename(const std::string & old_name, const std::string & new_name)
            {
                auto value = uniques.find(old_name);

                if (value != uniques.end())
                {
                    uniques.erase(value);

                    uniques.insert(new_name);
                }

                for (auto && name : values)
                {
                    if (Utils::equals(name, old_name))
                    {
                        name = new_name;
                    }
                }
            }

            void clear()
            {
                values.clear();
                uniques.clear();
            }

            bool empty() const
            {
                return values.empty();
            }
    };
}

#endif // NAMES_H