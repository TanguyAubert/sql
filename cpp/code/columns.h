#ifndef COLUMNS_H
#define COLUMNS_H

#include <set>
#include "column.h"
#include "container.h"
#include "names.h"

namespace SQL
{
    class Columns : public Container<Column>
    {
        private:

            struct is_less
            {
                bool operator() (const Column & x, const Column & y) const
                {
                    return Utils::to_upper(x.get_name()) < Utils::to_upper(y.get_name());
                }
            };

            std::set<Column, is_less> uniques;

        public:

            Columns() = default;

            Columns(const Names & names)
            {
                for (auto && name : names)
                {
                    add(name);
                }
            }

            template <typename... Args>
            void add(Args && ... args)
            {
                Column column(args...);

                auto name = column.get_name();

                if (contains(name))
                {
                    replace(name, column);
                }
                else
                {
                    values.emplace_back(column);
                }
                
                uniques.insert(column);
            }

            std::string render(const std::string & alias = "") const
            {
                std::string output;

                std::size_t i = 0;

                for (auto && column : values)
                {
                    if (i > 0) output += ", ";

                    output += column.render(alias);

                    ++i;
                }

                return output;
            }

            Names get_names() const
            {
                Names output;

                for (auto && column : values)
                {
                    output.add(column.get_name());
                }

                return output;
            }
            
            void keep(const Names & names)
            {
                std::vector<Column> output;
                
                uniques.clear();

                for (auto && column : values)
                {
                    if (names.contains(column.get_name()))
                    {
                        output.emplace_back(column);

                        uniques.insert(column);
                    }
                }

                values = output;
            }
            
            Names get_dependencies() const
            {
                Names output;

                for (auto && column : values)
                {
                    output.add(column.get_dependencies());
                }

                return output;
            }

            void rename(const std::string & old_name, const std::string & new_name)
            {
                auto position = uniques.find(old_name);

                if (position != uniques.end())
                {
                    uniques.erase(position);

                    for (auto && column : values)
                    {
                        if (Utils::equals(column.get_name(), old_name))
                        {
                            column.set_name(new_name);

                            uniques.insert(column);

                            return;
                        }
                    }
                }
            }

            bool is_same_as(const Columns & other) const
            {
                return Utils::are_same(values, other.values);
            }
            
            std::string get_name_before_rename(const std::string & name) const
            {
                auto column = uniques.find(name);

                if (column != uniques.end())
                {
                    return column->get_name_before_rename();
                }

                return name;
            }

            bool contains(const std::string & name) const
            {
                return uniques.find(name) != uniques.end();
            }

        private:

            void replace(const std::string & name, const Column & column)
            {
                for (auto && value : values)
                {
                    if (Utils::equals(value.get_name(), name))
                    {
                        value = column;

                        return;
                    }
                }
            }
    };
}

#endif // COLUMNS_H