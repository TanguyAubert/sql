#ifndef AGGREGATION_H
#define AGGREGATION_H

#include "node.h"
#include "filters.h"
#include "names.h"

namespace SQL
{
    struct Variable
    {
        std::string name;

        std::string code;

        Variable() = delete;

        template <typename... Args>
        Variable(const std::string & name, Args... args) : name(name)
        {
            code = Utils::concatenate(args...);
        }
    };

    struct By
    {
        Names names;

        By() = delete;

        template <typename... Args>
        By(Args... args)
        {
            names = Names(args...);
        }
    };

    class Aggregation : public Node
    {
        private:

            Columns columns;

            Filters filters;

            Names group_by;
            
            std::shared_ptr<Node> predecessor;

        public:

            Aggregation() : Node(Type::AGGREGATION)
            {
                // NOTHING TO DO
            }
            
            Aggregation(const Aggregation & other) = default;

            virtual std::string render() const
            {
                std::string output;

                output += "SELECT ";
                output += columns.render();
                output += " FROM ";
                output += predecessor->render_with_alias(predecessor->get_alias());
                output += " GROUP BY ";
                output += group_by.collapse(", ");

                if (!filters.empty())
                {
                    output += " HAVING ";
                    output += filters.render();
                }

                return output;
            }
            
            virtual std::shared_ptr<Node> copy() const
            {
                auto output = std::make_shared<Aggregation>(*this);

                output->predecessor = predecessor->copy();
                
                return output;
            }
            
            static std::shared_ptr<Aggregation> make(const std::shared_ptr<Node> & node)
            {
                auto output = std::make_shared<Aggregation>();

                output->predecessor = node;

                return output;
            }
            
            virtual Columns get_columns() const
            {
                return columns;
            }
            
            virtual bool keep(const Names & selection, bool recursive)
            {
                bool successful = all_columns_used_in_a_filter_are_in_the_selection(selection);

                auto new_selection = add_columns_used_in_a_filter_to_the_selection(selection);

                columns.keep(new_selection);

                remove_all_keys_if_any_one_of_them_is_not_in_the_selection(new_selection);
                
                if (recursive)
                {
                    propagate_selection_to_the_predecessor(recursive);
                }

                return successful;
            }

            void add_filter(const Instructions & instructions)
            {
                filters.add(instructions);
            }

            void aggregate_column(const std::string & name, const Instructions & instructions)
            {
                columns.add(name, instructions);
            }

            virtual void rename(const std::string & old_name, const std::string & new_name)
            {
                columns.rename(old_name, new_name);

                filters.rename_column(old_name, new_name);

                rename_key(old_name, new_name);
            }

            virtual std::shared_ptr<Node> simplify(const std::shared_ptr<Node> & self)
            {
                predecessor = predecessor->simplify(predecessor);

                return self;
            }
            
            void set_group_by(const Names & group_by)
            {
                this->group_by = group_by;

                for (auto && name : group_by)
                {
                    columns.add(name);
                }

                keys = group_by;
            }
            
            virtual void print(const std::string & tabulation) const
            {
                std::cout << std::endl;
                std::cout << tabulation << "AGGREGATION" << std::endl;
                std::cout << tabulation << "- COLUMNS  : " << columns.render() << std::endl;
                std::cout << tabulation << "- FILTERS  : " << filters.render() << std::endl;
                std::cout << tabulation << "- GROUP BY : " << group_by.collapse(", ") << std::endl;

                predecessor->print(tabulation + "    ");
            }
            
            virtual Names get_sources() const
            {
                return predecessor->get_sources();
            }

        private:

            bool is_used_in_a_filter(const std::string & name) const
            {
                return filters.get_dependencies().contains(name);
            }

            bool is_used_in_group_by(const std::string & name) const
            {
                return group_by.contains(columns.get_name_before_rename(name));
            }

            bool all_columns_used_in_a_filter_are_in_the_selection(const Names & selection) const
            {
                for (auto && column : columns)
                {
                    auto name = column.get_name();

                    if (is_used_in_a_filter(name) && !selection.contains(name))
                    {
                        return false;
                    }
                }

                return true;
            }

            Names add_columns_used_in_a_filter_to_the_selection(const Names & selection) const
            {
                auto new_selection = selection;
                
                for (auto && column : columns)
                {
                    auto name = column.get_name();

                    if (is_used_in_a_filter(name))
                    {
                        new_selection.add(name);
                    }
                }

                return new_selection;
            }

            void propagate_selection_to_the_predecessor(bool recursive)
            {
                auto dependencies = columns.get_dependencies();

                dependencies.add(filters.get_dependencies());

                dependencies.add(group_by);

                predecessor->keep(dependencies, recursive);
            }
    };
}

#endif // AGGREGATION_H