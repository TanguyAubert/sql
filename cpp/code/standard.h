#ifndef STANDARD_H
#define STANDARD_H

#include "node.h"
#include "filters.h"
#include <iostream>
#include <iomanip>
#include "sql.h"

namespace SQL
{
    class Standard : public Node
    {
        private:

            Columns columns;

            Filters filters;
            
            std::shared_ptr<Node> predecessor;

            bool distinct = false;

        public:

            Standard(const Names & names) : Node(Type::STANDARD), columns(names)
            {
                // NOTHING TO DO
            }
            
            Standard(const Columns & columns) : Node(Type::STANDARD), columns(columns)
            {
                // NOTHING TO DO
            }
            
            Standard(const Standard & other) = default;

            virtual std::string render() const
            {
                std::string output;

                output += "SELECT ";

                if (distinct)
                {
                    output += "DISTINCT ";
                }

                output += render_columns();
                output += " FROM ";
                output += predecessor->render_with_alias(predecessor->get_alias());

                if (!filters.empty())
                {
                    output += " WHERE ";
                    output += filters.render();
                }

                return output;
            }

            virtual std::shared_ptr<Node> copy() const
            {
                auto output = std::make_shared<Standard>(*this);

                output->predecessor = predecessor->copy();
                
                return output;
            }
            
            static std::shared_ptr<Standard> make(const std::shared_ptr<Node> & node)
            {
                auto output = std::make_shared<Standard>(node->get_columns().get_names());

                output->predecessor = node;

                output->keys = node->get_keys();

                return output;
            }
            
            virtual Columns get_columns() const
            {
                return columns;
            }
            
            virtual bool keep(const Names & selection, bool recursive)
            {
                remove_all_keys_if_any_one_of_them_is_not_in_the_selection(selection);

                columns.keep(selection);

                if (recursive)
                {
                    propagate_selection_to_the_predecessor(recursive);
                }

                return true;
            }

            void add_filter(const Instructions & instructions)
            {
                filters.add(instructions);
            }

            void create_column(const std::string & name, const Instructions & instructions)
            {
                columns.add(name, instructions);
            }

            virtual void rename(const std::string & old_name, const std::string & new_name)
            {
                columns.rename(old_name, new_name);

                rename_key(old_name, new_name);
            }

            virtual std::shared_ptr<Node> simplify(const std::shared_ptr<Node> & self)
            {
                predecessor = predecessor->simplify(predecessor);

                if (this->is_same_as(predecessor))
                {
                    return predecessor;
                }

                return self;
            }
            
            std::size_t has_no_filters() const
            {
                return filters.empty();
            }
            
            virtual void print(const std::string & tabulation) const
            {
                std::cout << std::endl;
                std::cout << std::boolalpha;
                std::cout << tabulation << "STANDARD" << std::endl;
                std::cout << tabulation << "- DISTINCT : " << distinct << std::endl;
                std::cout << tabulation << "- COLUMNS  : " << columns.render() << std::endl;
                std::cout << tabulation << "- FILTERS  : " << filters.render() << std::endl;

                predecessor->print(tabulation + "    ");
            }

            bool is_same_as(const std::shared_ptr<Node> & other) const
            {
                if (other->get_type() != Node::Type::STANDARD) return false;

                auto standard = std::dynamic_pointer_cast<Standard>(other);

                return columns.is_same_as(standard->columns)
                    && filters.is_same_as(standard->filters)
                    && distinct == standard->distinct;
            }

            void set_distinct(bool distinct)
            {
                this->distinct = distinct;
            }

            virtual Names get_sources() const
            {
                return predecessor->get_sources();
            }
            
        private:
            
            std::string render_columns() const
            {
                std::string output;

                Columns partition_not_to_render;
                Columns partition_to_render;

                bool render_all_columns = partition_columns(partition_not_to_render, partition_to_render);

                if (true)
                {
                    output += columns.render();
                }
                else if (render_all_columns)
                {
                    output += columns.render();
                }
                else
                {
                    output += predecessor->get_alias();
                    output += ".*";

                    if (partition_to_render.size() > 0)
                    {
                        output += ", ";
                        output += partition_to_render.render();
                    }
                }

               return output;
            }
            
            bool partition_columns(Columns & partition_not_to_render, Columns & partition_to_render) const
            {
                if (predecessor->get_type() == Node::Type::INITIAL) return true;

                Names names = predecessor->get_columns().get_names();

                for (auto && column : columns)
                {
                    if (names.contains(column.get_name_before_rename()) && column.get_type() == Column::Type::SELECTED)
                    {
                        partition_not_to_render.add(column);
                    }
                    else
                    {
                        partition_to_render.add(column);
                    }
                }
                
                if (names.size() != partition_not_to_render.size()) return true;

                return false;
            }

            void propagate_selection_to_the_predecessor(bool recursive)
            {
                auto dependencies = columns.get_dependencies();

                dependencies.add(filters.get_dependencies());

                predecessor->keep(dependencies, recursive);
            }
    };
}

#endif // STANDARD_H