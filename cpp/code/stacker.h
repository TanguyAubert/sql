#ifndef STACKER_H
#define STACKER_H

#include "node.h"

namespace SQL
{
    class Stacker : public Node
    {
        private:

            std::vector< std::shared_ptr<Node> > predecessors;

        public:

            Stacker() : Node(Type::STACKER)
            {
                // NOTHING TO DO
            }
            
            Stacker(const Stacker & other) = default;

            void add(const std::shared_ptr<Node> & node)
            {
                predecessors.emplace_back(node);
                
                update_keys_if_a_node_is_added(node);
            }

            static std::shared_ptr<Stacker> make(const std::shared_ptr<Node> & lhs, const std::shared_ptr<Node> & rhs)
            {
                auto output = std::make_shared<Stacker>();

                output->add(lhs);
                output->add(rhs);

                return output;
            }

            virtual std::string render() const
            {
                std::string output;
                
                auto target_columns = get_columns();

                std::size_t i = 0;

                for (auto && predecessor : predecessors)
                {
                    if (i > 0)
                    {
                        output += " UNION ALL ";
                    }

                    output += render_a_predecessor(predecessor, target_columns);

                    ++i;
                }

                return output;
            }
            
            virtual std::shared_ptr<Node> copy() const
            {
                auto output = std::make_shared<Stacker>(*this);

                output->predecessors.clear();

                for (auto && predecessor : predecessors)
                {
                    output->add(predecessor->copy());
                }
                
                return output;
            }

            virtual Columns get_columns() const
            {
                Columns columns;

                for (auto && predecessor : predecessors)
                {
                    for (auto && column : predecessor->get_columns())
                    {
                        columns.add(column.get_name());
                    }
                }
                
                return columns;
            }
            
            virtual bool keep(const Names & selection, bool recursive)
            {
                remove_all_keys_if_any_one_of_them_is_not_in_the_selection(selection);

                for (auto && predecessor : predecessors)
                {
                    predecessor->keep(selection, recursive);
                }

                return true;
            }
            
            virtual void rename(const std::string & old_name, const std::string & new_name)
            {
                for (auto && predecessor : predecessors)
                {
                    predecessor->rename(old_name, new_name);
                }

                rename_key(old_name, new_name);
            }
            
            virtual void print(const std::string & tabulation) const
            {
                std::cout << std::endl;
                std::cout << tabulation << "STACKER" << std::endl;

                for (auto && predecessor : predecessors)
                {
                    predecessor->print(tabulation + "    ");
                }
            }
            
            virtual Names get_sources() const
            {
                Names sources;

                for (auto && predecessor : predecessors)
                {
                    sources.add(predecessor->get_sources());
                }

                return sources;
            }

        private:

            void update_keys_if_a_node_is_added(const std::shared_ptr<Node> & node)
            {
                if (predecessors.empty())
                {
                    keys = node->get_keys();
                }
                else if (this->has_keys() && node->has_keys())
                {
                    keys.add(node->get_keys());
                }
                else
                {
                    keys.clear();
                }
            }

            std::string render_a_predecessor(const std::shared_ptr<Node> & predecessor, const Columns & target_columns) const
            {
                std::string output;
                
                auto actual_columns = predecessor->get_columns();

                output += "SELECT ";
                output += render_columns(actual_columns, target_columns);
                output += " FROM ";
                output += predecessor->render_with_alias(predecessor->get_alias());

                return output;
            }

            std::string render_columns(const Columns & actual_columns, const Columns & target_columns) const
            {
                std::string output;
                
                std::size_t i = 0;

                for (auto && column : target_columns)
                {
                    auto name = column.get_name();

                    if (i > 0)
                    {
                        output += ", ";
                    }

                    if (!actual_columns.contains(name))
                    {
                        output += "NULL AS ";
                    }

                    output += name;

                    ++i;
                }
                
                return output;
            }
    };
}

#endif // STACKER_H