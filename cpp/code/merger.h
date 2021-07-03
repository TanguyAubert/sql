#ifndef MERGER_H
#define MERGER_H

#include <iostream>
#include "node.h"
#include "names.h"
#include "sql.h"
#include "options.h"

namespace SQL
{
    class Standard;

    class Merger : public Node
    {
        private:

            std::shared_ptr<Node> lhs;

            std::shared_ptr<Node> rhs;

            std::string type;

            Names by;

        public:

            Merger() = delete;

            Merger(const Merger & other) = default;

            Merger(const std::shared_ptr<Node> & lhs, const std::shared_ptr<Node> & rhs, const std::string & type, const Names & by) : Node(Type::MERGER), type(type), by(by)
            {
                this->lhs = lhs;
                this->rhs = rhs;
            }

            virtual std::string render() const
            {
                std::string output;

                output += "SELECT ";
                output += render_columns();
                output += " FROM ";
                output += lhs->render_with_alias(lhs->get_alias());
                output += " ";
                output += type;
                output += " ";
                output += rhs->render_with_alias(rhs->get_alias());
                output += " ON (";
                output += render_by();
                output += ")";

                return output;
            }
            
            virtual std::shared_ptr<Node> copy() const
            {
                auto output = std::make_shared<Merger>(*this);
                
                output->lhs = lhs->copy();
                output->rhs = rhs->copy();

                return output;
            }

            virtual Columns get_columns() const
            {
                Columns columns;

                for (auto && column : lhs->get_columns()) columns.add(column.get_name());
                for (auto && column : rhs->get_columns()) columns.add(column.get_name());
                
                return columns;
            }
            
            virtual bool keep(const Names & selection, bool recursive)
            {
                remove_all_keys_if_any_one_of_them_is_not_in_the_selection(selection);

                bool successful = by.substract(selection).empty();

                auto new_selection = selection;

                new_selection.add(by);

                lhs->keep(new_selection, recursive);
                rhs->keep(new_selection, recursive);

                return successful;
            }
            
            virtual void rename(const std::string & old_name, const std::string & new_name)
            {
                lhs->rename(old_name, new_name);
                rhs->rename(old_name, new_name);

                by.rename(old_name, new_name);

                rename_key(old_name, new_name);
            }
            
            virtual std::shared_ptr<Node> simplify(const std::shared_ptr<Node> & self)
            {
                lhs = lhs->simplify(lhs);
                rhs = rhs->simplify(rhs);

                if (type == "LEFT JOIN"  && can_be_pruned(rhs)) return lhs;
                if (type == "RIGHT JOIN" && can_be_pruned(lhs)) return rhs;

                return self;
            }
            
            virtual void print(const std::string & tabulation) const
            {
                std::cout << std::endl;
                std::cout << tabulation << "MERGER" << std::endl;
                std::cout << tabulation << "- TYPE : " << type << std::endl;
                std::cout << tabulation << "- BY    : " << by.collapse(", ") << std::endl;

                lhs->print(tabulation + "    ");
                rhs->print(tabulation + "    ");
            }

            Names get_sources() const
            {
                Names sources;

                for (auto && predecessor : {lhs, rhs})
                {
                    sources.add(predecessor->get_sources());
                }

                return sources;
            }
        private:

            bool can_be_pruned(const std::shared_ptr<Node> & node) const
            {
                Names names = node->get_columns().get_names();

                return names.substract(by).empty() && node->has_keys();
            }
            
            std::string render_by() const
            {
                std::string output;

                std::size_t i = 0;

                for (auto && name : by)
                {
                    if (i > 0)
                    {
                        output += " AND ";
                    }

                    output += lhs->get_alias();
                    output += ".";
                    output += name;
                    output += " = ";
                    output += rhs->get_alias();
                    output += ".";
                    output += name;

                    ++i;
                }

                return output;
            }
            
            std::string render_columns() const
            {
                std::string output;

                auto lhs_names = lhs->get_columns().get_names().substract(by);
                auto rhs_names = rhs->get_columns().get_names().substract(by);

                if (type == "FULL OUTER JOIN")
                {
                    output += render_names_with_reconciliation(by, false);
                    output += render_names(lhs_names, lhs->get_alias(), true);
                    output += render_names(rhs_names, rhs->get_alias(), true);
                }
                else if (false)
                {
                    if (lhs->get_columns().size() >= rhs->get_columns().size())
                    {
                        output += lhs->get_alias();
                        output += ".*";
                        output += render_names(rhs_names, rhs->get_alias(), true);
                    }
                    else
                    {
                        output += rhs->get_alias();
                        output += ".*";
                        output += render_names(lhs_names, lhs->get_alias(), true);
                    }
                }
                else if (type == "RIGHT JOIN")
                {
                    output += render_names(by, rhs->get_alias(), false);
                    output += render_names(rhs_names, rhs->get_alias(), true);
                    output += render_names(lhs_names, lhs->get_alias(), true);
                }
                else
                {
                    output += render_names(by, lhs->get_alias(), false);
                    output += render_names(lhs_names, lhs->get_alias(), true);
                    output += render_names(rhs_names, rhs->get_alias(), true);
                }

                return output;
            }

            std::string render_names(const Names & names, const std::string & alias, bool start_with_comma) const
            {
                std::string output;

                std::size_t i = 0;

                for (auto && name : names)
                {
                    if (i > 0 || start_with_comma)
                    {
                        output += ", ";
                    }

                    output += alias;
                    output += ".";
                    output += name;
                    output += " AS ";
                    output += name;

                    ++i;
                }

                return output;
            }

            std::string render_names_with_reconciliation(const Names & names, bool start_with_comma) const
            {
                std::string output;

                std::size_t i = 0;

                for (auto && name : names)
                {
                    if (i > 0 || start_with_comma)
                    {
                        output += ", ";
                    }

                    output += "CASE WHEN ";
                    output += lhs->get_alias();
                    output += ".";
                    output += name;
                    output += " IS NULL THEN ";
                    output += rhs->get_alias();
                    output += ".";
                    output += name;
                    output += " ELSE ";
                    output += lhs->get_alias();
                    output += ".";
                    output += name;
                    output += " END AS ";
                    output += name;

                    ++i;
                }

                return output;
            }
    };
}

#endif // MERGER_H