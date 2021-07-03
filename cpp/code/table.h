
#ifndef TABLE_H
#define TABLE_H

#include "sql.h"
#include "initial.h"
#include "standard.h"
#include "merger.h"
#include "aggregation.h"
#include "stacker.h"
#include "parser.h"
#include "options.h"

namespace SQL
{
    class Table
    {
        private:

            std::shared_ptr<Node> node;

            std::vector<std::string> errors;

        public:

            Table(const Table & other) : errors(other.errors)
            {
                node = other.node->copy();
            }

            template <typename... Args>
            Table(const std::string & table_name, Args && ... columns_names)
            {
                auto columns = Names(columns_names...);

                node = std::make_shared<Initial>(table_name, Columns(columns));
            }

            const Names & get_keys() const
            {
                return node->get_keys();
            }

            template <typename... Args>
            Table & set_keys(Args && ... args)
            {
                auto keys = Names(args...);

                check_that_columns_exist("SET KEYS", {node}, keys);

                auto inexistants = keys.substract(get_columns());

                if (inexistants.empty())
                {
                    node->set_keys(keys);
                }

                return *this;
            }

            Names get_columns() const
            {
                return node->get_columns().get_names();
            }
            
            bool has_column(const std::string & name)
            {
                return get_columns().contains(name);
            }

            Table copy() const
            {
                return Table(*this);
            }

            Table & rename(const std::string & old_name, const std::string & new_name)
            {
                check_that_name_is_not_empty("RENAME", {node}, new_name);
                check_that_columns_exist("RENAME", {node}, {old_name});
                check_that_columns_do_not_exist("RENAME", {node}, {new_name});

                add_standard_node_if_current_node_is_an_initial_node();

                node->rename(old_name, new_name);

                return *this;
            }

            template <typename... Args>
            Table & prefix(const std::string & prefix, Args... args)
            {
                auto columns = Names(args...);

                check_that_argument_is_not_empty("PREFIX", {node}, columns);
                check_that_columns_exist("PREFIX", {node}, columns);

                add_standard_node_if_current_node_is_an_initial_node();

                for (auto && column : columns)
                {
                    node->rename(column, prefix + column);
                }

                return *this;
            }

            template <typename... Args>
            Table & prefix_all_except(const std::string & prefix, Args... args)
            {
                auto exceptions = Names(args...);

                check_that_columns_exist("PREFIX ALL EXCEPT", {node}, exceptions);

                add_standard_node_if_current_node_is_an_initial_node();

                auto columns = get_columns().substract(exceptions);

                for (auto && column : columns)
                {
                    node->rename(column, prefix + column);
                }

                return *this;
            }

            template <typename... Args>
            Table & select(Args... args)
            {
                auto selection = Names(args...);

                check_that_argument_is_not_empty("SELECT", {node}, selection);
                check_that_columns_exist("SELECT", {node}, selection);

                return keep(selection);
            }

            template <typename... Args>
            Table & drop(Args... args)
            {
                auto to_be_dropped = Names(args...);

                check_that_argument_is_not_empty("DROP", {node}, to_be_dropped);
                check_that_columns_exist("DROP", {node}, to_be_dropped);

                auto selection = get_columns().substract(to_be_dropped);

                return keep(selection);
            }

            template <typename T>
            Table & drop_key(const std::string & name, T value)
            {
                auto keys = this->get_keys();

                check_that_columns_exist("DROP KEY", {node}, {name});
                check_that_column_is_a_key("DROP KEY", {node}, keys, name);

                keys = keys.substract(Names({name}));

                this->filter(name, " = ", Utils::single_quote(Utils::to_string(value)));
                this->drop(name);
                this->set_keys(keys);

                return *this;
            }

            template <typename... Args>
            Table & filter(Args... args)
            {
                check_that_argument_is_not_empty("FILTER", {node}, Names(args...));

                auto instructions = parse_flag_and_check("FILTER", {node}, args...);

                auto columns = node->get_columns();

                if (node->get_type() == Node::Type::AGGREGATION)
                {
                    std::dynamic_pointer_cast<Aggregation>(node)->add_filter(instructions);
                }
                else if (node->get_type() == Node::Type::STANDARD && !uses_computed_column(instructions, columns))
                {
                    replace_columns_names_by_their_names_before_they_were_renamed(instructions);

                    std::dynamic_pointer_cast<Standard>(node)->add_filter(instructions);
                }
                else
                {
                    node = Standard::make(node);

                    std::dynamic_pointer_cast<Standard>(node)->add_filter(instructions);
                }

                return *this;
            }

            template <typename... Args>
            Table & create_column(const std::string & name, Args... args)
            {
                check_that_name_is_not_empty("CREATE COLUMN", {node}, name);
                check_that_argument_is_not_empty("CREATE COLUMN", {node}, Names(args...));

                auto instructions = parse_flag_and_check("CREATE COLUMN", {node}, args...);

                auto columns = node->get_columns();

                if (node->get_type() != Node::Type::STANDARD || uses_computed_column(instructions, columns))
                {
                    node = Standard::make(node);
                }
                
                replace_columns_names_by_their_names_before_they_were_renamed(instructions);

                std::dynamic_pointer_cast<Standard>(node)->create_column(name, instructions);

                return *this;
            }
            
            Table & aggregate(const std::vector<Variable> & variables, const By & by)
            {
                check_that_group_by_is_not_empty("AGGREGATION", {node}, by.names);
                check_that_columns_exist("AGGREGATION", {node}, by.names);
                check_that_there_is_no_duplicate_in_aggregation("AGGREGATION", {node}, variables, by);

                auto aggregation = Aggregation::make(node);

                aggregation->set_group_by(by.names);

                for (auto && variable : variables)
                {
                    check_that_name_is_not_empty("AGGREGATION", {node}, variable.name);

                    auto instructions = parse_flag_and_check("AGGREGATION", {node}, variable.code);

                    aggregation->aggregate_column(variable.name, instructions);
                }
                
                aggregation->keep(aggregation->get_columns().get_names(), false);

                node = aggregation;

                return *this;
            }

            template <typename... Args>
            Table & inner_join(const Table & other, Args... args)
            {
                return join(other, "INNER JOIN", Names(args...));
            }

            template <typename... Args>
            Table & left_join(const Table & other, Args... args)
            {
                return join(other, "LEFT JOIN", Names(args...));
            }

            template <typename... Args>
            Table & right_join(const Table & other, Args... args)
            {
                return join(other, "RIGHT JOIN", Names(args...));
            }

            template <typename... Args>
            Table & full_join(const Table & other, Args... args)
            {
                return join(other, "FULL OUTER JOIN", Names(args...));
            }

            Table & unique()
            {
                if (node->get_type() != Node::Type::STANDARD)
                {
                    node = Standard::make(node);
                }

                std::dynamic_pointer_cast<Standard>(node)->set_distinct(true);

                node->set_keys(node->get_columns().get_names());

                node = Standard::make(node);

                return *this;
            }

            Table & stack(const Table & other)
            {
                if (node->get_type() != Node::Type::STACKER)
                {
                    node = Stacker::make(node, other.node->copy());
                }
                else
                {
                    std::dynamic_pointer_cast<Stacker>(node)->add(other.node->copy());
                }

                errors.insert(errors.end(), other.errors.begin(), other.errors.end());

                return *this;
            }

            std::string render() const
            {
                auto output = node->copy();

                auto selection = get_columns();
                
                bool successful = output->keep(selection, true);

                if (!successful)
                {
                    output = Standard::make(output);
                    output->keep(selection, true);
                }
                
                output = output->simplify(output);
                
                if (output->get_type() == Node::Type::INITIAL)
                {
                    return Standard::make(output)->render();
                }
                else
                {
                    return output->render();
                }
            }

            void print() const
            {
                node->print("");
            }
    
            bool has_errors() const
            {
                return errors.size() > 0;
            }

            void display_errors() const
            {
                for (auto && error : errors)
                {
                    std::cerr << error << std::endl << std::endl;
                }
            }

            Table & ignore_latest_error()
            {
                if (!errors.empty())
                {
                    errors.pop_back();
                }

                return *this;
            }

        private:

            Table() = default;

            void add_standard_node_if_current_node_is_an_initial_node()
            {
                if (node->get_type() == Node::Type::INITIAL)
                {
                    node = Standard::make(node);
                }
            }
    
            Table & keep(const Names & selection)
            {
                bool successful = node->keep(selection, false);

                if (!successful)
                {
                    node = Standard::make(node);
                    node->keep(selection, false);
                }

                return *this;
            }

            template <typename... Args>
            Instructions parse_flag_and_check(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, Args ... args)
            {
                std::string code = Utils::concatenate(args...);

                check_that_code_is_not_empty(operation, nodes, code);

                Parser parser;
                
                auto instructions = parser.parse_and_flag(code, get_columns());

                for (auto && error : parser.get_errors())
                {
                    log_error(operation, nodes, error);
                }

                check_that_all_tokens_are_recognized(operation, nodes, instructions);
                
                return instructions;
            }

            Table & join(const Table & other, const std::string & type, const Names & by)
            {
                auto lhs = this->node;
                auto rhs = other.node->copy();

                errors.insert(errors.end(), other.errors.begin(), other.errors.end());

                check_that_columns_are_consistent_in_join(type, lhs, rhs, by);

                auto result = std::make_shared<Merger>(lhs, rhs, type, by);

                update_keys_in_join(result, lhs, rhs, type, by);

                node = result;

                return *this;
            }

            void update_keys_in_join(std::shared_ptr<Merger> & result, const std::shared_ptr<Node> & lhs, const std::shared_ptr<Node> & rhs, const std::string & type, const Names & by)
            {
                if (lhs->has_keys() && rhs->has_keys())
                {
                    Names lhs_keys = lhs->get_keys();
                    Names rhs_keys = rhs->get_keys();

                    bool all_lhs_keys_are_used_in_by = lhs_keys.substract(by).size() == 0;
                    bool all_rhs_keys_are_used_in_by = rhs_keys.substract(by).size() == 0;

                    if ((type == "LEFT JOIN" || type == "INNER JOIN") && all_rhs_keys_are_used_in_by)
                    {
                        result->set_keys(lhs_keys);
                    }
                    else if ((type == "RIGHT JOIN" || type == "INNER JOIN") && all_lhs_keys_are_used_in_by)
                    {
                        result->set_keys(rhs_keys);
                    }
                    else
                    {
                        auto keys = lhs_keys;

                        keys.add(rhs_keys);

                        result->set_keys(keys);

                        if (!all_lhs_keys_are_used_in_by && !all_rhs_keys_are_used_in_by)
                        {
                            log_error(type, {lhs, rhs}, "please check that you did not forget some keys (", by.collapse(", "), ")");
                        }
                    }
                }
            }
            
            bool uses_computed_column(const Instructions & instructions, const Columns & columns) const
            {
                auto dependencies = instructions.get_values(Instruction::Type::COLUMN);

                for (auto && column : columns)
                {
                    if (column.get_type() == Column::Type::COMPUTED && dependencies.contains(column.get_name()))
                    {
                        return true;
                    }
                }

                return false;
            }

            void replace_columns_names_by_their_names_before_they_were_renamed(Instructions & instructions) const
            {
                Columns columns = node->get_columns();

                for (auto && instruction : instructions)
                {
                    if (instruction.get_type() == Instruction::Type::COLUMN)
                    {
                        auto name_before_rename = columns.get_name_before_rename(instruction.get_value());

                        instruction.set_value(name_before_rename);
                    }
                }
            }

            void check_that_columns_exist(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, const Names & names)
            {
                auto missing = names.substract(get_columns());

                if (missing.size() > 0)
                {
                    log_error(operation, nodes, "column(s) ", missing.collapse(", "), " do(es) not exist(s)");
                }
            }

            void check_that_column_is_a_key(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, const Names & keys, const std::string & name)
            {
                if (!keys.contains(name))
                {
                    log_error("DROP KEY", {node}, "column ", name, " is not a key");
                }
            }

            void check_that_there_is_no_duplicate_in_aggregation(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, const std::vector<Variable> & variables, const By & by)
            {
                Names names;

                std::vector<std::string> duplicates;

                for (auto && variable : variables)
                {
                    if (names.contains(variable.name))
                    {
                        duplicates.emplace_back(variable.name);
                    }

                    names.add(variable.name);
                }

                for (auto && name : by.names)
                {
                    if (names.contains(name))
                    {
                        duplicates.emplace_back(name);
                    }

                    names.add(name);
                }

                if (duplicates.size() > 0)
                {
                    log_error(operation, nodes, "column(s) ", Utils::collapse(duplicates, ", "), " are defined multiple times");
                }
            }

            void check_that_columns_do_not_exist(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, const Names & names)
            {
                auto existing = names.intersect(get_columns());

                if (existing.size() > 0)
                {
                    log_error(operation, nodes, "column(s) ", existing.collapse(", "), " already exist(s)");
                }
            }

            void check_that_code_is_not_empty(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, const std::string & code)
            {
                if (code.empty())
                {
                    log_error(operation, nodes, "code cannot be empty");
                }
            }

            void check_that_name_is_not_empty(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, const std::string & name)
            {
                if (name.empty())
                {
                    log_error(operation, nodes, "name cannot be empty");
                }
            }

            void check_that_all_tokens_are_recognized(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, const Instructions & instructions)
            {
                std::vector<std::string> tokens;

                for (auto && instruction : instructions)
                {
                    if (instruction.get_type() == Instruction::Type::UNDEFINED
                    || instruction.get_type() == Instruction::Type::ALPHA)
                    {
                        tokens.emplace_back(instruction.get_value());
                    }
                }

                if (tokens.size() > 0)
                {
                    log_error(operation, nodes, "tokens ", Utils::collapse(tokens, ", "), " could not be recognized");
                }
            }

            void check_that_argument_is_not_empty(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, const Names & names)
            {
                if (names.empty())
                {
                    log_error(operation, nodes, "argument must not be empty");
                }
            }

            void check_that_group_by_is_not_empty(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, const Names & group_by)
            {
                if (group_by.empty())
                {
                    log_error(operation, nodes, "group by must not be empty");
                }
            }

            void check_that_columns_are_consistent_in_join(const std::string & operation, const std::shared_ptr<Node> & lhs, const std::shared_ptr<Node> & rhs, const Names & by)
            {
                Names lhs_columns = lhs->get_columns().get_names();
                Names rhs_columns = rhs->get_columns().get_names();

                auto missing_columns_in_by_1 = by.substract(lhs_columns);
                auto missing_columns_in_by_2 = by.substract(rhs_columns);

                auto duplicate_columns = lhs_columns.intersect(rhs_columns).substract(by);

                if (missing_columns_in_by_1.size() > 0)
                {
                    log_error(operation, {lhs, rhs}, "column(s) ", missing_columns_in_by_1.collapse(", "), " used in by but missing in left-hand-side table");
                }

                if (missing_columns_in_by_2.size() > 0)
                {
                    log_error(operation, {lhs, rhs}, "column(s) ", missing_columns_in_by_2.collapse(", "), " used in by but missing in right-hand-side table");
                }

                if (duplicate_columns.size() > 0)
                {
                    log_error(operation, {lhs, rhs}, "column(s) ", duplicate_columns.collapse(", "), " exist(s) in both tables");
                }
            }

            template <typename... Args>
            void log_error(const std::string & operation, const std::vector< std::shared_ptr<Node> > & nodes, Args ... args)
            {
                std::string message;

                message += "ERROR";
                message += "\n";
                message += Utils::to_upper(operation);
                message += " : ";
                message += Names(args...).collapse("");
                message += " !";
                
                for (auto && node : nodes)
                {
                    message += "\n";
                    message += "CONTEXT : ";
                    message += node->get_sources().collapse(" ");
                }

                errors.emplace_back(message);
            }
    };
}

#endif // TABLE_H