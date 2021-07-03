#ifndef PARSER_H
#define PARSER_H

#include "instructions.h"
#include "sql.h"
#include "names.h"
#include "keywords.h"
#include "functions.h"
#include <unordered_set>

namespace SQL
{
    class Parser
    {
        private:

            std::vector<std::string> errors;

        public:

            Instructions parse_and_flag(const std::string & input, const Names & column_names)
            {
                errors.clear();

                Instructions instructions = parse(input);

                instructions = flag_keywords_functions_and_punctuations(instructions);

                flag_columns(instructions, column_names);

                return instructions;
            }

            std::vector<std::string> get_errors() const
            {
                return errors;
            }
            
        private:

            Instructions parse(const std::string & input)
            {
                Instructions instructions;

                std::size_t size = input.size();

                for (std::size_t i = 0 ; i < size ; ++i)
                {
                    auto x = input[i];
                    
                    if(x != ' ' && x != '\n' && x != '\t')
                    {
                        if (is_alpha(x))
                        {
                            collect(instructions, Instruction::Type::ALPHA, input, i, is_alpha_or_numeric);
                        }
                        else if (is_numeric(x))
                        {
                            collect(instructions, Instruction::Type::NUMBER, input, i, is_numeric_or_dot);
                        }
                        else if (is_operator(x))
                        {
                            collect(instructions, Instruction::Type::OPERATOR, input, i, is_operator);
                        }
                        else if (x == '\'' || x == '"')
                        {
                            collect_text(instructions, input, i, x);
                        }
                        else
                        {
                            instructions.add(Instruction::Type::UNDEFINED, Utils::to_string(x));
                        }
                    }
                }

                return instructions;
            }

            void collect(Instructions & instructions, Instruction::Type type, const std::string & input, std::size_t & i, bool (*predicate) (char))
            {
                std::string token;
                
                token += input[i];

                while (i + 1 < input.size() && predicate(input[i + 1]))
                {
                    token += input[i + 1];
                    ++i;
                }

                instructions.add(type, token);
            }

            void collect_text(Instructions & instructions, const std::string & input, std::size_t & i, char x)
            {
                std::string token;

                std::size_t size = input.size();

                while (true)
                {
                    if (i + 1 >= size)
                    {
                        log_error("quote mismatch in ", input);

                        break;
                    }
                    else if (input[i + 1] == x)
                    {
                        ++i;
                        break;
                    }
                    else
                    {
                        // Skip escape character
                        if (i + 2 < size && input[i + 1] == '\\' && input[i + 2] == x)
                        {
                            ++i;
                        }

                        token += input[i + 1];
                        
                        ++i;
                    }
                }

                instructions.add(Instruction::Type::TEXT, token);
            }

            Instructions flag_keywords_functions_and_punctuations(const Instructions & instructions)
            {
                Instructions output;

                int size = static_cast<int>(instructions.size());

                Names punctuations = {"(", ")", ",", ";", "."};

                for (int i = 0 ; i < size ; ++i)
                {
                    if (instructions[i].get_type() == Instruction::Type::ALPHA)
                    {
                        if (contains_keyword(instructions, i, output))
                        {
                            // NOTHING
                        }
                        else if (is_a_function(instructions[i].get_value()))
                        {
                            output.add(Instruction::Type::FUNCTION, instructions[i].get_value());

                            if (i >= size || instructions[i + 1].get_value() != "(")
                            {
                                log_error("missing parenthesis after '", instructions[i].get_value(), "'");
                            }
                        }
                        else if (i + 1 < size && instructions[i + 1].get_value() == "(")
                        {
                            output.add(Instruction::Type::FUNCTION, instructions[i].get_value());
                            
                            log_error("unkwown function '", instructions[i].get_value(), "'");
                        }
                        else
                        {
                            output.add(instructions[i]);
                        }
                    }
                    else if (instructions[i].get_type() == Instruction::Type::UNDEFINED)
                    {
                        if (punctuations.contains(instructions[i].get_value()))
                        {
                            output.add(Instruction::Type::PUNCTUATION, instructions[i].get_value());
                        }
                        else
                        {
                            output.add(instructions[i]);
                        }
                    }
                    else if (i + 2 < size && is_scientific_number_1(instructions, i))
                    {
                        auto value = instructions[i].get_value()
                        + instructions[i + 1].get_value()
                        + instructions[i + 2].get_value();

                        output.add(Instruction::Type::NUMBER, value);

                        i += 2;
                    }
                    else if (i + 3 < size && is_scientific_number_2(instructions, i))
                    {
                        auto value = instructions[i].get_value()
                        + instructions[i + 1].get_value()
                        + instructions[i + 2].get_value()
                        + instructions[i + 3].get_value();

                        output.add(Instruction::Type::NUMBER, value);

                        i += 3;
                    }
                    else if (instructions[i].get_value() == "==")
                    {
                        output.add(instructions[i].get_type(), "=");
                    }
                    else
                    {
                        output.add(instructions[i]);
                    }
                }

                return output;
            }

            bool contains_keyword(const Instructions & instructions, int & i, Instructions & output)
            {
                int size = static_cast<int>(instructions.size());

                int j = i;

                while (j + 1 < size && instructions[j + 1].get_type() == Instruction::Type::ALPHA)
                {
                    ++j;
                }

                while (j >= i)
                {
                    std::string value;

                    for (int k = i ; k <= j ; ++k)
                    {
                        if (k > i)
                        {
                            value += " ";
                        }

                        value += instructions[k].get_value();
                    }

                    if (is_a_keyword(value))
                    {
                        output.add(Instruction::Type::KEYWORD, value);

                        i = j;

                        return true;
                    }

                    --j;
                }

                return false;
            }

            void flag_columns(Instructions & instructions, const Names & names)
            {
                for (std::size_t i = 0 ; i < instructions.size() ; ++i)
                {
                    if (instructions[i].get_type() == Instruction::Type::ALPHA)
                    {
                        if (names.contains(instructions[i].get_value()))
                        {
                            instructions[i].set_type(Instruction::Type::COLUMN);
                        }
                    }
                }
            }

            static bool is_alpha(char x)
            {
                return (65 <= x && x <= 90) || (97 <= x && x <= 122) || x == '_';
            }

            static bool is_numeric(char x)
            {
                return (48 <= x && x <= 57);
            }

            static bool is_numeric_or_dot(char x)
            {
                return is_numeric(x) || x == '.';
            }

            static bool is_alpha_or_numeric(char x)
            {
                return is_alpha(x) || is_numeric(x);
            }

            static bool is_operator(char x)
            {
                // https://www.w3schools.com/sql/sql_operators.asp

                return (x == '+' || x == '-' || x == '*' || x == '/' || x == '%'
                || x == '&' || x == '|' || x == '^'
                || x == '=' || x == '<' || x == '>'
                || x == '!');
            }

            static bool is_not_single_quote(char x)
            {
                return x != '\'';
            }

            static bool is_not_double_quote(char x)
            {
                return x != '"';
            }

            bool is_scientific_number_1(const Instructions & instructions, std::size_t i) const
            {
                return instructions[i].get_type() == Instruction::Type::NUMBER
                && (instructions[i + 1].get_value() == "e" || instructions[i + 1].get_value() == "E")
                && instructions[i + 2].get_type() == Instruction::Type::NUMBER
                ;
            }
            
            bool is_scientific_number_2(const Instructions & instructions, std::size_t i) const
            {
                return instructions[i].get_type() == Instruction::Type::NUMBER
                && (instructions[i + 1].get_value() == "e" || instructions[i + 1].get_value() == "E")
                && (instructions[i + 2].get_value() == "-" || instructions[i + 2].get_value() == "+")
                && instructions[i + 3].get_type() == Instruction::Type::NUMBER
                ;
            }
            
            template <typename... Args>
            void log_error(Args ... args)
            {
                auto tokens = Utils::flatten(args...);

                auto message = Utils::collapse(tokens, "");

                errors.emplace_back(message);
            }
    };
}

#endif // PARSER_H