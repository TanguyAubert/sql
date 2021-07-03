
#ifndef INSTRUCTIONS_H
#define INSTRUCTIONS_H

#include <string>
#include <vector>
#include "instruction.h"
#include "container.h"
#include "names.h"
#include "sql.h"

namespace SQL
{
    class Instructions : public Container<Instruction>
    {
        public:

            template <typename... Args>
            void add(Args && ... args)
            {
                values.emplace_back(args...);
            }

            std::string render(const std::string & alias = "") const
            {
                std::string output;

                for (std::size_t i = 0 ; i < values.size() ; ++i)
                {
                    if (i > 0
                    && is_not_parenthesis_after_function(i)
                    && is_not_a_comma(i)
                    && previous_is_not_an_opening_parenthesis(i)
                    && next_is_not_a_closing_parenthesis(i))
                    {
                        output += " ";
                    }
                    
                    output += values[i].render(alias);
                }

                return output;
            }

            bool is_not_parenthesis_after_function(std::size_t i) const
            {
                return (
                    (i <= 0)
                    || (values[i - 1].get_type() != Instruction::Type::FUNCTION)
                    || (values[i].get_value() != "(")
                );
            }
            
            bool previous_is_not_an_opening_parenthesis(std::size_t i) const
            {
                return i <= 0 || values[i - 1].get_value() != "(";
            }

            bool next_is_not_a_closing_parenthesis(std::size_t i) const
            {
                return i >= values.size() || values[i].get_value() != ")";
            }

            bool is_not_a_comma(std::size_t i) const
            {
                return values[i].get_value() != ",";
            }

            Names get_values(Instruction::Type type) const
            {
                Names output;

                for (auto && instruction : values)
                {
                    if (instruction.get_type() == type)
                    {
                        output.add(instruction.get_value());
                    }
                }

                return output;
            }
            
            bool is_same_as(const Instructions & other) const
            {
                return Utils::are_same(values, other.values);
            }
    };
}

#endif // INSTRUCTIONS_H