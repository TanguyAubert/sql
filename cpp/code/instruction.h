
#ifndef INSTRUCTION_H
#define INSTRUCTION_H

#include <string>
#include "sql.h"

namespace SQL
{
    class Instruction
    {
        public:
            
            enum class Type {COLUMN, OPERATOR, FUNCTION, KEYWORD, NUMBER, ALPHA, TEXT, PUNCTUATION, UNDEFINED};

        private:

            Type type = Type::UNDEFINED;

            std::string value;

        public:

            Instruction(const Instruction & other) = default;

            Instruction(Type type, const std::string & value) : type(type), value(value)
            {

            }

            Type get_type() const
            {
                return type;
            }

            const std::string & get_value() const
            {
                return value;
            }

            void set_type(Type type)
            {
                this->type = type;
            }

            void set_value(const std::string value)
            {
                this->value = value;
            }

            std::string render(const std::string & alias) const
            {
                if (type == Type::COLUMN && !alias.empty())
                {
                    return alias + "." + value;
                }
                else if (type == Type::TEXT)
                {
                    return Utils::single_quote(value);
                }
                else
                {
                    return value;
                }
            }

            bool is_same_as(const Instruction & other) const
            {
                return type == other.type && Utils::equals(value, other.value);
            }
    };
}

#endif // INSTRUCTION_H