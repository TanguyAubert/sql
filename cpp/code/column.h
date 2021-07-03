
#ifndef COLUMN_H
#define COLUMN_H

#include "instructions.h"

namespace SQL
{
    class Column
    {
        public:
        
            enum class Type {SELECTED, RENAMED, COMPUTED};

        private:

            std::string name;

            Instructions instructions;

        public:

            Column() = default;

            Column(const std::string & name) : name(name)
            {
                instructions.add(Instruction::Type::COLUMN, name);
            }

            Column(const std::string & name, const Instructions & instructions) : name(name), instructions(instructions)
            {
                
            }

            std::string render(const std::string & alias = "") const
            {
                if (get_type() == Type::SELECTED && alias.empty())
                {
                    return name;
                }
                else
                {
                    return instructions.render(alias) + " AS " + name;
                }
            }

            Type get_type() const
            {
                if (instructions.size() == 1 && instructions[0].get_type() == Instruction::Type::COLUMN)
                {
                    return (instructions[0].get_value() == name ? Type::SELECTED : Type::RENAMED);
                }

                return Type::COMPUTED;
            }

            const std::string & get_name() const
            {
                return name;
            }

            void set_name(const std::string & name)
            {
                this->name = name;
            }

            void set_instructions(const Instructions & instructions)
            {
                this->instructions = instructions;
            }
            
            Names get_dependencies() const
            {
                return instructions.get_values(Instruction::Type::COLUMN);
            }

            std::string get_name_before_rename() const
            {
                if (get_type() == Type::RENAMED)
                {
                    return instructions[0].get_value();
                }
                else
                {
                    return name;
                }
            }
            
            bool is_same_as(const Column & other) const
            {
                return name == other.name && instructions.is_same_as(other.instructions);
            }
    };
}

#endif // COLUMN_H