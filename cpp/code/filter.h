#ifndef FILTER_H
#define FILTER_H

#include "instructions.h"
#include "names.h"

namespace SQL
{
    class Filter
    {
        private:

            Instructions instructions;

        public:

            Filter(const Instructions & instructions) : instructions(instructions)
            {
                // NOTHING TO DO
            }

            std::string render() const
            {
                return instructions.render();
            }
            
            Names get_dependencies() const
            {
                return instructions.get_values(Instruction::Type::COLUMN);
            }
            
            bool is_same_as(const Filter & other) const
            {
                return instructions.is_same_as(other.instructions);
            }

            void rename_column(const std::string & old_name, const std::string & new_name)
            {
                for (auto && instruction : instructions)
                {
                    if (instruction.get_type() == Instruction::Type::COLUMN && Utils::equals(instruction.get_value(), old_name))
                    {
                        instruction.set_value(new_name);
                    }
                }
            }
    };
}

#endif // FILTER_H