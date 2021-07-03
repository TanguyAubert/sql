#ifndef INITIAL_H
#define INITIAL_H

#include "node.h"
#include "sql.h"
#include "options.h"

namespace SQL
{
    class Initial : public Node
    {
        private:
            
            std::string name;

            Columns columns;

        public:

            Initial(const Initial & other) = default;

            Initial(const std::string & name, const Columns & columns) : Node(Type::INITIAL), name(name), columns(columns)
            {
                
            }

            virtual std::string render() const
            {
                return name;
            }
            
            virtual std::string render_with_alias(const std::string & alias) const
            {
                if (Options::get_sgbd() == Options::SGBD::ORACLE)
                {
                    return name;
                }
                else
                {
                    return name + " AS " + alias;
                }
            }

            virtual std::shared_ptr<Node> copy() const
            {
                return std::make_shared<Initial>(*this);
            }

            virtual bool keep(const Names & selection, bool recursive)
            {
                if (!keys.substract(selection).empty())
                {
                    keys.clear();
                }

                columns.keep(selection);

                return true;
            }
            
            virtual Columns get_columns() const
            {
                return columns;
            }
            
            virtual void rename(const std::string & old_name, const std::string & new_name)
            {
                columns.rename(old_name, new_name);

                rename_key(old_name, new_name);
            }
            
            virtual void print(const std::string & tabulation) const
            {
                std::cout << std::endl;
                std::cout << tabulation << "INITIAL" << std::endl;
                std::cout << tabulation << "- NAME    : " << name << std::endl;
                std::cout << tabulation << "- COLUMNS : " << columns.render() << std::endl;
            }
            
            virtual Names get_sources() const
            {
                return {name};
            }
    };
}

#endif // INITIAL_H