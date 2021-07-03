#ifndef NODE_H
#define NODE_H

#include <string>
#include <vector>
#include <memory>
#include "columns.h"
#include "names.h"
#include "options.h"
#include "sql.h"

namespace SQL
{
    class Node
    {
        public:
        
            enum class Type {INITIAL, STANDARD, AGGREGATION, MERGER, STACKER};

        protected:

            std::string alias;

            Type type;
            
            Names keys;
            
        public:

            Node(Type type) : alias(Utils::create_alias()), type(type)
            {
                // NOTHING TO DO
            }

            Node(const Node & other) = default;

            virtual std::string render() const = 0;

            virtual std::shared_ptr<Node> copy() const = 0;

            virtual bool keep(const Names & selection, bool recursive) = 0;

            virtual void rename(const std::string & old_name, const std::string & new_name) = 0;
            
            virtual Columns get_columns() const = 0;
            
            virtual void print(const std::string & tabulation) const = 0;

            virtual Names get_sources() const = 0;

            virtual std::shared_ptr<Node> simplify(const std::shared_ptr<Node> & self)
            {
                return self;
            }

            void set_keys(const Names & keys)
            {
                this->keys = keys;
            }

            const Names & get_keys() const
            {
                return keys;
            }

            void rename_key(const std::string & old_name, const std::string & new_name)
            {
                keys.rename(old_name, new_name);
            }

            bool has_keys() const
            {
                return keys.size() > 0;
            }

            virtual std::string render_with_alias(const std::string & alias) const
            {
                if (Options::get_sgbd() == Options::SGBD::ORACLE)
                {
                    return std::string("(") + render() + ") " + alias;
                }
                else
                {
                    return std::string("(") + render() + ") AS " + alias;
                }
            }

            std::string get_alias() const
            {
                return alias;
            }

            Type get_type() const
            {
                return type;
            }

        protected:
        
            void remove_all_keys_if_any_one_of_them_is_not_in_the_selection(const Names & selection)
            {
                if (!keys.substract(selection).empty())
                {
                    keys.clear();
                }
            }
    };
}

#endif // NODE_H