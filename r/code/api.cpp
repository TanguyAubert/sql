
#include "C:/Tanguy/Programs/projects/sql/cpp/code/table.h"
#include <Rcpp.h>

using namespace Rcpp;

// [[Rcpp::export]]
void set_sgbd(const std::string & SGBD = "HIVE")
{
    if (SGBD == "ORACLE")
    {
        Options::set_sgbd(Options::SGBD::ORACLE);
    }
    else
    {
        Options::set_sgbd(Options::SGBD::HIVE);
    }
}

// [[Rcpp::export]]
std::string get_sgbd()
{
    if (Options::get_sgbd() == Options::SGBD::ORACLE)
    {
        return "ORACLE";
    }
    else
    {
        return "HIVE";
    }
}

class SQLTable
{
    private:

        SQL::Table table;

    public:

        SQLTable(std::string name, std::vector<std::string> columns) : table(name, columns)
        {

        }

        SQLTable(const SQLTable & other) = default;

        std::vector<std::string> get_keys() const
        {
            return to_vector(table.get_keys());
        }

        void set_keys(std::vector<std::string> keys)
        {
            table.set_keys(keys);
        }

        std::vector<std::string> get_columns() const
        {
            return to_vector(table.get_columns());
        }

        bool has_column(std::string name)
        {
            return table.has_column(name);
        }
        
        SQLTable copy() const
        {
            return SQLTable(*this);
        }

        void rename(std::string old_name, std::string new_name)
        {
            table.rename(old_name, new_name);
        }

        void prefix(std::string prefix, std::vector<std::string> columns)
        {
            table.prefix(prefix, columns);
        }

        void prefix_all_except(std::string prefix, std::vector<std::string> exceptions)
        {
            table.prefix_all_except(prefix, exceptions);
        }

        void select(std::vector<std::string> columns)
        {
            table.select(columns);
        }

        void drop(std::vector<std::string> columns)
        {
            table.drop(columns);
        }

        void drop_key(std::string name, std::string value)
        {
            table.drop_key(name, value);
        }

        void filter(std::vector<std::string> tokens)
        {
            table.filter(tokens);
        }

        void create_column(std::string name, std::vector<std::string> tokens)
        {
            table.create_column(name, tokens);
        }

        void aggregate(Rcpp::List list_of_variables, std::vector<std::string> by)
        {
            std::vector<SQL::Variable> variables;
            
            std::vector<std::string> names = list_of_variables.names();
            
            for (int i = 0 ; i < list_of_variables.size() ; ++i)
            {
                std::string name = names[i];
                std::string code = list_of_variables[i];
                
                variables.emplace_back(name, code);
            }
            
            table.aggregate(variables, SQL::By(by));
        }

        void inner_join(SQLTable other, std::vector<std::string> by)
        {
            table.inner_join(other.table, by);
        }

        void left_join(SQLTable other, std::vector<std::string> by)
        {
            table.left_join(other.table, by);
        }

        void right_join(SQLTable other, std::vector<std::string> by)
        {
            table.right_join(other.table, by);
        }

        void full_join(SQLTable other, std::vector<std::string> by)
        {
            table.full_join(other.table, by);
        }

        void unique()
        {
            table.unique();
        }

        void stack(SQLTable other)
        {
            table.stack(other.table);
        }

        std::string render() const
        {
            std::string output = table.render();
            
            SQL::Utils::reset_alias_id();
            
            return output;
        }

        void print() const
        {
            table.print();
        }

        bool has_errors() const
        {
            return table.has_errors();
        }

        void display_errors() const
        {
            table.display_errors();
        }
        
        void ignore_latest_error()
        {
            table.ignore_latest_error();
        }

    private:

        std::vector<std::string> to_vector(const SQL::Names & names) const
        {
            std::vector<std::string> output;

            for (auto && name : names)
            {
                output.emplace_back(name);
            }

            return output;
        }
};

RCPP_EXPOSED_CLASS(SQLTable);

RCPP_MODULE(SQLModule)
{
    class_<SQLTable>("SQLTable")

    .constructor<std::string, std::vector<std::string> >()

    .method("get_keys", &SQLTable::get_keys)
    .method("set_keys", &SQLTable::set_keys)
    .method("get_columns", &SQLTable::get_columns)
    .method("has_column", &SQLTable::has_column)
    .method("copy", &SQLTable::copy)
    .method("rename", &SQLTable::rename)
    .method("prefix", &SQLTable::prefix)
    .method("prefix_all_except", &SQLTable::prefix_all_except)
    .method("select", &SQLTable::select)
    .method("drop", &SQLTable::drop)
    .method("drop_key", &SQLTable::drop_key)
    .method("filter", &SQLTable::filter)
    .method("create_column", &SQLTable::create_column)
    .method("aggregate", &SQLTable::aggregate)
    .method("inner_join", &SQLTable::inner_join)
    .method("left_join", &SQLTable::left_join)
    .method("right_join", &SQLTable::right_join)
    .method("full_join", &SQLTable::full_join)
    .method("unique", &SQLTable::unique)
    .method("stack", &SQLTable::stack)
    .method("render", &SQLTable::render)
    .method("print", &SQLTable::print)
    .method("has_errors", &SQLTable::has_errors)
    .method("display_errors", &SQLTable::display_errors)
    .method("ignore_latest_error", &SQLTable::ignore_latest_error)
    ;
}
