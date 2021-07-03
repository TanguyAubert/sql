
#ifndef SQL_H
#define SQL_H

#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <set>
#include <algorithm>

class Names;

namespace SQL
{
    namespace Utils
    {
        template <typename T>
        std::string to_string(T value)
        {
            std::stringstream out;
            out << value;
            return out.str();
        }

        inline int to_int(const std::string & text)
        {
            char* p;

            int result = (int) strtol(text.c_str(), &p, 10);

            bool success = (*p == 0 && !text.empty());

            if (success)
            {
                return result;
            }
            else
            {
                std::cerr << "ERROR : could not convert " << text << " to an integer !" << std::endl;

                return std::numeric_limits<int>::max();
            }
        }

        template <typename T>
        typename std::enable_if<std::is_integral<T>::value>::type
        flatten_annex(std::vector<std::string> & output, T value)
        {
            output.emplace_back(to_string(value));
        }

        template <typename T>
        typename std::enable_if<std::is_convertible<T, std::string>::value>::type
        flatten_annex(std::vector<std::string> & output, T value)
        {
            output.emplace_back(value);
        }

        template <typename T>
        typename std::enable_if<!std::is_convertible<T, std::string>::value && !std::is_integral<T>::value>::type
        flatten_annex(std::vector<std::string> & output, T value)
        {
            for (auto && element : value)
            {
                flatten_annex(output, element);
            }
        }

        inline void flatten_annex(std::vector<std::string> & output)
        {
            // Nothing to do
        }

        template <typename T, typename... Args>
        void flatten_annex(std::vector<std::string> & output, T value, Args ... args)
        {
            flatten_annex(output, value);
            flatten_annex(output, args...);
        }

        template <typename... Args>
        std::vector<std::string> flatten(Args ... args)
        {
            std::vector<std::string> output;

            flatten_annex(output, args...);

            return output;
        }
        
        template <typename T>
        std::string collapse(const std::vector<T> & values, const std::string & separator)
        {
            std::string output;

            for (std::size_t i = 0 ; i < values.size() ; ++i)
            {
                if (i > 0)
                {
                    output += separator;
                }

                output += to_string(values[i]);
            }

            return output;
        }

        template <typename... Args>
        std::string concatenate(Args ...args)
        {
            return collapse(flatten(args...), "");
        }

        inline int generate_unique_id()
        {
            static int id = 0;

            return ++id;
        }

        static int alias_id = 0;

        inline int generate_alias_id()
        {
            return ++alias_id;
        }

        inline void reset_alias_id()
        {
            alias_id = 0;
        }

        inline std::string create_alias()
        {
            return concatenate("A", generate_alias_id());
        }
        
        inline std::string to_upper(std::string text)
        {
            std::transform(text.begin(), text.end(), text.begin(), ::toupper);

            return text;
        }

        inline bool equals(const std::string & x, const std::string y)
        {
            return (to_upper(x) == to_upper(y));
        }
        
        inline std::string & replace_all(std::string & text, const std::string & from, const std::string & to)
        {
            if(from.empty())
            {
                return text;
            }

            std::size_t first = 0;

            while ((first = text.find(from, first)) != std::string::npos)
            {
                text.replace(first, from.length(), to);
                first += to.length();
            }
            
            return text;
        }
        
        inline std::string single_quote(std::string text)
        {
            replace_all(text, "'", "\\'");

            return std::string("'") + text + "'";
        }

        inline std::vector<std::string> deduplicate(const std::vector<std::string> & values)
        {
            auto is_less = [](const std::string & x, const std::string & y) { return to_upper(x) < to_upper(y); };
            
            std::set<std::string, decltype(is_less)> uniques(is_less);

            for (auto && value : values)
            {
                uniques.insert(value);
            }
            
            std::vector<std::string> output;

            for (auto && value : uniques)
            {
                output.emplace_back(value);
            }

            return output;
        }

        template <typename... Args>
        std::string sum(Args ... args)
        {
            return concatenate("SUM(", args..., ")");
        }

        inline std::string count(const std::string & variable = "*")
        {
            return std::string("COUNT(") + variable + ")";
        }

        template <typename... Args>
        std::string over_partition_by(Args ... args)
        {
            return concatenate("OVER (PARTITION BY ", collapse(flatten(args...), ", "), ")");
        }
        
        template <typename T>
        bool are_same(const std::vector<T> & lhs, const std::vector<T> & rhs)
        {
            auto size = lhs.size();

            if (size != rhs.size()) return false;

            for (std::size_t i = 0 ; i < size ; ++i)
            {
                if (!lhs[i].is_same_as(rhs[i])) return false;
            }

            return true;
        }
    }
};

#endif // SQL_H