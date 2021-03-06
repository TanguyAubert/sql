
#ifndef KEYWORDS_H
#define KEYWORDS_H

#include "names.h"

namespace SQL
{
    // https://www.w3schools.com/sql/sql_ref_keywords.asp
    
    static Names keywords = {
        "ADD",
        "ADD CONSTRAINT",
        "ALTER",
        "ALTER COLUMN",
        "ALTER TABLE",
        "ALL",
        "AND",
        "ANY",
        "AS",
        "ASC",
        "BACKUP DATABASE",
        "BETWEEN",
        "CASE",
        "CHECK",
        "COLUMN",
        "CONSTRAINT",
        "CREATE",
        "CREATE DATABASE",
        "CREATE INDEX",
        "CREATE OR REPLACE VIEW",
        "CREATE TABLE",
        "CREATE PROCEDURE",
        "CREATE UNIQUE INDEX",
        "CREATE VIEW",
        "DATABASE",
        "DEFAULT",
        "DELETE",
        "DESC",
        "DISTINCT",
        "DROP",
        "DROP COLUMN",
        "DROP CONSTRAINT",
        "DROP DATABASE",
        "DROP DEFAULT",
        "DROP INDEX",
        "DROP TABLE",
        "DROP VIEW",
        "EXEC",
        "EXISTS",
        "FOREIGN KEY",
        "FROM",
        "FULL OUTER JOIN",
        "GROUP BY",
        "HAVING",
        "IN",
        "INDEX",
        "INNER JOIN",
        "INSERT INTO",
        "INSERT INTO SELECT",
        "IS NULL",
        "IS NOT NULL",
        "JOIN",
        "LEFT JOIN",
        "LIKE",
        "LIMIT",
        "NOT",
        "NOT NULL",
        "OR",
        "ORDER BY",
        "OUTER JOIN",
        "PRIMARY KEY",
        "PROCEDURE",
        "RIGHT JOIN",
        "ROWNUM",
        "SELECT",
        "SELECT DISTINCT",
        "SELECT INTO",
        "SELECT TOP",
        "SET",
        "TABLE",
        "TOP",
        "TRUNCATE TABLE",
        "UNION",
        "UNION ALL",
        "UNIQUE",
        "UPDATE",
        "VALUES",
        "VIEW",
        "WHERE",
        
        // Ajout(s)
        "NULL",
        "PARTITION BY",
        "OVER",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "IS",
        "INT",
        "DECIMAL",
        "RLIKE",
        "STRING"
    };
    
    inline bool is_a_keyword(const std::string & name)
    {
        return keywords.contains(name);
    }
}

#endif // KEYWORDS_H