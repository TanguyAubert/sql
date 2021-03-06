
#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include "names.h"

namespace SQL
{
    // https://www.w3schools.com/sql/sql_ref_sqlserver.asp
    
    Names functions = {
        "ASCII",
        "CHAR",
        "CHARINDEX",
        "CONCAT",
        "CONCAT_WS",
        "DATALENGTH",
        "DIFFERENCE",
        "FORMAT",
        "LEFT",
        "LEN",
        "LOWER",
        "LTRIM",
        "NCHAR",
        "PATINDEX",
        "QUOTENAME",
        "REPLACE",
        "REPLICATE",
        "REVERSE",
        "RIGHT",
        "RTRIM",
        "SOUNDEX",
        "SPACE",
        "STR",
        "STUFF",
        "SUBSTRING",
        "TRANSLATE",
        "TRIM",
        "UNICODE",
        "UPPER",
        "ABS",
        "ACOS",
        "ASIN",
        "ATAN",
        "ATN2",
        "AVG",
        "CEILING",
        "COUNT",
        "COS",
        "COT",
        "DEGREES",
        "EXP",
        "FLOOR",
        "LOG",
        "LOG10",
        "MAX",
        "MIN",
        "PI",
        "POWER",
        "RADIANS",
        "RAND",
        "ROUND",
        "SIGN",
        "SIN",
        "SQRT",
        "SQUARE",
        "SUM",
        "TAN",
        "CURRENT_TIMESTAMP",
        "DATEADD",
        "DATEDIFF",
        "DATEFROMPARTS",
        "DATENAME",
        "DATEPART",
        "DAY",
        "GETDATE",
        "GETUTCDATE",
        "ISDATE",
        "MONTH",
        "SYSDATETIME",
        "YEAR",
        "CAST",
        "COALESCE",
        "CONVERT",
        "CURRENT_USER",
        "IIF",
        "ISNULL",
        "ISNUMERIC",
        "NULLIF",
        "SESSION_USER",
        "SESSIONPROPERTY",
        "SYSTEM_USER",
        "USER_NAME",

        // Ajouts
        "NVL",
        "MONTHS_BETWEEN",
        "TO_DATE",
        "SUBSTR",
        "REGEXP_EXTRACT"
    };

    bool is_a_function(const std::string & name)
    {
        return functions.contains(name);
    }
}

#endif // FUNCTIONS_H