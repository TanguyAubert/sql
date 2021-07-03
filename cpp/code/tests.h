
#ifndef TESTS_H
#define TESTS_H

#include "table.h"

namespace SQL
{
    Table test_1()
    {
        return Table("orc_decla_t3", "C1", "C2", "C3", "C4", "C7")
        .drop("C7")
        .select("C1", "C3")
        .create_column("C9", "C1 + C3")
        .filter("C3 = 'Un peu de texte !'")
        .filter("C9 < 2.3")
        .rename("C3", "C6")
        .create_column("C10", "1 - C6")
        .create_column("C11", "C10 / 100")
        .create_column("C11", "C11 * 2")
        .create_column("C12", "C1 * 0.5")
        .filter("C6 = C1")
        ;
    }

    std::string expected_result_1()
    {
        /*
        return std::string(
            "SELECT C1, C6, C9, C10, C11 * 2 AS C11, C1 * 0.5 AS C12"
            " FROM (SELECT A3.*, C10 / 100 AS C11"
            " FROM (SELECT C1, C3 AS C6, C9, 1 - C3 AS C10"
            " FROM (SELECT C1, C3, C1 + C3 AS C9"
            " FROM orc_decla_t3 AS A1"
            " WHERE C3 = 'Un peu de texte !') AS A2"
            " WHERE C9 < 2.3) AS A3) AS A4"
            " WHERE C6 = C1"
        );
        */

        return std::string(
            "SELECT C1, C6, C9, C10, C11 * 2 AS C11, C1 * 0.5 AS C12"
            " FROM (SELECT C1, C6, C9, C10, C10 / 100 AS C11"
            " FROM (SELECT C1, C3 AS C6, C9, 1 - C3 AS C10"
            " FROM (SELECT C1, C3, C1 + C3 AS C9"
            " FROM orc_decla_t3 AS A1"
            " WHERE C3 = 'Un peu de texte !') AS A2"
            " WHERE C9 < 2.3) AS A3) AS A4"
            " WHERE C6 = C1"
        );
    }

    Table test_2()
    {
        return test_1()
        .aggregate
        (
            {
                Variable("C1", Utils::sum("C1")),
                Variable("C2", Utils::count())
            },
            By("C6")
        )
        .rename("C1", "C13")
        .rename("C6", "C14")
        ;
    }

    std::string expected_result_2()
    {
        /*
        return std::string(
            "SELECT C6 AS C14, SUM(C1) AS C13, COUNT(*) AS C2"
            " FROM (SELECT A4.*"
            " FROM (SELECT A3.*"
            " FROM (SELECT C1, C3 AS C6"
            " FROM (SELECT C1, C3, C1 + C3 AS C9"
            " FROM orc_decla_t3 AS A1"
            " WHERE C3 = 'Un peu de texte !') AS A2"
            " WHERE C9 < 2.3) AS A3) AS A4"
            " WHERE C6 = C1) AS A5"
            " GROUP BY C6"
        );
        */

        return std::string(
            "SELECT C6 AS C14, SUM(C1) AS C13, COUNT(*) AS C2"
            " FROM (SELECT C1, C6"
            " FROM (SELECT C1, C6"
            " FROM (SELECT C1, C3 AS C6"
            " FROM (SELECT C1, C3, C1 + C3 AS C9"
            " FROM orc_decla_t3 AS A1"
            " WHERE C3 = 'Un peu de texte !') AS A2"
            " WHERE C9 < 2.3) AS A3) AS A4"
            " WHERE C6 = C1) AS A5"
            " GROUP BY C6"
        );
    }

    Table test_3()
    {
        auto table_1 = Table("T1", "C1", "C2", "C3", "C10")
        .rename("C3", "C5")
        .create_column("C6", "1 - C5")
        ;

        auto table_2 = Table("T2", "C4", "C2", "C1")
        .create_column("C7", "C4 * 100")
        .drop("C4")
        ;

        auto table_3 = table_1.inner_join(table_2, "C1", "C2");
        table_3.rename("C5", "C8");
        table_3.create_column("C9", "C6 / C7");
        table_3.select("C1", "C2", "C9");

        return table_3;
    }

    std::string expected_result_3()
    {
        /*
        return std::string(
            "SELECT C1, C2, C6 / C7 AS C9"
            " FROM (SELECT A7.*, A6.C6 AS C6"
            " FROM (SELECT C1, C2, 1 - C3 AS C6"
            " FROM T1 AS A1) AS A6"
            " INNER JOIN (SELECT C2, C1, C4 * 100 AS C7"
            " FROM T2 AS A3) AS A7"
            " ON (A6.C1 = A7.C1 AND A6.C2 = A7.C2)) AS A5"
        );
        */

        return std::string(
            "SELECT C1, C2, C6 / C7 AS C9"
            " FROM (SELECT A2.C1 AS C1, A2.C2 AS C2, A2.C6 AS C6, A4.C7 AS C7"
            " FROM (SELECT C1, C2, 1 - C3 AS C6"
            " FROM T1 AS A1) AS A2"
            " INNER JOIN (SELECT C2, C1, C4 * 100 AS C7"
            " FROM T2 AS A3) AS A4"
            " ON (A2.C1 = A4.C1 AND A2.C2 = A4.C2)) AS A5"
        );
    }
    
    Table test_4()
    {
        auto table = Table("T1", "C1", "C2", "C3")
        .select("C1", "C3");

        return table;
    }

    std::string expected_result_4()
    {
        return std::string(
            "SELECT C1, C3 FROM T1 AS A1"
        );
    }

    Table test_5()
    {
        return Table("T1", "C1", "C2", "C3")
        .drop("C2")
        ;
    }

    std::string expected_result_5()
    {
        return expected_result_4();
    }

    Table test_6()
    {
        return Table("T1", "C1", "C2", "C3")
        .create_column("C4", "C3 / C2")
        .create_column("C5", "C4 - 1")
        ;
    }

    std::string expected_result_6()
    {
        /*
        return std::string(
            "SELECT A2.*, C4 - 1 AS C5"
            " FROM (SELECT C1, C2, C3, C3 / C2 AS C4"
            " FROM T1 AS A1) AS A2"
        );
        */

        return std::string(
            "SELECT C1, C2, C3, C4, C4 - 1 AS C5"
            " FROM (SELECT C1, C2, C3, C3 / C2 AS C4"
            " FROM T1 AS A1) AS A2"
        );
    }

    Table test_7()
    {
        auto table_1 = Table("T1", "C1", "C2", "C3");
        auto table_2 = Table("T2", "C4", "C2", "C1");
        auto table = table_1.left_join(table_2, "C1", "C2");

        return table;
    }

    std::string expected_result_7()
    {
        /*
        return std::string(
            "SELECT A4.*, A5.C4 AS C4"
            " FROM T1 AS A4"
            " LEFT JOIN T2 AS A5"
            " ON (A4.C1 = A5.C1 AND A4.C2 = A5.C2)"
        );
        */

        return std::string(
            "SELECT A1.C1 AS C1, A1.C2 AS C2, A1.C3 AS C3, A2.C4 AS C4"
            " FROM T1 AS A1"
            " LEFT JOIN T2 AS A2"
            " ON (A1.C1 = A2.C1 AND A1.C2 = A2.C2)"
        );
    }

    Table test_8()
    {
        auto table_1 = Table("T1", "C1", "C2", "C3");
        auto table_2 = Table("T2", "C4", "C2", "C1");

        table_1.create_column("C5", "C3 * 100");
        table_1.rename("C5", "C6");
        table_1.drop("C3");

        auto table = table_1.left_join(table_2, "C1", "C2");

        return table;
    }

    std::string expected_result_8()
    {
        /*
        return std::string(
            "SELECT A5.*, A6.C4 AS C4"
            " FROM (SELECT C1, C2, C3 * 100 AS C6"
            " FROM T1 AS A1) AS A5"
            " LEFT JOIN T2 AS A6"
            " ON (A5.C1 = A6.C1 AND A5.C2 = A6.C2)"
        );
        */

        return std::string(
            "SELECT A3.C1 AS C1, A3.C2 AS C2, A3.C6 AS C6, A2.C4 AS C4"
            " FROM (SELECT C1, C2, C3 * 100 AS C6"
            " FROM T1 AS A1) AS A3"
            " LEFT JOIN T2 AS A2"
            " ON (A3.C1 = A2.C1 AND A3.C2 = A2.C2)"
        );
    }

    Table test_9()
    {
        return Table("T1", "C1", "C2", "C3")
        .filter("C1 = C3")
        ;
    }

    std::string expected_result_9()
    {
        return std::string(
            "SELECT C1, C2, C3"
            " FROM T1 AS A1"
            " WHERE C1 = C3"
        );
    }

    Table test_10()
    {
        return Table("T1", "C1", "C2", "C3", "C4")
        .select("C1", "C3", "C4")
        .filter("C1 = C4")
        .filter("C1 < 100")
        .drop("C4")
        ;
    }

    std::string expected_result_10()
    {
        return std::string(
            "SELECT C1, C3"
            " FROM T1 AS A1"
            " WHERE C1 = C4"
            " AND C1 < 100"
        );
    }

    Table test_11()
    {
        auto table = Table("T1", "C1", "C2", "C3", "C5")
        .aggregate
        (
            {
                Variable("C3", Utils::sum("C3")),
                Variable("C4", Utils::count())
            },
            By("C1", "C2")
        );
        
        return table;
    }

    std::string expected_result_11()
    {
        return std::string(
            "SELECT C1, C2, SUM(C3) AS C3, COUNT(*) AS C4"
            " FROM T1 AS A1"
            " GROUP BY C1, C2"
        );
    }

    Table test_12()
    {
        auto table_1 = Table("T1", "C1", "C2", "C3");
        auto table_2 = table_1;

        table_1.rename("C2", "C4");

        return table_2;
    }

    std::string expected_result_12()
    {
        return std::string(
            "SELECT C1, C2, C3 FROM T1 AS A1"
        );
    }

    Table test_13()
    {
        auto table_1 = Table("T1", "C1", "C2", "C3");
        auto table_2 = Table("T2", "C6", "C1", "C4", "C2", "C5");

        auto table = table_1.stack(table_2);

        return table;
    }

    std::string expected_result_13()
    {
        return std::string(
            "SELECT C1, C2, C3, NULL AS C6, NULL AS C4, NULL AS C5 FROM T1 AS A1"
            " UNION ALL"
            " SELECT C1, C2, NULL AS C3, C6, C4, C5 FROM T2 AS A2"
        );
    }

    Table test_14()
    {
        auto table_1 = Table("T1", "C1", "C2", "C3");
        auto table_2 = Table("T2", "C1", "C3", "C4");

        auto table = table_1.inner_join(table_2, "C1", "C3");

        table.filter("C2 = C4");

        return table;
    }

    std::string expected_result_14()
    {
        /*
        return std::string(
            "SELECT A3.*"
            " FROM (SELECT A5.*, A4.C2 AS C2"
            " FROM T1 AS A4"
            " INNER JOIN T2 AS A5"
            " ON (A4.C1 = A5.C1 AND A4.C3 = A5.C3)) AS A3"
            " WHERE C2 = C4"
        );
        */

        return std::string(
            "SELECT C1, C2, C3, C4"
            " FROM (SELECT A1.C1 AS C1, A1.C3 AS C3, A1.C2 AS C2, A2.C4 AS C4"
            " FROM T1 AS A1"
            " INNER JOIN T2 AS A2"
            " ON (A1.C1 = A2.C1 AND A1.C3 = A2.C3)) AS A3"
            " WHERE C2 = C4"
        );
    }

    Table test_15()
    {
        auto table = Table("T1", "C1", "C2", "C3")
        .aggregate
        (
            {
                Variable("C3", Utils::sum("C3")),
                Variable("C5", Utils::count())
            },
            By("C2", "C1")
        )
        .filter("C2 = 'Du texte'")
        .filter("C5 > 10")
        ;

        return table;
    }

    std::string expected_result_15()
    {
        return std::string(
            "SELECT C2, C1, SUM(C3) AS C3, COUNT(*) AS C5"
            " FROM T1 AS A1"
            " GROUP BY C2, C1"
            " HAVING C2 = 'Du texte' AND C5 > 10"
        );
    }

    Table test_16()
    {
        auto table_1 = Table("T1", "C1", "C2", "C3");
        auto table_2 = Table("T2", "C1", "C3", "C4");
        auto table_3 = Table("T3", "C2", "C5", "C6");

        auto table = table_1.stack(table_2).stack(table_3);

        return table;
    }

    std::string expected_result_16()
    {
        return std::string(
            "SELECT C1, C2, C3, NULL AS C4, NULL AS C5, NULL AS C6 FROM T1 AS A1"
            " UNION ALL"
            " SELECT C1, NULL AS C2, C3, C4, NULL AS C5, NULL AS C6 FROM T2 AS A2"
            " UNION ALL"
            " SELECT NULL AS C1, C2, NULL AS C3, NULL AS C4, C5, C6 FROM T3 AS A3"
        );
    }

    Table test_17()
    {
        auto table = Table("T1", "C1", "C2", "C3")
        .filter("C1 > 10")
        .unique()
        .rename("C3", "C4")
        ;

        return table;
    }

    std::string expected_result_17()
    {
        /*
        return std::string(
            "SELECT DISTINCT C1, C2, C3 AS C4 FROM T1 AS A1 WHERE C1 > 10"
        );
        */

        return std::string(
            "SELECT C1, C2, C3 AS C4"
            " FROM (SELECT DISTINCT C1, C2, C3"
            " FROM T1 AS A1"
            " WHERE C1 > 10) AS A2"
        );
    }

    Table test_18()
    {
        auto table = Table("T1", "C1", "C2", "C3")
        .aggregate(
            {
                Variable("C3", "SUM(C3)"),
                Variable("C4", "COUNT(*)"),
                Variable("C5", "COUNT(*)")
            },
            By("C1", "C2")
        )
        .filter("C4 > 3")
        .select("C1", "C3")
        ;

        return table;
    }

    std::string expected_result_18()
    {
        return std::string(
            "SELECT C1, C3 FROM (SELECT C1, SUM(C3) AS C3, COUNT(*) AS C4 FROM T1 AS A1 GROUP BY C1, C2 HAVING C4 > 3) AS A2"
        );
    }

    Table test_19()
    {
        auto table = Table("T1", "C1", "C2", "C3")
        .filter("C2 = 'Je m\\'appelle Tanguy !'")
        ;

        return table;
    }

    std::string expected_result_19()
    {
        return std::string(
            "SELECT C1, C2, C3 FROM T1 AS A1 WHERE C2 = 'Je m\\'appelle Tanguy !'"
        );
    }

    Table test_20()
    {
        auto table = Table("T1", "C1", "C2", "C3")
        .aggregate(
            {
                Variable("C3", "SUM(C3)"),
                Variable("C4", "COUNT(*)")
            },
            By("C1", "C2")
        )
        .filter("C4 > 3")
        .rename("C4", "C7")
        ;

        return table;
    }

    std::string expected_result_20()
    {
        return std::string(
            "SELECT C1, C2, SUM(C3) AS C3, COUNT(*) AS C7 FROM T1 AS A1 GROUP BY C1, C2 HAVING C7 > 3"
        );
    }

    void run_one_test(std::size_t i, Table (*test)(), std::string (expected_result)())
    {
        Utils::reset_alias_id();

        std::string actual;

        try
        {
            auto table = test();

            if (table.has_errors())
            {
                table.display_errors();
            }

            actual = table.render();
        }
        catch (...)
        {
            std::cout << "Test N°" << i << " : ERROR" << std::endl;

            return;
        }

        auto expected = expected_result();

        bool actual_matches_expected = actual == expected;

        std::cout << std::boolalpha;

        if (actual_matches_expected)
        {
            std::cout << "Test N°" << i << " : " << actual_matches_expected << std::endl;
        }
        else
        {
            std::cout << std::endl;
            std::cout << "Test N°" << i << " : " << actual_matches_expected << std::endl;
            std::cout << "ACTUAL : " << std::endl;
            std::cout << actual << std::endl;
            std::cout << "EXPECTED : " << std::endl;
            std::cout << expected << std::endl;
            std::cout << std::endl;
        }
    }

    void run_all_tests()
    {
        int i = 0;

        run_one_test(++i, test_1, expected_result_1);
        run_one_test(++i, test_2, expected_result_2);
        run_one_test(++i, test_3, expected_result_3);
        run_one_test(++i, test_4, expected_result_4);
        run_one_test(++i, test_5, expected_result_5);
        run_one_test(++i, test_6, expected_result_6);
        run_one_test(++i, test_7, expected_result_7);
        run_one_test(++i, test_8, expected_result_8);
        run_one_test(++i, test_9, expected_result_9);
        run_one_test(++i, test_10, expected_result_10);
        run_one_test(++i, test_11, expected_result_11);
        run_one_test(++i, test_12, expected_result_12);
        run_one_test(++i, test_13, expected_result_13);
        run_one_test(++i, test_14, expected_result_14);
        run_one_test(++i, test_15, expected_result_15);
        run_one_test(++i, test_16, expected_result_16);
        run_one_test(++i, test_17, expected_result_17);
        run_one_test(++i, test_18, expected_result_18);
        run_one_test(++i, test_19, expected_result_19);
        run_one_test(++i, test_20, expected_result_20);
    }
}

#endif // TESTS_H