#ifndef EXAMPLES_ORC_DECLA_H
#define EXAMPLES_ORC_DECLA_H

#include "examples-communs.h"
#include "examples-tables.h"

namespace SQL
{
    void compute_pge_orc_decla(Table & table, const std::string & ref_date, const std::string & ra, const std::string & oa)
    {
        auto T7 = get_orc_decla_t7();
        auto T8 = get_orc_decla_t8();
        auto code_riad_ci_pge = get_code_riad_ci();

        apply_filters(T7, ref_date, ra, oa);
        apply_filters(T8, ref_date, ra, oa);
        apply_filters(code_riad_ci_pge, ref_date, ra, oa);

        code_riad_ci_pge
        .select("ra", "conterparty_identifier", "code_riad")
        .rename("conterparty_identifier", "prtctn_prvdr_cd")
        .rename("code_riad", "prtctn_prvdr_code_riad")
        ;

        auto pge = T7
        .copy()
        .select("ref_date", "ra", "oa", "prtctn_id", "prtctn_prvdr_cd")
        .left_join(code_riad_ci_pge, "ra", "prtctn_prvdr_cd")
        ;

        pge = T8
        .copy()
        .select("ref_date", "ra", "oa", "prtctn_id", "cntrct_id", "instrmnt_id")
        .left_join(pge, "ref_date", "ra", "oa", "prtctn_id")
        .filter("prtctn_prvdr_code_riad = 'FR130019763'")
        .select("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
        .unique()
        .set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
        .create_column("pge", 1)
        ;

        table.left_join(pge, "ref_date", "ra", "oa", "cntrct_id", "instrmnt_id");
        table.create_column("pge", "CASE WHEN pge = 1 THEN 1 ELSE 0 END");
    }

    void example_2()
    {
        try
        {
            auto begin = std::chrono::steady_clock::now();

            std::string ref_date = "2020-11";
            std::string ra = "30007";
            std::string oa = "30007";

            std::vector<std::string> keys = {"ref_date", "ra", "oa", "cntrct_id", "instrmnt_id"};

            auto T2 = get_orc_decla_t2();
            auto T3 = get_orc_decla_t3();
            auto T6 = get_orc_decla_t6();
            auto T4C = get_orc_decla_t4();
            auto T4D = get_orc_decla_t4();
            auto T5 = get_orc_decla_t5();
            auto code_riad_ci = get_code_riad_ci();
            auto golden_record_t1 = get_golden_record_t1();
            auto T1 = get_orc_decla_t1();
            auto T9 = get_orc_decla_t9();
            auto T10 = get_orc_decla_t10();

            apply_filters(T2, ref_date, ra, oa);
            apply_filters(T3, ref_date, ra, oa);
            apply_filters(T6, "", ra, oa);
            apply_filters(T4C, ref_date, ra, oa);
            apply_filters(T4D, ref_date, ra, oa);
            apply_filters(T5, ref_date, ra, oa);
            apply_filters(code_riad_ci, ref_date, ra, oa);
            apply_filters(T1, ref_date, ra, oa);
            apply_filters(T9, ref_date, ra, oa);
            apply_filters(T10, ref_date, ra, oa);
                
            T6.drop_key("ref_date", get_previous_quarter(ref_date));

            auto instruments = T3
            .left_join(T2, "ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
            .left_join(T6, "ra", "oa", "cntrct_id", "instrmnt_id")
            ;

            T4C
            .drop_key("entty_rl", 1)
            .rename("entty_id", "cre_entty_id")
            .create_column("number_of_creditors", Utils::count(), Utils::over_partition_by(keys))
            ;

            T5
            .rename("jnt_lblty_amnt", "deb_jnt_lblty_amnt")
            .rename("entty_id", "deb_entty_id")
            ;

            T4D
            .drop_key("entty_rl", 2)
            .rename("entty_id", "deb_entty_id")
            .left_join(T5, keys, "deb_entty_id")
            .create_column("number_of_debtors", Utils::count(), Utils::over_partition_by(keys))
            .create_column(
                "total_deb_jnt_lblty_amnt",
                "SUM"
                "("
                    "CASE"
                    " WHEN deb_jnt_lblty_amnt IS NULL"
                    " THEN 0"
                    " ELSE deb_jnt_lblty_amnt"
                    " END"
                ") OVER (PARTITION BY ref_date, ra, oa, cntrct_id, instrmnt_id)")
            .create_column("main_debtor", "CASE WHEN NVL(deb_jnt_lblty_amnt,0) = MAX(NVL(deb_jnt_lblty_amnt,0)) OVER (PARTITION BY ref_date, ra, oa, cntrct_id, instrmnt_id) THEN 1 ELSE 0 END")
            .create_column("number_of_main_debtors", "SUM(main_debtor) OVER (PARTITION BY ref_date, ra, oa, cntrct_id, instrmnt_id)")
            ;

            code_riad_ci
            .select("ra", "conterparty_identifier", "code_riad")
            .rename("conterparty_identifier", "entty_id")
            ;

            golden_record_t1
            .prefix_all_except("gr_", "code_riad")
            .inner_join(code_riad_ci, "code_riad")
            ;

            T1
            .create_column(
                "insttnl_sctr",
                "CASE",
                " WHEN insttnl_sctr = 'S122_A' THEN 'S122'",
                " WHEN insttnl_sctr = 'S122_B' THEN 'S122'",
                " WHEN insttnl_sctr = 'S125_A' THEN 'S125'",
                " WHEN insttnl_sctr = 'S125_B' THEN 'S125'",
                " WHEN insttnl_sctr = 'S125_C' THEN 'S125'",
                " WHEN insttnl_sctr = 'S125_NN' THEN 'S125'",
                " WHEN insttnl_sctr = 'S126_A' THEN 'S126'",
                " WHEN insttnl_sctr = 'S126_D' THEN 'S126'",
                " WHEN insttnl_sctr = 'S13111' THEN 'S1311'",
                " WHEN insttnl_sctr = 'S13112' THEN 'S1311'",
                " WHEN insttnl_sctr = 'S1313_A' THEN 'S1313'",
                " WHEN insttnl_sctr = 'S1313_B' THEN 'S1313'",
                " WHEN insttnl_sctr = 'S1314_A' THEN 'S1314'",
                " WHEN insttnl_sctr = 'S1314_B' THEN 'S1314'",
                " WHEN insttnl_sctr = 'S14_A' THEN 'S14'",
                " ELSE insttnl_sctr",
                " END"
            )
            .prefix_all_except("orc_", "ra", "oa", "entty_id")
            .left_join(golden_record_t1, "ra", "entty_id")
            ;

            auto columns = get_orc_decla_t1().get_columns().intersect(get_golden_record_t1().get_columns());

            for (auto && column : columns)
            {
                T1.create_column(column, "CASE WHEN gr_", column, " IS NULL THEN orc_", column, " ELSE gr_", column, " END");
            }

            auto T1D = T1.copy().prefix_all_except("deb_", "ra", "oa");
            auto T1C = T1.copy().prefix_all_except("cre_", "ra", "oa");

            auto T9_T10 = T9.full_join(T10, "ref_date", "ra", "oa", "entty_id");

            auto T9_T10D = T9_T10.copy().prefix_all_except("deb_", "ref_date", "ra", "oa");
            auto T9_T10C = T9_T10.copy().prefix_all_except("cre_", "ref_date", "ra", "oa");

            auto creditors = T4C
            .left_join(T1C, "ra", "oa", "cre_entty_id")
            .left_join(T9_T10C, "ref_date", "ra", "oa", "cre_entty_id")
            ;

            auto debtors = T4D
            .left_join(T1D, "ra", "oa", "deb_entty_id")
            .left_join(T9_T10D, "ref_date", "ra", "oa", "deb_entty_id")
            ;

            auto table = instruments
            .left_join(creditors, "ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
            .left_join(debtors, "ref_date", "ra", "oa", "cntrct_id", "instrmnt_id");
            
            table.create_column("obsrvd_agnt_country", "'FR'");

            compute_common_variables(table);

            table
            .create_column("deb_insttnl_sctr_s", simplify_institutional_sector("deb_insttnl_sctr"))
            .create_column("cre_insttnl_sctr_s", simplify_institutional_sector("cre_insttnl_sctr"))
            .create_column("deb_addr_cntry_s", simplify_country("deb_addr_cntry"))
            .create_column("cre_addr_cntry_s", simplify_country("cre_addr_cntry"))
            .create_column("deb_entrprse_sze_s", simplify_enterprise_size("deb_entrprse_sze"))
            .create_column("deb_ecnmc_actvty_s", "SUBSTR(deb_ecnmc_actvty, 1, 2)")
            .create_column("deb_ecnmc_actvty_s", simplify_economic_activity("deb_ecnmc_actvty_s"))
            .create_column("crrncy_dnmntn_s", simplify_currency())
            .create_column("prfrmng_stts_s", simplify_non_performing_loans())
            ;

            compute_pge(table, "orc_decla", ref_date, ra, oa);

            adjust_interest_rate(table);

            table
            .aggregate
            ({
                Variable("encours", "SUM(encours)"),
                Variable(
                    "taux",
                    "CASE",
                    " WHEN ROUND(SUM(ponderation_du_taux), 5) = 0",
                    " THEN NULL",
                    " ELSE SUM(taux * ponderation_du_taux) / SUM(ponderation_du_taux)",
                    " END"
                ),
                Variable("ponderation_du_taux", "SUM(ponderation_du_taux)")
            },
            By(
                "ref_date", "ra", "oa",
                // "ticket_id_t3",
                "bsi_instrument", "mir_instrument",
                // "is_syndicated_duplicate", "intra_caisses",
                "new_business", "crrncy_dnmntn_s", "amount_category",
                "typ_instrmnt", "prps",
                "prfrmng_stts_s",
                "maturity",
                "deb_insttnl_sctr_s", "deb_addr_cntry_s", "deb_entrprse_sze_s", "deb_ecnmc_actvty_s",
                "cre_insttnl_sctr_s", "cre_addr_cntry_s",
                "pge",
                "settled_flag",
                "interest_rate_divided_by_100"
                // "siren_du_groupe"
            ));

            print_table("TABLE", table);

            auto end = std::chrono::steady_clock::now();

            std::cout << "Finished in " << (std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() / 1000.0) << " s !" << std::endl;
        
        }
        catch(...)
        {

        }
    }
}

#endif // EXAMPLES_ORC_DECLA_H