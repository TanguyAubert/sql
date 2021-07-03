#ifndef EXAMPLES_ENVOI_BCE_H
#define EXAMPLES_ENVOI_BCE_H

#include "examples-communs.h"
#include "examples-tables.h"

namespace SQL
{
    void compute_pge_envoi_bce(Table & table, const std::string & ref_date, const std::string & ra, const std::string & oa)
    {
        auto T7 = get_envoi_bce_t7();
        auto T8 = get_envoi_bce_t8();

        apply_filters(T7, ref_date, ra, oa);
        apply_filters(T8, ref_date, ra, oa);

        auto T7_pge = T7.copy().select("ref_date", "ra", "oa", "prtctn_id", "prtctn_prvdr_cd");
        auto T8_pge = T8.copy().select("ref_date", "ra", "oa", "prtctn_id", "cntrct_id", "instrmnt_id");

        auto pge = T8_pge
        .left_join(T7_pge, "ref_date", "ra", "oa", "prtctn_id")
        .filter("prtctn_prvdr_cd = 'FR130019763'")
        .select("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
        .unique()
        .set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
        .create_column("pge", 1)
        ;

        table.left_join(pge, "ref_date", "ra", "oa", "cntrct_id", "instrmnt_id");
        table.create_column("pge", "CASE WHEN pge = 1 THEN 1 ELSE 0 END");
    }

    void example_3()
    {
        try
        {
            auto begin = std::chrono::steady_clock::now();

            std::string ref_date = "2020-11";
            std::string ra = "30007";
            std::string oa = "30007";

            std::vector<std::string> keys = {"ref_date", "ra", "oa", "cntrct_id", "instrmnt_id"};

            auto T2 = get_envoi_bce_t2();
            auto T3 = get_envoi_bce_t3();
            auto T6 = get_envoi_bce_t6();
            auto T4C = get_envoi_bce_t4();
            auto T4D = get_envoi_bce_t4();
            auto T5 = get_envoi_bce_t5();
            auto T1 = get_golden_record_t1();
            auto T9 = get_envoi_bce_t9();
            auto T10 = get_envoi_bce_t10();

            apply_filters(T2, ref_date, ra, oa);
            apply_filters(T3, ref_date, ra, oa);
            apply_filters(T6, "", ra, oa);
            apply_filters(T4C, ref_date, ra, oa);
            apply_filters(T4D, ref_date, ra, oa);
            apply_filters(T5, ref_date, ra, oa);
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
            .rename("code_riad", "cre_code_riad")
            .create_column("number_of_creditors", Utils::count(), Utils::over_partition_by(keys))
            ;

            T5
            .rename("jnt_lblty_amnt", "deb_jnt_lblty_amnt")
            .rename("entty_id", "deb_entty_id")
            .rename("code_riad", "deb_code_riad")
            ;

            T4D
            .drop_key("entty_rl", 2)
            .rename("entty_id", "deb_entty_id")
            .rename("code_riad", "deb_code_riad")
            .left_join(T5, keys, "deb_entty_id", "deb_code_riad")
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

            // ----------------------------------- //

            auto T1D = T1.copy().prefix_all_except("deb_");
            auto T1C = T1.copy().prefix_all_except("cre_");

            auto T9_T10 = T9.full_join(T10, "ref_date", "ra", "oa", "entty_id", "code_riad");

            auto T9_T10D = T9_T10.copy().prefix_all_except("deb_", "ref_date", "ra", "oa");
            auto T9_T10C = T9_T10.copy().prefix_all_except("cre_", "ref_date", "ra", "oa");
            
            auto creditors = T4C
            .left_join(T1C, "cre_code_riad")
            .left_join(T9_T10C, "ref_date", "ra", "oa", "cre_entty_id", "cre_code_riad")
            ;

            auto debtors = T4D
            .left_join(T1D, "deb_code_riad")
            .left_join(T9_T10D, "ref_date", "ra", "oa", "deb_entty_id", "deb_code_riad")
            ;

            // ************************************ //

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

            // ----------------------------------- //

            compute_pge(table, "envoi_bce", ref_date, ra, oa);

            // ************************************ //

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
                "bsi_instrument", "mir_instrument",
                "new_business", "crrncy_dnmntn_s", "amount_category",
                "typ_instrmnt", "prps",
                "prfrmng_stts_s",
                "maturity",
                "deb_insttnl_sctr_s", "deb_addr_cntry_s", "deb_entrprse_sze_s", "deb_ecnmc_actvty_s",
                "cre_insttnl_sctr_s", "cre_addr_cntry_s",
                "pge"
            ));

            // ----------------------------------- //

            //table.select("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id", "cre_entty_id", "deb_entty_id", "type_de_taux");

            print_table("TABLE", table);

            auto end = std::chrono::steady_clock::now();

            std::cout << "Finished in " << (std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() / 1000.0) << " s !" << std::endl;
        }
        catch(...)
        {

        }
    }
}

#endif // EXAMPLES_ENVOI_BCE_H