#ifndef EXAMPLES_SECURE_DESKTOP_H
#define EXAMPLES_SECURE_DESKTOP_H

#include "examples-communs.h"
#include "examples-tables.h"

namespace SQL
{
    std::string get_obsrvd_agnt_acr_id_from_oa(const std::string & dt_rfrnc, const std::string & oa)
    {
        // TODO : Exécuter la requête ci-dessous et renvoyer la valeur du champ entty_id
        /*
        auto table = get_d_entty_by_dt_rfrnc()
        .filter("dt_rfrnc = '", dt_rfrnc, "'")
        .filter("entty_riad_cd = '", oa, "'")
        .select("entty_id")
        ;
        */
        
        return "4858";
    }

    void filter_if_not_empty(Table & table, const std::string & name, const std::string & value)
    {
        if (!value.empty())
        {
            table.filter(name, " = '", value, "'");
        }
    }

    void filter_dt_rfrnc(Table & table, const std::string & dt_rfrnc)
    {
        filter_if_not_empty(table, "dt_rfrnc", dt_rfrnc);
    }

    void filter_obsrvd_agnt_acr_id(Table & table, const std::string & obsrvd_agnt_acr_id)
    {
        filter_if_not_empty(table, "obsrvd_agnt_acr_id", obsrvd_agnt_acr_id);
    }

    Table create_t2(const std::string & dt_rfrnc, const std::string & obsrvd_agnt_acr_id)
    {
        auto table = get_v_instrmnt_by_snpsht();

        filter_dt_rfrnc(table, dt_rfrnc);
        filter_obsrvd_agnt_acr_id(table, obsrvd_agnt_acr_id);

        return table;
    }

    Table create_t3(const std::string & dt_rfrnc, const std::string & obsrvd_agnt_acr_id)
    {
        auto table = get_v_fnncl_by_snpsht();

        filter_dt_rfrnc(table, dt_rfrnc);
        filter_obsrvd_agnt_acr_id(table, obsrvd_agnt_acr_id);

        return table;
    }

    Table create_t6(const std::string & ref_date, const std::string & obsrvd_agnt_acr_id)
    {
        auto table = get_v_accntng_by_snpsht();

        auto dt_rfrnc = get_previous_quarter(ref_date);

        Utils::replace_all(dt_rfrnc, "-", "");

        table.drop_key("dt_rfrnc", dt_rfrnc);

        filter_obsrvd_agnt_acr_id(table, obsrvd_agnt_acr_id);

        return table;
    }

    Table create_t4c(const std::string & dt_rfrnc, const std::string & obsrvd_agnt_acr_id)
    {
        auto table = get_v_entty_instrmnt_by_snpsht();

        filter_dt_rfrnc(table, dt_rfrnc);
        filter_obsrvd_agnt_acr_id(table, obsrvd_agnt_acr_id);
        
        table
        .drop_key("entty_rl", 1)
        .drop("jnt_lblty_amnt")
        .rename("entty_id", "cre_entty_id")
        .create_column("number_of_creditors", "COUNT(*) OVER (PARTITION BY dt_rfrnc, obsrvd_agnt_acr_id, cntrct_id, instrmnt_id)")
        ;

        return table;
    }

    Table create_t4d(const std::string & dt_rfrnc, const std::string & obsrvd_agnt_acr_id)
    {
        auto table = get_v_entty_instrmnt_by_snpsht();

        filter_dt_rfrnc(table, dt_rfrnc);
        filter_obsrvd_agnt_acr_id(table, obsrvd_agnt_acr_id);
        
        table
        .drop_key("entty_rl", 2)
        .rename("entty_id", "deb_entty_id")
        .rename("jnt_lblty_amnt", "deb_jnt_lblty_amnt")
        .create_column("number_of_debtors", "COUNT(*) OVER (PARTITION BY dt_rfrnc, obsrvd_agnt_acr_id, cntrct_id, instrmnt_id)")
        .create_column(
            "total_deb_jnt_lblty_amnt",
            "SUM"
            "("
                "CASE"
                " WHEN deb_jnt_lblty_amnt IS NULL"
                " THEN 0"
                " ELSE deb_jnt_lblty_amnt"
                " END"
            ") OVER (PARTITION BY dt_rfrnc, obsrvd_agnt_acr_id, cntrct_id, instrmnt_id)")
        .create_column("main_debtor", "CASE WHEN NVL(deb_jnt_lblty_amnt,0) = MAX(NVL(deb_jnt_lblty_amnt,0)) OVER (PARTITION BY dt_rfrnc, obsrvd_agnt_acr_id, cntrct_id, instrmnt_id) THEN 1 ELSE 0 END")
        .create_column("number_of_main_debtors", "SUM(main_debtor) OVER (PARTITION BY dt_rfrnc, obsrvd_agnt_acr_id, cntrct_id, instrmnt_id)")
        ;

        return table;
    }

    Table create_t1(const std::string & dt_rfrnc)
    {
        auto table = get_d_entty_by_dt_rfrnc();

        filter_dt_rfrnc(table, dt_rfrnc);

        return table;
    }

    Table create_t9_t10(const std::string & dt_rfrnc, const std::string & obsrvd_agnt_acr_id)
    {
        auto table = get_v_entty_rsk_by_snpsht();

        filter_dt_rfrnc(table, dt_rfrnc);
        filter_obsrvd_agnt_acr_id(table, obsrvd_agnt_acr_id);

        return table;
    }

    void compute_observed_agent_country_and_riad_code(Table & table, const std::string & dt_rfrnc, const std::string & obsrvd_agnt_acr_id)
    {
        auto code_riad = get_d_entty_by_dt_rfrnc();

        filter_dt_rfrnc(code_riad, dt_rfrnc);
        
        code_riad
        .filter("entty_riad_cd IS NOT NULL")
        .select("entty_id", "entty_riad_cd")
        .create_column("obsrvd_agnt_country", "SUBSTR(entty_riad_cd, 1, 2)")
        .rename("entty_id", "obsrvd_agnt_acr_id")
        .rename("entty_riad_cd", "oa")
        .unique()
        .set_keys("obsrvd_agnt_acr_id")
        ;

        table.left_join(code_riad, "obsrvd_agnt_acr_id");
    }

    void example_4()
    {
        try
        {
            auto begin = std::chrono::steady_clock::now();

            Options::set_sgbd(Options::SGBD::ORACLE);

            std::string ref_date = "2020-11";
            std::string ra = "30007";
            std::string oa = "30007";

            std::string dt_rfrnc = ref_date;

            Utils::replace_all(dt_rfrnc, "-", "");

            std::string obsrvd_agnt_acr_id;

            if (oa.size() == 5)
            {
                ra = std::string("FR") + ra;
            }

            if (!oa.empty())
            {
                if (oa.size() == 5)
                {
                    oa = std::string("FR") + oa;
                }

                obsrvd_agnt_acr_id = get_obsrvd_agnt_acr_id_from_oa(dt_rfrnc, oa);
            }

            auto T2 = create_t2(dt_rfrnc, obsrvd_agnt_acr_id);
            auto T3 = create_t3(dt_rfrnc, obsrvd_agnt_acr_id);
            auto T6 = create_t6(ref_date, obsrvd_agnt_acr_id);
            auto T4C = create_t4c(dt_rfrnc, obsrvd_agnt_acr_id);
            auto T4D = create_t4d(dt_rfrnc, obsrvd_agnt_acr_id);
            auto T1 = create_t1(dt_rfrnc);
            auto T9_T10 = create_t9_t10(dt_rfrnc, obsrvd_agnt_acr_id);

            auto instruments = T3
            .left_join(T2, "dt_rfrnc", "obsrvd_agnt_acr_id", "cntrct_id", "instrmnt_id")
            .left_join(T6, "obsrvd_agnt_acr_id", "cntrct_id", "instrmnt_id")
            ;

            auto T1D = T1.copy().prefix_all_except("deb_", "dt_rfrnc");
            auto T1C = T1.copy().prefix_all_except("cre_", "dt_rfrnc");

            auto T9_T10D = T9_T10.copy().prefix_all_except("deb_", "dt_rfrnc", "obsrvd_agnt_acr_id");
            auto T9_T10C = T9_T10.copy().prefix_all_except("cre_", "dt_rfrnc", "obsrvd_agnt_acr_id");
            
            auto creditors = T4C
            .left_join(T1C, "dt_rfrnc", "cre_entty_id")
            .left_join(T9_T10C, "dt_rfrnc", "obsrvd_agnt_acr_id", "cre_entty_id")
            ;

            auto debtors = T4D
            .left_join(T1D, "dt_rfrnc", "deb_entty_id")
            .left_join(T9_T10D, "dt_rfrnc", "obsrvd_agnt_acr_id", "deb_entty_id")
            ;

            auto table = instruments
            .left_join(creditors, "dt_rfrnc", "obsrvd_agnt_acr_id", "cntrct_id", "instrmnt_id")
            .left_join(debtors, "dt_rfrnc", "obsrvd_agnt_acr_id", "cntrct_id", "instrmnt_id");

            compute_observed_agent_country_and_riad_code(table, dt_rfrnc, obsrvd_agnt_acr_id);

            auto v_oa_by_dt_rfrnc_typ_rprtng = get_v_oa_by_dt_rfrnc_typ_rprtng();

            filter_dt_rfrnc(v_oa_by_dt_rfrnc_typ_rprtng, dt_rfrnc);

            v_oa_by_dt_rfrnc_typ_rprtng
            .select("obsrvd_agnt_acr_id", "ra_cd")
            .rename("ra_cd", "ra")
            .unique()
            .set_keys("obsrvd_agnt_acr_id")
            ;

            table.left_join(v_oa_by_dt_rfrnc_typ_rprtng, "obsrvd_agnt_acr_id");

            filter_if_not_empty(table, "ra", ra);

            table.create_column("ref_date", Utils::single_quote(ref_date));

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

            //compute_pge(table, "envoi_bce", ref_date, ra, oa);

            //adjust_interest_rate(table);
            
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
                "ref_date", 
                "ra",
                "oa",
                "obsrvd_agnt_country",
                "bsi_instrument", "mir_instrument",
                "new_business", "crrncy_dnmntn_s", "amount_category",
                "typ_instrmnt", "prps",
                "prfrmng_stts_s",
                "maturity",
                "deb_insttnl_sctr_s", "deb_addr_cntry_s", "deb_entrprse_sze_s", "deb_ecnmc_actvty_s",
                "cre_insttnl_sctr_s", "cre_addr_cntry_s"
                //"pge"
            ));

            print_table("TABLE", table);

            auto end = std::chrono::steady_clock::now();

            std::cout << "Finished in " << (std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() / 1000.0) << " s !" << std::endl;
        
            Options::set_sgbd(Options::SGBD::HIVE);
        }
        catch(...)
        {

        }
    }
}

#endif // EXAMPLES_SECURE_DESKTOP_H