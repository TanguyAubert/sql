#ifndef EXAMPLES_COMMUNS_H
#define EXAMPLES_COMMUNS_H

#include "table.h"
#include <chrono>

namespace SQL
{
    void print_table(const std::string & name, const Table & table)
    {
        std::cout << "-----------------" << std::endl;
        std::cout << std::endl;
        std::cout << name << std::endl;
        std::cout << std::endl;

        // table.print();

        auto text = table.render();

        std::cout << text << std::endl;
        std::cout << std::endl;
        
        if (table.has_errors())
        {
            table.display_errors();
        }
        std::cout << std::endl;

        std::cout << "KEYS : " << table.get_keys().collapse(", ") << std::endl;
        std::cout << std::endl;

        std::cout << "NUMBER OF CHARACTERS : " << text.size() << std::endl;
        std::cout << std::endl;
    }
    
    void apply_filters(Table & table, const std::string & ref_date, const std::string & ra, const std::string & oa)
    {
        if (table.has_column("ref_date") && !ref_date.empty())
        {
            table.filter("ref_date = '", ref_date, "'");
        }

        if (table.has_column("ra") && !ra.empty())
        {
            table.filter("ra = '", ra, "'");
        }
        
        if (table.has_column("oa") && !oa.empty())
        {
            table.filter("oa = '", oa, "'");
        }
    }

    std::string get_previous_quarter(const std::string & ref_date)
    {
        auto year = Utils::to_int(ref_date.substr(0, 4));
        auto month = Utils::to_int(ref_date.substr(5, 2));

        if (month % 3 == 0)
        {
            return ref_date;
        }
        else
        {
            month -= month % 3;

            if (month == 0)
            {
                month = 12;
                --year;
            }

            return Utils::to_string(year) + (month < 10 ? "-0" : "-") + Utils::to_string(month); 
        }
    }

    void compute_partially_transferred_flag(Table & table)
    {
        table.create_column(
            "partially_transferred_flag",
            "CASE WHEN otstndng_nmnl_amnt > trnsfrrd_amnt"
            " AND trnsfrrd_amnt > 0"
            " THEN 1"
            " ELSE 0 END"
        );
    }
    
    void compute_partial_transfer_applicable(Table & table)
    {
        table.create_column(
            "partial_transfer_applicable",
            "CASE WHEN obsrvd_agnt_country = 'ES'"
            " THEN 0"
            " ELSE 1 END"
        );
    }
    
    void compute_impairment_flag(Table & table)
    {
        table.create_column(
            "impairment_flag",
            "CASE",
            " WHEN obsrvd_agnt_country = 'FI' AND accmltd_imprmnt IS NOT NULL THEN 1",
            " WHEN obsrvd_agnt_country = 'DE'"
            " AND accmltd_imprmnt IS NOT NULL"
            " AND imprmnt_assssmnt_mthd = 2 THEN 1",
            " ELSE 0 END"
        );
    }
    
    void compute_fair_value_flag(Table & table)
    {
        table.create_column(
            "fair_value_flag", 
            "CASE",
            " WHEN obsrvd_agnt_country IN ('DE','FI')",
            " AND accmltd_chngs_fv_cr IS NOT NULL",
            " THEN 1",
            " ELSE 0 END");
    }
    
    void compute_acquisition_price_flag(Table & table)
    {
        table.create_column(
            "acquisition_price_flag", 
            "CASE",
            " WHEN obsrvd_agnt_country IN ('AT', 'BE', 'DE', 'IT', 'SI')",
            " AND fv_chng_cr_bfr_prchs IS NOT NULL",
            " THEN 1",
            " ELSE 0 END"
        );
    }
    
    void compute_bsi_instrument_balance(Table & table)
    {
        table.create_column(
            "bsi_instrument_balance",
            "ABS("
            "NVL(otstndng_nmnl_amnt, 0)",
            " - NVL(trnsfrrd_amnt, 0) * partially_transferred_flag * partial_transfer_applicable",
            " - NVL(accmltd_imprmnt, 0) * impairment_flag",
            " - NVL(accmltd_chngs_fv_cr, 0) * fair_value_flag",
            " - NVL(fv_chng_cr_bfr_prchs, 0) * acquisition_price_flag"
            ")"
        );
    }
    
    void compute_pro_rata_debtor_share(Table & table)
    {
        table.create_column(
            "pro_rata_debtor_share",
            "CASE",
            " WHEN number_of_debtors IS NULL THEN 1",
            " WHEN number_of_debtors = 1 THEN 1",
            " WHEN number_of_debtors > 1 AND ROUND(total_deb_jnt_lblty_amnt, 5) = 0 THEN 1 / number_of_debtors",
            " WHEN number_of_debtors > 1 AND ROUND(total_deb_jnt_lblty_amnt, 5) <> 0 THEN NVL(deb_jnt_lblty_amnt, 0) / total_deb_jnt_lblty_amnt",
            " ELSE 0",
            " END"
        );
    }
    
    void compute_main_debtor_share(Table & table)
    {
        table.create_column(
            "main_debtor_share",
            "CASE",
            " WHEN number_of_debtors = 1 THEN 1",
            " WHEN main_debtor = 1 THEN 1 / number_of_main_debtors",
            " ELSE 0",
            " END"
        );
    }
    
    void compute_creditor_share(Table & table)
    {
        table.create_column(
            "creditor_share",
            "CASE",
            " WHEN number_of_creditors IS NULL THEN 1",
            " WHEN number_of_creditors = 1 THEN 1",
            " WHEN number_of_creditors > 1 THEN 1 / number_of_creditors",
            " ELSE 0",
            " END"
        );
    }
    
    void compute_aggregable_amount(Table & table)
    {
        table.create_column(
            "encours",
            "CASE",
            " WHEN obsrvd_agnt_country IN ('AT', 'EE', 'FR', 'GR', 'IE', 'LT', 'LU', 'MT', 'NL', 'PT', 'SI', 'SK')",
            " THEN bsi_instrument_balance * creditor_share * pro_rata_debtor_share",
            " ELSE bsi_instrument_balance * creditor_share * main_debtor_share",
            " END"
        );
    }
    
    void compute_interest_rate_type(Table & table)
    {
        table.create_column(
            "type_de_taux",
            "CASE",
            " WHEN ABS(annlsd_agrd_rt) < 1e-6 THEN 'ZERO'",
            " WHEN annlsd_agrd_rt IS NULL THEN 'NULL'",
            " WHEN ABS(annlsd_agrd_rt) > 99 THEN 'DUMMY'",
            " ELSE 'OK'",
            " END"
        );
    }
    
    void compute_interest_rate(Table & table)
    {
        table.create_column(
            "taux",
            "CASE"
            " WHEN type_de_taux = 'OK'"
            " THEN annlsd_agrd_rt"
            " ELSE NULL END"
        );
    }
    
    void compute_interest_rate_weight(Table & table)
    {
        table.create_column(
            "ponderation_du_taux",
            "CASE WHEN type_de_taux = 'OK'"
            " THEN encours"
            " ELSE NULL END"
        );
    }

    void compute_non_fiduciary_flag(Table & table)
    {
        table.create_column(
            "non_fiduciary_flag",
            "CASE",
            " WHEN fdcry = '1'",
            " THEN 0",
            " ELSE 1 END"
        );
    }
    
    void compute_recognition_flag(Table & table)
    {
        table.create_column(
            "recognition_flag",
            "CASE"
            " WHEN rcgntn_stts = 3"
            " THEN 0"
            " ELSE 1 END"
        );
    }
    
    void compute_intra_company_instrument_flag(Table & table)
    {
        table.create_column(
            "intra_company_instrument_flag",
            "CASE"
            " WHEN cre_entty_id <> deb_entty_id"
            " THEN 0"
            " ELSE 1 END"
        );
    }
    
    void compute_non_tradi_securitised_flag(Table & table)
    {
        table.create_column(
            "non_tradi_securitised_flag",
            "CASE WHEN typ_scrtstn = 1"
            " AND obsrvd_agnt_country = 'IE'"
            " THEN 0"
            " ELSE 1 END"
        );
    }
    
    void compute_settled_flag(Table & table)
    {
        // Ajuster la date pour tenir compte du nombre de caractères
        table.create_column(
            "settled_flag",
            "CASE"
            " WHEN SUBSTR(dt_sttlmnt, 1, 7) <= ref_date"
            " THEN 1"
            " ELSE 0 END"
        );
    }
    
    void compute_bsi_instrument(Table & table)
    {
        table
        .create_column(
            "bsi_instrument",
            "CASE",
            " WHEN non_fiduciary_flag = 1",
            " AND non_tradi_securitised_flag = 1",
            " AND settled_flag = 1",
            " AND (recognition_flag = 1 OR intra_company_instrument_flag = 1)",
            " THEN 1 ELSE 0 END"
        );
    }

    void compute_bad_loan_flag(Table & table)
    {
        table
        .create_column(
            "bad_loan_flag",
            "CASE WHEN dflt_stts IS NOT NULL THEN",
            "    (CASE",
            "      WHEN dflt_stts IN ('18', '19', '20') THEN 1",
            "      WHEN dflt_stts <> '14' AND deb_dflt_stts IN ('18', '19', '20') THEN 1",
            "      ELSE 0",
            "    END)",
            "  ELSE ",
            "    (CASE",
            "      WHEN frbrnc_stts = '4' OR prfrmng_stts = '1' OR imprmnt_stts = '25' THEN 1",
            "      ELSE 0",
            "    END)",
            "  END"
        );
    }

    void compute_mir_instrument(Table & table)
    {
        table.create_column(
            "mir_instrument",
            "CASE",
            " WHEN non_fiduciary_flag = 1",
            " AND bad_loan_flag = 0",
            " AND non_tradi_securitised_flag = 1",
            " AND (recognition_flag = 1",
            "      OR intra_company_instrument_flag = 1)",
            " THEN 1 ELSE 0 END"
        );
    }

    std::string compute_number_of_months_between(const std::string & date_1, const std::string & date_2)
    {
        if (Options::get_sgbd() == Options::SGBD::ORACLE)
        {
            return Utils::concatenate("MONTHS_BETWEEN(TO_DATE(", date_1, ", 'YYYYMMDD'), TO_DATE(", date_2, ", 'YYYYMMDD'))");
        }
        else
        {
            return Utils::concatenate("CAST(MONTHS_BETWEEN(TO_DATE(", date_1, "), TO_DATE(", date_2, ")) AS INT)");
        }
    }

    void compute_maturity_in_months(Table & table)
    {
        table.create_column("maturity_in_months", compute_number_of_months_between("dt_lgl_fnl_mtrty", "dt_sttlmnt"));
    }

    void compute_maturity(Table & table)
    {
        table.create_column(
            "maturity",
            "CASE WHEN dt_lgl_fnl_mtrty IS NULL THEN",
            "   (CASE",
            "   WHEN",
            "     typ_instrmnt IN ('20', '51', '1001')",
            "     OR (typ_instrmnt = '71' AND obsrvd_agnt_country IN ('FR', 'IT', 'AT'))",
            "     OR (typ_instrmnt = '1003' AND obsrvd_agnt_country IN ('AT'))",
            "     OR rpymnt_rghts = '1' THEN '(0 : 3M]'",
            "   ELSE 'Inconnue'",
            "   END)",
            " ELSE",
            "   (CASE",
            "   WHEN maturity_in_months <= 0 THEN '(-Inf : 0]'",
            "   WHEN maturity_in_months <= 3 THEN '(0 : 3M]'",
            "   WHEN maturity_in_months <= 12 THEN '(3M : 1Y]'",
            "   WHEN maturity_in_months <= 36 THEN '(1Y : 3Y]'",
            "   WHEN maturity_in_months <= 60 THEN '(3Y : 5Y]'",
            "   WHEN maturity_in_months <= 120 THEN '(5Y : 10Y]'",
            "   WHEN maturity_in_months > 120 THEN '(10Y : +Inf)'",
            "   ELSE 'Inconnue'",
            "   END)",
            " END"
        );
    }

    void compute_new_business(Table & table)
    {
        // Ajuster la date pour tenir compte du nombre de caractères
        table.create_column(
            "new_business",
            "CASE",
            " WHEN SUBSTR(dt_incptn, 1, 7) = ref_date",
            " THEN 1",
            " ELSE 0 END"
        );
    }
    
    void compute_amount_category(Table & table)
    {
        table.create_column(
            "amount_category", 
            "CASE"
            " WHEN otstndng_nmnl_amnt <= 25000 THEN '(-Inf : 25000]'"
            " WHEN otstndng_nmnl_amnt <= 50000 THEN '(25000 : 50000]'"
            " WHEN otstndng_nmnl_amnt <= 100000 THEN '(50000 : 100000]'"
            " WHEN otstndng_nmnl_amnt <= 250000 THEN '(100000 : 250000]'"
            " WHEN otstndng_nmnl_amnt <= 1000000 THEN '(250000 : 1000000]'"
            " WHEN otstndng_nmnl_amnt > 1000000 THEN '(1000000 : +Inf)'"
            " ELSE 'Inconnue' END"
        );
    }

    void compute_common_variables(Table & table)
    {
        compute_partially_transferred_flag(table);
        compute_partial_transfer_applicable(table);
        compute_impairment_flag(table);
        compute_fair_value_flag(table);
        compute_acquisition_price_flag(table);
        compute_pro_rata_debtor_share(table);
        compute_main_debtor_share(table);
        compute_creditor_share(table);
        compute_interest_rate_type(table);
        compute_non_fiduciary_flag(table);
        compute_recognition_flag(table);
        compute_intra_company_instrument_flag(table);
        compute_non_tradi_securitised_flag(table);
        compute_settled_flag(table);
        compute_bad_loan_flag(table);
        compute_maturity_in_months(table);
        compute_new_business(table);
        compute_amount_category(table);

        compute_bsi_instrument_balance(table);
        compute_interest_rate(table);
        compute_bsi_instrument(table);
        compute_mir_instrument(table);
        compute_maturity(table);
        
        compute_aggregable_amount(table);
        
        compute_interest_rate_weight(table);
    }

    void compute_pge_orc_decla(Table & table, const std::string & ref_date, const std::string & ra, const std::string & oa);
    void compute_pge_envoi_bce(Table & table, const std::string & ref_date, const std::string & ra, const std::string & oa);

    void compute_pge(Table & table, const std::string & method, const std::string & ref_date, const std::string & ra, const std::string & oa)
    {
        if (method == "envoi_bce") compute_pge_envoi_bce(table, ref_date, ra, oa);
        else                       compute_pge_orc_decla(table, ref_date, ra, oa);
    }

    void adjust_interest_rate(Table & table)
    {
        table
        .create_column("numerator", "SUM(ABS(taux) * ponderation_du_taux) OVER (PARTITION BY ref_date, ra, oa, ticket_id)")
        .create_column("denominator", "SUM(ponderation_du_taux) OVER (PARTITION BY ref_date, ra, oa, ticket_id)")
        .create_column(
            "criteria", 
            "CASE",
            " WHEN numerator IS NULL",
            " OR denominator IS NULL",
            " OR ROUND(denominator, 5) = 0",
            " THEN 0",
            " ELSE numerator / denominator END"
        )
        .create_column(
            "interest_rate_divided_by_100",
            "CASE WHEN criteria > 0.2",
            " THEN 1",
            " ELSE 0 END"
        )
        .create_column(
            "taux", 
            "CASE",
            " WHEN interest_rate_divided_by_100 = 1",
            " THEN taux / 100",
            " ELSE taux END"
        )
        .drop("ticket_id", "numerator", "denominator", "criteria")
        ;
    }

    std::string is_null(const std::string & variable)
    {
        return Utils::concatenate(variable, " IS NULL OR ", variable, " IN ('-4', '-99', 'DL_NULL', '', 'N-A') ");
    }

    std::string simplify_institutional_sector(const std::string & variable)
    {
        return Utils::concatenate(
            "CASE",
            " WHEN ", is_null(variable), " THEN 'Inconnu'",
            " WHEN ", variable, " = 'S11' THEN 'SNF'",
            " WHEN ", variable, " = 'S121' THEN 'ESCB'",
            " WHEN ", variable, " IN ('S122', 'S123') THEN 'IFM'",
            " WHEN ", variable, " IN ('S124', 'S125', 'S126', 'S127') THEN 'OFI'",
            " WHEN ", variable, " IN ('S128', 'S129') THEN 'Assurance / fond de pension'",
            " WHEN ", variable, " IN ('S1311', 'S1312', 'S1313', 'S1314') THEN 'APU'",
            " WHEN ", variable, " = 'S14' THEN 'EI'",
            " WHEN ", variable, " = 'S15' THEN 'ISBLSM'",
            " ELSE ", variable,
            " END"
        );
    }

    std::string simplify_country(const std::string & variable)
    {
        return Utils::concatenate(
            "CASE",
            " WHEN ", variable, " IN ('AT', 'BE', 'CY', 'DE', 'EE', 'ES', 'FI', 'FR', 'IE', 'IT', 'GR', 'LT', 'LU', 'LV', 'MT', 'NL', 'PT', 'SI', 'SK') THEN ", variable,
            " WHEN ", variable, " = 'MC' THEN 'FR'",
            " WHEN ", is_null(variable), " THEN 'Inconnu'",
            " ELSE 'Hors zone euro'",
            " END"
        );
    }

    std::string simplify_economic_activity(const std::string & variable)
    {
        // Revoir le code pour réduire le nombre de caractère (ne mettre qu'une seule inégalité)
        // Passer la variable originale et celle sur deux caractères en paramètre pour pouvoir utiliser is_null !

        return Utils::concatenate(
            "CASE",
            " WHEN ", variable, " >= '01' AND ", variable, " <= '03' THEN 'A'",
            " WHEN ", variable, " >= '05' AND ", variable, " <= '09' THEN 'B'",
            " WHEN ", variable, " >= '10' AND ", variable, " <= '33' THEN 'C'",
            " WHEN ", variable, " >= '35' AND ", variable, " <= '35' THEN 'D'",
            " WHEN ", variable, " >= '36' AND ", variable, " <= '39' THEN 'E'",
            " WHEN ", variable, " >= '41' AND ", variable, " <= '43' THEN 'F'",
            " WHEN ", variable, " >= '45' AND ", variable, " <= '47' THEN 'G'",
            " WHEN ", variable, " >= '49' AND ", variable, " <= '53' THEN 'H'",
            " WHEN ", variable, " >= '55' AND ", variable, " <= '56' THEN 'I'",
            " WHEN ", variable, " >= '58' AND ", variable, " <= '63' THEN 'J'",
            " WHEN ", variable, " >= '64' AND ", variable, " <= '66' THEN 'K'",
            " WHEN ", variable, " >= '68' AND ", variable, " <= '68' THEN 'L'",
            " WHEN ", variable, " >= '69' AND ", variable, " <= '75' THEN 'M'",
            " WHEN ", variable, " >= '77' AND ", variable, " <= '82' THEN 'N'",
            " WHEN ", variable, " >= '84' AND ", variable, " <= '84' THEN 'O'",
            " WHEN ", variable, " >= '85' AND ", variable, " <= '85' THEN 'P'",
            " WHEN ", variable, " >= '86' AND ", variable, " <= '88' THEN 'Q'",
            " WHEN ", variable, " >= '90' AND ", variable, " <= '93' THEN 'R'",
            " WHEN ", variable, " >= '94' AND ", variable, " <= '96' THEN 'S'",
            " WHEN ", variable, " >= '97' AND ", variable, " <= '98' THEN 'T'",
            " WHEN ", variable, " >= '99' AND ", variable, " <= '99' THEN 'U'",
            " WHEN ", variable, " IS NULL OR ", variable, " IN ('-4', '-9', '', 'N-', 'DL', '00') THEN 'Inconnue'",
            " ELSE ", variable,
            " END"
        );
    }

    std::string simplify_enterprise_size(const std::string & variable)
    {
        return Utils::concatenate(
            "CASE",
            " WHEN ", variable, " = 1 THEN 'GE / ETI'",
            " WHEN ", variable, " IN (2, 3) THEN 'PME hors TPE'",
            " WHEN ", variable, " = 4 THEN 'TPE'",
            " ELSE 'Inconnue'",
            " END"
        );
    }

    std::string simplify_currency()
    {
        return Utils::concatenate(
            "CASE",
            " WHEN crrncy_dnmntn = 'EUR' THEN 'EUR'",
            " WHEN ", is_null("crrncy_dnmntn"), " THEN 'Inconnue'",
            " ELSE 'Z06'",
            " END"
        );
    }

    std::string simplify_non_performing_loans()
    {
        return Utils::concatenate(
            "CASE",
            " WHEN prfrmng_stts = '1' THEN 'Non-performing'",
            " WHEN prfrmng_stts = '11' THEN 'Performing'",
            " ELSE 'Inconnu'",
            " END"
        );
    }
}

#endif // EXAMPLES_COMMUNS_H