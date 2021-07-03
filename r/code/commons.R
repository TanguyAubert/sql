
filter_if_not_empty <- function(table, name, value, quote = TRUE)
{
    value <- remove_null_values(value)
    
    if (table$has_column(name))
    {
        if (base::length(value) == 1)
        {
            if (quote) value <- stringr::str_c("'", value, "'")
            
            table$filter(name, " = ", value)
            
        } else if (base::length(value) > 1)
        {
            if (quote) value <- stringr::str_c("'", value, "'")
            
            value <- stringr::str_c(value, collapse = ", ")
            
            table$filter(name, " IN (", value, ")")
        }
    }
}

filter_ref_date_if_not_empty <- function(table, ref_date)
{
    filter_if_not_empty(table, "ref_date", ref_date)
}

filter_ra_if_not_empty <- function(table, ra)
{
    filter_if_not_empty(table, "ra", ra)
}

filter_oa_if_not_empty <- function(table, oa)
{
    filter_if_not_empty(table, "oa", oa)
}

filter_dt_rfrnc_if_not_empty <- function(table, dt_rfrnc)
{
    dt_rfrnc <- stringr::str_replace_all(dt_rfrnc, "-", "")
    
    filter_if_not_empty(table, "dt_rfrnc", dt_rfrnc, quote = FALSE)
}

filter_obsrvd_agnt_acr_id_if_not_empty <- function(table, obsrvd_agnt_acr_id)
{
    filter_if_not_empty(table, "obsrvd_agnt_acr_id", obsrvd_agnt_acr_id)
}

remove_null_values <- function(values)
{
    return (base::setdiff(values, c("", NA)))
}

cast_as_int <- function(column)
{
    return (stringr::str_c("CAST(", column, " AS INT)"))
}

cast_as_decimal <- function(column)
{
    return (stringr::str_c("CAST(", column, " AS DECIMAL(20, 8))"))
}

cast_as_string <- function(column)
{
    if (get_sgbd() == "ORACLE")
    {
        return (stringr::str_c("TO_CHAR(", column, ")"))
        
    } else
    {
        return (stringr::str_c("CAST(", column, " AS STRING)"))
    }
}

cast_all_as_decimal <- function(table, columns)
{
    for (column in columns)
    {
        table$create_column(column, cast_as_decimal(column))
    }
}

cast_all_as_string <- function(table, columns)
{
    for (column in columns)
    {
        table$create_column(column, cast_as_string(column))
    }
}

concatenate <- function(...)
{
    tokens <- base::list(...)
    tokens <- base::unlist(tokens)
    
    if (base::length(tokens) <= 0)
    {
        base::stop("concatenate must have at least one argument !")
    }
    
    if (get_sgbd() == "ORACLE")
    {
        if (base::length(tokens) == 1)
        {
            output <- stringr::str_c("CONCAT(", tokens, ")")
            
        } else if (base::length(tokens) == 2)
        {
            output <- stringr::str_c("CONCAT(", tokens[1], ", ", tokens[2], ")")
            
        } else
        {
            output <- stringr::str_c("CONCAT(", tokens[1], ", ", concatenate(tokens[2:base::length(tokens)]), ")")
        }
        
        return (output)
        
    } else
    {
        output <- stringr::str_c(tokens, collapse = ", ")
        output <- stringr::str_c("CONCAT(", output, ")")
        
        return (output)
    }
}

add_accounting_table <- function(instruments, accounting, ref_date)
{
    ref_date_quarter <- get_previous_quarter(ref_date)
    ref_date_quarter <- base::unique(ref_date_quarter)
    
    if (base::length(ref_date) == 1)
    {
        accounting$drop_key("ref_date", ref_date_quarter)
        
        instruments$left_join(accounting, "ra", "oa", "cntrct_id", "instrmnt_id")
        
    } else
    {
        filter_dt_rfrnc_if_not_empty(accounting, ref_date_quarter)
        
        accounting$rename("ref_date", "ref_date_t6")
        
        compute_ref_month(instruments)
        compute_ref_year(instruments)
        compute_previous_ref_year(instruments)
        
        instruments$
            create_column("ref_date_t6", compute_previous_reference_date("-", "ref_date"))$
            left_join(accounting, "ref_date_t6", "ra", "oa", "cntrct_id", "instrmnt_id")$
            drop("ref_month", "ref_year", "previous_ref_year", "ref_date_t6")
    }
}

get_previous_quarter <- function(ref_date)
{
    year <- base::as.numeric(stringr::str_sub(ref_date, 1, 4))
    month <- base::as.numeric(stringr::str_sub(ref_date, 6, 7))
    
    month <- month - month %% 3
    
    year[month == 0] <- year[month == 0] - 1
    month[month == 0] <- 12
    
    ref_date <- stringr::str_c(year, base::ifelse(month < 10, "-0", "-"), month)
    
    return (ref_date)
}

compute_ref_month <- function(table)
{
    if (get_sgbd() == "ORACLE")
    {
        table$create_column("ref_month", "SUBSTR(", cast_as_string("dt_rfrnc"), ", 5, 2)")
        
    } else
    {
        table$create_column("ref_month", "SUBSTR(ref_date, 6, 2)")
    }
}

compute_ref_year <- function(table)
{
    if (get_sgbd() == "ORACLE")
    {
        table$create_column("ref_year", "SUBSTR(", cast_as_string("dt_rfrnc"), ", 1, 4)")
        
    } else
    {
        table$create_column("ref_year", "SUBSTR(ref_date, 1, 4)")
    }
}

compute_previous_ref_year <- function(table)
{
    table$create_column("previous_ref_year", cast_as_string(stringr::str_c("(", cast_as_int("ref_year"), " - 1)")))
}

compute_previous_reference_date <- function(separator, default_reference_date)
{
    format_month <- function(number)
    {
        number <- stringr::str_pad(number, width = 2, side = "left", pad = "0")
        
        return (stringr::str_c("'", separator, number, "'"))
    }
    
    return (stringr::str_c(
        "CASE",
        " WHEN ref_month IN ('03', '04', '05') THEN ", concatenate("ref_year", format_month(3)),
        " WHEN ref_month IN ('06', '07', '08') THEN ", concatenate("ref_year", format_month(6)),
        " WHEN ref_month IN ('09', '10', '11') THEN ", concatenate("ref_year", format_month(9)),
        " WHEN ref_month IN ('01', '02') THEN ", concatenate("previous_ref_year", format_month(12)),
        " ELSE ", default_reference_date,
        " END"
    ))
}

compute_number_of_creditors <- function(table, variables)
{
    variables <- stringr::str_c(variables, collapse = ", ")
    
    table$create_column("number_of_creditors", "COUNT(*) OVER (PARTITION BY ", variables, ")")
}

compute_number_of_debtors <- function(table, variables)
{
    variables <- stringr::str_c(variables, collapse = ", ")
    
    table$create_column("number_of_debtors", "COUNT(*) OVER (PARTITION BY ", variables, ")")
}

compute_total_deb_jnt_lblty_amnt <- function(table, variables)
{
    variables <- stringr::str_c(variables, collapse = ", ")
    
    table$create_column(
        "total_deb_jnt_lblty_amnt",
        "SUM",
        "(",
        "CASE",
        " WHEN deb_jnt_lblty_amnt IS NULL",
        " THEN 0",
        " ELSE deb_jnt_lblty_amnt",
        " END",
        ") OVER (PARTITION BY ", variables, ")"
    )
}

compute_main_debtor <- function(table, variables)
{
    variables <- stringr::str_c(variables, collapse = ", ")
    
    table$create_column(
        "main_debtor", 
        "CASE",
        " WHEN NVL(deb_jnt_lblty_amnt,0) = MAX(NVL(deb_jnt_lblty_amnt,0))",
        " OVER (PARTITION BY ", variables, ")",
        " THEN 1",
        " ELSE 0 END"
    )
}

compute_number_of_main_debtors <- function(table, variables)
{
    variables <- stringr::str_c(variables, collapse = ", ")
    
    table$create_column(
        "number_of_main_debtors",
        "SUM(main_debtor)",
        " OVER (PARTITION BY ", variables, ")")
}

compute_partially_transferred_flag <- function(table)
{
    table$create_column(
        "partially_transferred_flag",
        "CASE WHEN otstndng_nmnl_amnt > trnsfrrd_amnt",
        " AND trnsfrrd_amnt > 0",
        " THEN 1",
        " ELSE 0 END"
    )
}

compute_partial_transfer_applicable <- function(table)
{
    table$create_column(
        "partial_transfer_applicable",
        "CASE WHEN obsrvd_agnt_country = 'ES'",
        " THEN 0",
        " ELSE 1 END"
    )
}

compute_impairment_flag <- function(table)
{
    table$create_column(
        "impairment_flag",
        "CASE",
        " WHEN obsrvd_agnt_country = 'FI' AND accmltd_imprmnt IS NOT NULL THEN 1",
        " WHEN obsrvd_agnt_country = 'DE'",
        " AND accmltd_imprmnt IS NOT NULL",
        " AND imprmnt_assssmnt_mthd = 2 THEN 1",
        " ELSE 0 END"
    )
}

compute_fair_value_flag <- function(table)
{
    table$create_column(
        "fair_value_flag", 
        "CASE",
        " WHEN obsrvd_agnt_country IN ('DE','FI')",
        " AND accmltd_chngs_fv_cr IS NOT NULL",
        " THEN 1",
        " ELSE 0 END")
}

compute_acquisition_price_flag <- function(table)
{
    table$create_column(
        "acquisition_price_flag", 
        "CASE",
        " WHEN obsrvd_agnt_country IN ('AT', 'BE', 'DE', 'IT', 'SI')",
        " AND fv_chng_cr_bfr_prchs IS NOT NULL",
        " THEN 1",
        " ELSE 0 END"
    )
}

compute_aggregable_amount <- function(table)
{
    table$create_column(
        "aggregable_amount",
        "ABS(",
        "NVL(otstndng_nmnl_amnt, 0)",
        " - NVL(trnsfrrd_amnt, 0) * partially_transferred_flag * partial_transfer_applicable",
        " - NVL(accmltd_imprmnt, 0) * impairment_flag",
        " - NVL(accmltd_chngs_fv_cr, 0) * fair_value_flag",
        " - NVL(fv_chng_cr_bfr_prchs, 0) * acquisition_price_flag",
        ")"
    )
}

compute_pro_rata_debtor_share <- function(table)
{
    table$create_column(
        "pro_rata_debtor_share",
        "CASE",
        " WHEN number_of_debtors IS NULL THEN 1",
        " WHEN number_of_debtors = 1 THEN 1",
        " WHEN number_of_debtors > 1 AND ROUND(total_deb_jnt_lblty_amnt, 5) = 0 THEN 1 / number_of_debtors",
        " WHEN number_of_debtors > 1 AND ROUND(total_deb_jnt_lblty_amnt, 5) <> 0 THEN NVL(deb_jnt_lblty_amnt, 0) / total_deb_jnt_lblty_amnt",
        " ELSE 0",
        " END"
    )
}

compute_main_debtor_share <- function(table)
{
    table$create_column(
        "main_debtor_share",
        "CASE",
        " WHEN number_of_debtors = 1 THEN 1",
        " WHEN main_debtor = 1 THEN 1 / number_of_main_debtors",
        " ELSE 0",
        " END"
    )
}

compute_creditor_share <- function(table)
{
    table$create_column(
        "creditor_share",
        "CASE",
        " WHEN number_of_creditors IS NULL THEN 1",
        " WHEN number_of_creditors = 1 THEN 1",
        " WHEN number_of_creditors > 1 THEN 1 / number_of_creditors",
        " ELSE 0",
        " END"
    )
}

compute_adjusted_aggregable_amount <- function(table)
{
    adjust_amount(table, "aggregable_amount")
}

compute_adjusted_transferred_amount <- function(table)
{
    adjust_amount(table, "trnsfrrd_amnt")
}

compute_adjusted_arrears <- function(table)
{
    adjust_amount(table, "arrrs")
}

compute_adjusted_outstanding_nominal_amount <- function(table)
{
    adjust_amount(table, "otstndng_nmnl_amnt")
}

compute_adjusted_accrued_interest <- function(table)
{
    adjust_amount(table, "accrd_intrst")
}

compute_adjusted_off_balance_sheet_amount <- function(table)
{
    adjust_amount(table, "off_blnc_sht_amnt")
}

adjust_amount <- function(table, variable, name = stringr::str_c(variable, "_a"))
{
    table$create_column(
        name,
        "CASE",
        " WHEN obsrvd_agnt_country IN ('AT', 'EE', 'FR', 'GR', 'IE', 'LT', 'LU', 'MT', 'NL', 'PT', 'SI', 'SK')",
        " THEN ", variable, " * creditor_share * pro_rata_debtor_share",
        " ELSE ", variable, " * creditor_share * main_debtor_share",
        " END"
    )
}

compute_interest_rate_type <- function(table)
{
    table$create_column(
        "annlsd_agrd_rt_type",
        "CASE",
        " WHEN ABS(annlsd_agrd_rt) < 1e-6 THEN 'ZERO'",
        " WHEN annlsd_agrd_rt IS NULL THEN 'NULL'",
        " WHEN ABS(annlsd_agrd_rt) > 99 THEN 'DUMMY'",
        " ELSE 'OK'",
        " END"
    )
}

compute_interest_rate <- function(table)
{
    table$create_column(
        "annlsd_agrd_rt_a",
        "CASE",
        " WHEN annlsd_agrd_rt_type = 'OK'",
        " THEN annlsd_agrd_rt",
        " ELSE NULL END"
    )
}

compute_interest_rate_weight <- function(table)
{
    table$create_column(
        "annlsd_agrd_rt_weight",
        "CASE WHEN annlsd_agrd_rt_type = 'OK'",
        " THEN aggregable_amount_a",
        " ELSE NULL END"
    )
}

compute_non_fiduciary_flag <- function(table)
{
    table$create_column(
        "non_fiduciary_flag",
        "CASE",
        " WHEN fdcry = '1'",
        " THEN 0",
        " ELSE 1 END"
    )
}

compute_recognition_flag <- function(table)
{
    table$create_column(
        "recognition_flag",
        "CASE",
        " WHEN rcgntn_stts = 3",
        " THEN 0",
        " ELSE 1 END"
    )
}

compute_intra_company_instrument_flag <- function(table)
{
    table$create_column(
        "intra_company_instrument_flag",
        "CASE",
        " WHEN cre_entty_id <> deb_entty_id",
        " THEN 0",
        " ELSE 1 END"
    )
}

compute_non_tradi_securitised_flag <- function(table)
{
    table$create_column(
        "non_tradi_securitised_flag",
        "CASE WHEN typ_scrtstn = 1",
        " AND obsrvd_agnt_country = 'IE'",
        " THEN 0",
        " ELSE 1 END"
    )
}

compute_settled_flag <- function(table)
{
    table$create_column(
        "settled_flag",
        "CASE",
        " WHEN SUBSTR(dt_sttlmnt, 1, 7) <= ref_date",
        " THEN 1",
        " ELSE 0 END"
    )
}

compute_bsi_instrument <- function(table)
{
    table$
        create_column(
            "bsi_instrument",
            "CASE",
            " WHEN non_fiduciary_flag = 1",
            " AND non_tradi_securitised_flag = 1",
            " AND settled_flag = 1",
            " AND (recognition_flag = 1 OR intra_company_instrument_flag = 1)",
            " THEN 1 ELSE 0 END"
        )
}

compute_bad_loan_flag <- function(table)
{
    table$
        create_column(
            "bad_loan_flag",
            "CASE WHEN ", is_not_null("dflt_stts"), " THEN",
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
        )
}

compute_mir_instrument <- function(table)
{
    table$create_column(
        "mir_instrument",
        "CASE",
        " WHEN non_fiduciary_flag = 1",
        " AND bad_loan_flag = 0",
        " AND non_tradi_securitised_flag = 1",
        " AND (recognition_flag = 1",
        "      OR intra_company_instrument_flag = 1)",
        " THEN 1 ELSE 0 END"
    )
}

compute_number_of_months_between <- function(date_1, date_2)
{
    if (get_sgbd() == "ORACLE")
    {
        return (stringr::str_c("MONTHS_BETWEEN(TO_DATE(", date_1, ", 'YYYY-MM-DD'), TO_DATE(", date_2, ", 'YYYY-MM-DD'))"))
    }
    else
    {
        return (cast_as_decimal(stringr::str_c("MONTHS_BETWEEN(TO_DATE(", date_1, "), TO_DATE(", date_2, "))")))
    }
}

compute_maturity_in_months <- function(table)
{
    table$create_column("maturity_in_months", compute_number_of_months_between("dt_lgl_fnl_mtrty", "dt_sttlmnt"))
}

compute_maturity <- function(table)
{
    table$create_column(
        "maturity",
        "CASE WHEN ", is_null("dt_lgl_fnl_mtrty"), " THEN",
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
    )
}

compute_new_business <- function(table)
{
    table$create_column(
        "new_business",
        "CASE",
        " WHEN SUBSTR(dt_incptn, 1, 7) = ref_date",
        " THEN 1",
        " ELSE 0 END"
    )
}

compute_amount_category <- function(table)
{
    table$create_column(
        "amount_category", 
        "CASE",
        " WHEN otstndng_nmnl_amnt <= 25000 THEN '(-Inf : 25000]'",
        " WHEN otstndng_nmnl_amnt <= 50000 THEN '(25000 : 50000]'",
        " WHEN otstndng_nmnl_amnt <= 100000 THEN '(50000 : 100000]'",
        " WHEN otstndng_nmnl_amnt <= 250000 THEN '(100000 : 250000]'",
        " WHEN otstndng_nmnl_amnt <= 1000000 THEN '(250000 : 1000000]'",
        " WHEN otstndng_nmnl_amnt > 1000000 THEN '(1000000 : +Inf)'",
        " ELSE 'Inconnue' END"
    )
}

compute_common_variables <- function(table)
{
    compute_partially_transferred_flag(table)
    compute_partial_transfer_applicable(table)
    compute_impairment_flag(table)
    compute_fair_value_flag(table)
    compute_acquisition_price_flag(table)
    compute_pro_rata_debtor_share(table)
    compute_main_debtor_share(table)
    compute_creditor_share(table)
    compute_interest_rate_type(table)
    compute_non_fiduciary_flag(table)
    compute_recognition_flag(table)
    compute_intra_company_instrument_flag(table)
    compute_non_tradi_securitised_flag(table)
    compute_settled_flag(table)
    compute_bad_loan_flag(table)
    compute_maturity_in_months(table)
    compute_new_business(table)
    compute_amount_category(table)
    
    compute_aggregable_amount(table)
    compute_interest_rate(table)
    compute_bsi_instrument(table)
    compute_mir_instrument(table)
    compute_maturity(table)
    compute_adjusted_transferred_amount(table)
    compute_adjusted_arrears(table)
    compute_adjusted_outstanding_nominal_amount(table)
    compute_adjusted_accrued_interest(table)
    compute_adjusted_off_balance_sheet_amount(table)
    
    compute_adjusted_aggregable_amount(table)
    
    table$create_column("encours", "aggregable_amount_a")
    
    compute_interest_rate_weight(table)
    
    simplify_institutional_sector(table)
    simplify_country(table)
    simplify_enterprise_size(table)
    simplify_economic_activity(table)
    simplify_currency(table)
    simplify_non_performing_loans(table)
}

adjust_interest_rate <- function(table, variables)
{
    variables <- stringr::str_c(variables, collapse = ", ")
    
    table$
        create_column("numerator", "SUM(ABS(annlsd_agrd_rt_a) * annlsd_agrd_rt_weight) OVER (PARTITION BY ", variables, ")")$
        create_column("denominator", "SUM(annlsd_agrd_rt_weight) OVER (PARTITION BY ", variables, ")")$
        create_column(
            "criteria", 
            "CASE",
            " WHEN numerator IS NULL",
            " OR denominator IS NULL",
            " OR ROUND(denominator, 5) = 0",
            " THEN 0",
            " ELSE numerator / denominator END"
        )$
        create_column(
            "annlsd_agrd_rt_divided_by_100",
            "CASE WHEN criteria > 0.2",
            " THEN 1",
            " ELSE 0 END"
        )$
        create_column(
            "annlsd_agrd_rt_a", 
            "CASE",
            " WHEN annlsd_agrd_rt_divided_by_100 = 1",
            " THEN annlsd_agrd_rt_a / 100",
            " ELSE annlsd_agrd_rt_a END"
        )$
        drop("numerator", "denominator", "criteria")
    
    if (table$has_column("ticket_id"))
    {
        table$drop("ticket_id")
    }
}

compute_weighted_average <- function(variable, weight)
{
    return (stringr::str_c(
        "CASE",
        " WHEN ROUND(SUM(", weight, "), 5) = 0",
        " THEN NULL",
        " ELSE SUM(", variable, " * ", weight, ") / SUM(", weight, ")",
        " END"
    ))
}

is_null <- function(variable)
{
    return (stringr::str_c("(", variable, " IS NULL OR ", cast_as_string(variable), " IN ('-4', '-99', 'DL_NULL', '', 'N-A'))"))
}

is_not_null <- function(variable)
{
    return (stringr::str_c("(", variable, " IS NOT NULL AND ", cast_as_string(variable), " NOT IN ('-4', '-99', 'DL_NULL', '', 'N-A'))"))
}

simplify_institutional_sector <- function(table)
{
    for (variable in c("deb_insttnl_sctr", "cre_insttnl_sctr"))
    {
        table$create_column(
            stringr::str_c(variable, "_s"),
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
        )
    }
}

simplify_country <- function(table)
{
    for (variable in c("deb_addr_cntry", "cre_addr_cntry"))
    {
        table$create_column(
            stringr::str_c(variable, "_s"),
            "CASE",
            " WHEN ", variable, " IN ('AT', 'BE', 'CY', 'DE', 'EE', 'ES', 'FI', 'FR', 'IE', 'IT', 'GR', 'LT', 'LU', 'LV', 'MT', 'NL', 'PT', 'SI', 'SK') THEN ", variable,
            " WHEN ", variable, " = 'MC' THEN 'FR'",
            " WHEN ", is_null(variable), " THEN 'Inconnu'",
            " ELSE 'Hors zone euro'",
            " END"
        )
    }
}

simplify_economic_activity <- function(table)
{
    for (variable in c("deb_ecnmc_actvty"))
    {
        variable_s <- stringr::str_c(variable, "_s")
        
        table$create_column(variable_s, "SUBSTR(", variable, ", 1, 2)")
        
        table$create_column(
            variable_s,
            "CASE",
            " WHEN ", variable_s, " >= '01' AND ", variable_s, " <= '03' THEN 'A'",
            " WHEN ", variable_s, " >= '05' AND ", variable_s, " <= '09' THEN 'B'",
            " WHEN ", variable_s, " >= '10' AND ", variable_s, " <= '33' THEN 'C'",
            " WHEN ", variable_s, " >= '35' AND ", variable_s, " <= '35' THEN 'D'",
            " WHEN ", variable_s, " >= '36' AND ", variable_s, " <= '39' THEN 'E'",
            " WHEN ", variable_s, " >= '41' AND ", variable_s, " <= '43' THEN 'F'",
            " WHEN ", variable_s, " >= '45' AND ", variable_s, " <= '47' THEN 'G'",
            " WHEN ", variable_s, " >= '49' AND ", variable_s, " <= '53' THEN 'H'",
            " WHEN ", variable_s, " >= '55' AND ", variable_s, " <= '56' THEN 'I'",
            " WHEN ", variable_s, " >= '58' AND ", variable_s, " <= '63' THEN 'J'",
            " WHEN ", variable_s, " >= '64' AND ", variable_s, " <= '66' THEN 'K'",
            " WHEN ", variable_s, " >= '68' AND ", variable_s, " <= '68' THEN 'L'",
            " WHEN ", variable_s, " >= '69' AND ", variable_s, " <= '75' THEN 'M'",
            " WHEN ", variable_s, " >= '77' AND ", variable_s, " <= '82' THEN 'N'",
            " WHEN ", variable_s, " >= '84' AND ", variable_s, " <= '84' THEN 'O'",
            " WHEN ", variable_s, " >= '85' AND ", variable_s, " <= '85' THEN 'P'",
            " WHEN ", variable_s, " >= '86' AND ", variable_s, " <= '88' THEN 'Q'",
            " WHEN ", variable_s, " >= '90' AND ", variable_s, " <= '93' THEN 'R'",
            " WHEN ", variable_s, " >= '94' AND ", variable_s, " <= '96' THEN 'S'",
            " WHEN ", variable_s, " >= '97' AND ", variable_s, " <= '98' THEN 'T'",
            " WHEN ", variable_s, " >= '99' AND ", variable_s, " <= '99' THEN 'U'",
            " WHEN ", is_null(variable), " OR ", variable, " = '00_00' THEN 'Inconnue'",
            " ELSE ", variable,
            " END"
        )
    }
}

simplify_enterprise_size <- function(table)
{
    for (variable in c("deb_entrprse_sze"))
    {
        table$create_column(
            stringr::str_c(variable, "_s"),
            "CASE",
            " WHEN ", variable, " = 1 THEN 'GE / ETI'",
            " WHEN ", variable, " IN (2, 3) THEN 'PME hors TPE'",
            " WHEN ", variable, " = 4 THEN 'TPE'",
            " ELSE 'Inconnue'",
            " END"
        )
    }
}

simplify_currency <- function(table)
{
    table$create_column(
        "crrncy_dnmntn_s",
        "CASE",
        " WHEN crrncy_dnmntn = 'EUR' THEN 'EUR'",
        " WHEN ", is_null("crrncy_dnmntn"), " THEN 'Inconnue'",
        " ELSE 'Z06'",
        " END"
    )
}

simplify_non_performing_loans <- function(table)
{
    table$create_column(
        "prfrmng_stts_s",
        "CASE",
        " WHEN prfrmng_stts = '1' THEN 'Non-performing'",
        " WHEN prfrmng_stts = '11' THEN 'Performing'",
        " ELSE 'Inconnu'",
        " END"
    )
}

format_date_from_dd_mm_yyyy_to_yyyy_mm_dd <- function(table, variable)
{
    # HIVE ONLY
    
    table$create_column(
        variable,
        concatenate(
            stringr::str_c("SUBSTR(", variable, ", 7, 4)"),
            "'-'", 
            stringr::str_c(" SUBSTR(", variable, ", 4, 2)"),
            "'-'", 
            stringr::str_c(" SUBSTR(", variable, ", 1, 2)")
        )
    )
}

format_date_from_yyyymmdd_to_yyyy_mm_dd <- function(table, variable)
{
    # ORACLE ONLY
    
    table$create_column(
        variable, 
        "CASE WHEN REGEXP_LIKE(", variable, ", '^\\d{8}$') THEN",
        " ",
        concatenate(
            stringr::str_c("SUBSTR(", variable, ", 1, 4)"),
            "'-'",
            stringr::str_c("SUBSTR(", variable, ", 5, 2)"),
            "'-'",
            stringr::str_c("SUBSTR(", variable, ", 7, 2)")
        ),
        " ELSE NULL END"
    )
}

