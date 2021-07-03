
get_creditor_view_secure_desktop <- function(ref_date = "", ra = "", oa = "")
{
    set_sgbd("ORACLE")
    
    ref_date <- remove_null_values(ref_date)
    ra <- remove_null_values(ra)
    oa <- remove_null_values(oa)
    
    if (base::length(ref_date) == 0 & base::length(ra) == 0 & base::length(oa) == 0)
    {
        base::stop("This function cannot be called without at least one filter !")
    }
    
    dt_rfrnc <- stringr::str_replace_all(ref_date, "-", "")
    
    is_numeric <- function(x)
    {
        return(!base::is.na(base::suppressWarnings(base::is.numeric(x))))
    }
    
    filter_ra <- stringr::str_length(ra) == 5 & is_numeric(ra)
    filter_oa <- stringr::str_length(oa) == 5 & is_numeric(oa)
    
    ra[filter_ra] <- stringr::str_c("FR", ra[filter_ra])
    oa[filter_oa] <- stringr::str_c("FR", oa[filter_oa])
    
    obsrvd_agnt_acr_id <- c()
    
    for (searched in oa)
    {
        dt_rfrnc_searched <- ""
        
        if (base::length(dt_rfrnc) > 0)
        {
            dt_rfrnc_searched <- base::max(dt_rfrnc, na.rm = TRUE)
        }
        
        found <- get_obsrvd_agnt_acr_id_from_oa(dt_rfrnc_searched, searched)
        
        if (base::length(found) == 0)
        {
            base::stop("Could not find oa ", searched, " !")
        }
        
        found <- found[1]
        
        obsrvd_agnt_acr_id <- c(obsrvd_agnt_acr_id, found)
    }
    
    T2 <- get_v_instrmnt_by_snpsht(dt_rfrnc, obsrvd_agnt_acr_id)
    T3 <- get_v_fnncl_by_snpsht(dt_rfrnc, obsrvd_agnt_acr_id)
    T6 <- get_v_accntng_by_snpsht("", obsrvd_agnt_acr_id)
    T4C <- get_v_entty_instrmnt_by_snpsht(dt_rfrnc, obsrvd_agnt_acr_id)
    T4D <- get_v_entty_instrmnt_by_snpsht(dt_rfrnc, obsrvd_agnt_acr_id)
    T1 <- get_d_entty_by_dt_rfrnc(dt_rfrnc)
    T9_T10 <- get_v_entty_rsk_by_snpsht(dt_rfrnc, obsrvd_agnt_acr_id)
    
    keys <- c("dt_rfrnc", "obsrvd_agnt_acr_id", "cntrct_id", "instrmnt_id")
    
    instruments <- T3$left_join(T2, keys)
    
    add_accounting_table_secure_desktop(instruments, T6, ref_date)
    
    T4C$
        drop_key("entty_rl", 1)$
        drop("jnt_lblty_amnt")$
        rename("entty_id", "cre_entty_id")
    
    compute_number_of_creditors(T4C, keys)
    
    T4D$
        drop_key("entty_rl", 2)$
        rename("entty_id", "deb_entty_id")$
        rename("jnt_lblty_amnt", "deb_jnt_lblty_amnt")
    
    compute_number_of_debtors        (T4D, keys)
    compute_total_deb_jnt_lblty_amnt (T4D, keys)
    compute_main_debtor              (T4D, keys)
    compute_number_of_main_debtors   (T4D, keys)
    
    T1D <- T1$copy()$prefix_all_except("deb_", "dt_rfrnc")
    T1C <- T1$copy()$prefix_all_except("cre_", "dt_rfrnc")
    
    T9_T10D <- T9_T10$copy()$prefix_all_except("deb_", "dt_rfrnc", "obsrvd_agnt_acr_id")
    T9_T10C <- T9_T10$copy()$prefix_all_except("cre_", "dt_rfrnc", "obsrvd_agnt_acr_id")
    
    creditors <- T4C$
        left_join(T1C, "dt_rfrnc", "cre_entty_id")$
        left_join(T9_T10C, "dt_rfrnc", "obsrvd_agnt_acr_id", "cre_entty_id")
    
    debtors <- T4D$
        left_join(T1D, "dt_rfrnc", "deb_entty_id")$
        left_join(T9_T10D, "dt_rfrnc", "obsrvd_agnt_acr_id", "deb_entty_id")
    
    table <- instruments$
        left_join(creditors, keys)$
        left_join(debtors, keys)$
        ignore_latest_error()
    
    compute_observed_agent_country_and_riad_code(table, dt_rfrnc, obsrvd_agnt_acr_id)
    
    v_oa_by_dt_rfrnc_typ_rprtng <- get_v_oa_by_dt_rfrnc_typ_rprtng(dt_rfrnc)
    
    v_oa_by_dt_rfrnc_typ_rprtng$
        select("obsrvd_agnt_acr_id", "ra_cd")$
        rename("ra_cd", "ra")$
        unique()$
        set_keys("obsrvd_agnt_acr_id")
    
    table$left_join(v_oa_by_dt_rfrnc_typ_rprtng, "obsrvd_agnt_acr_id")
    
    filter_ra_if_not_empty(table, ra)
    
    compute_common_variables(table)
    
    compute_pge_secure_desktop(table, ref_date, ra, oa)
    
    adjust_interest_rate(table, c("dt_rfrnc", "obsrvd_agnt_acr_id"))
    
    return (table)
}

get_obsrvd_agnt_acr_id_from_oa <- function(dt_rfrnc, oa)
{
    table <- get_d_entty_by_dt_rfrnc(dt_rfrnc)$
        filter("entty_riad_cd = '", oa, "'")$
        select("entty_id")$
        execute(force = TRUE)
    
    data.table::setnames(table, stringr::str_to_lower(base::colnames(table)))
    
    return (table[["entty_id"]])
}

compute_observed_agent_country_and_riad_code <- function(table, dt_rfrnc, obsrvd_agnt_acr_id)
{
    code_riad <- get_d_entty_by_dt_rfrnc(dt_rfrnc)$
        filter("entty_riad_cd IS NOT NULL")$
        select("entty_id", "entty_riad_cd")$
        create_column("obsrvd_agnt_country", "SUBSTR(entty_riad_cd, 1, 2)")$
        rename("entty_id", "obsrvd_agnt_acr_id")$
        rename("entty_riad_cd", "oa")$
        unique()$
        set_keys("obsrvd_agnt_acr_id")
    
    table$left_join(code_riad, "obsrvd_agnt_acr_id")
}

compute_pge_secure_desktop <- function(table, ref_date, ra, oa)
{
    # TO DO
}

add_accounting_table_secure_desktop <- function(instruments, accounting, ref_date)
{
    dt_rfrnc_quarter <- get_previous_quarter(ref_date)
    dt_rfrnc_quarter <- base::unique(dt_rfrnc_quarter)
    dt_rfrnc_quarter <- stringr::str_replace_all(dt_rfrnc_quarter, "-", "")
    
    compute_ref_month(instruments)
    compute_ref_year(instruments)
    
    instruments$create_column("ref_date", concatenate("ref_year", "'-'", "ref_month"))
    
    if (base::length(ref_date) == 1)
    {
        accounting$drop_key("dt_rfrnc", dt_rfrnc_quarter)
        
        instruments$left_join(accounting, "obsrvd_agnt_acr_id", "cntrct_id", "instrmnt_id")
        
    } else
    {
        filter_dt_rfrnc_if_not_empty(accounting, dt_rfrnc_quarter)
        
        accounting$rename("dt_rfrnc", "dt_rfrnc_t6")
        
        compute_previous_ref_year(instruments)
        
        instruments$
            create_column("dt_rfrnc_t6", cast_as_int(compute_previous_reference_date("", cast_as_string("dt_rfrnc"))))$
            left_join(accounting, "dt_rfrnc_t6", "obsrvd_agnt_acr_id", "cntrct_id", "instrmnt_id")$
            drop("previous_ref_year", "dt_rfrnc_t6")
    }
    
    instruments$drop("ref_month", "ref_year")
}
