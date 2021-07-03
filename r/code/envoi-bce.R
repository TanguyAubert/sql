
get_creditor_view_envoi_bce <- function(ref_date = "", ra = "", oa = "")
{
    set_sgbd("HIVE")
    
    ref_date <- remove_null_values(ref_date)
    ra <- remove_null_values(ra)
    oa <- remove_null_values(oa)
    
    if (base::length(ref_date) == 0 & base::length(ra) == 0 & base::length(oa) == 0)
    {
        base::stop("This function cannot be called without at least one filter !")
    }
    
    T2   <- get_envoi_bce_t2(ref_date, ra, oa)
    T3   <- get_envoi_bce_t3(ref_date, ra, oa)
    T6   <- get_envoi_bce_t6("", ra, oa)
    T4C  <- get_envoi_bce_t4(ref_date, ra, oa)
    T4D  <- get_envoi_bce_t4(ref_date, ra, oa)
    T5   <- get_envoi_bce_t5(ref_date, ra, oa)
    T1   <- get_golden_record_t1()
    T9   <- get_envoi_bce_t9(ref_date, ra, oa)
    T10  <- get_envoi_bce_t10(ref_date, ra, oa)
    
    keys <- c("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
    
    instruments <- T3$left_join(T2, keys)
    
    add_accounting_table(instruments, T6, ref_date)
    
    T4C$
        drop_key("entty_rl", 1)$
        rename("entty_id", "cre_entty_id")$
        rename("entty_riad_cd", "cre_entty_riad_cd")
    
    compute_number_of_creditors(T4C, keys)
    
    T5$
        rename("jnt_lblty_amnt", "deb_jnt_lblty_amnt")$
        rename("entty_id", "deb_entty_id")$
        rename("entty_riad_cd", "deb_entty_riad_cd")
    
    T4D$
        drop_key("entty_rl", 2)$
        rename("entty_id", "deb_entty_id")$
        rename("entty_riad_cd", "deb_entty_riad_cd")$
        left_join(T5, keys, "deb_entty_id", "deb_entty_riad_cd")
    
    compute_number_of_debtors        (T4D, keys)
    compute_total_deb_jnt_lblty_amnt (T4D, keys)
    compute_main_debtor              (T4D, keys)
    compute_number_of_main_debtors   (T4D, keys)
    
    T1D <- T1$copy()$prefix_all_except("deb_")
    T1C <- T1$copy()$prefix_all_except("cre_")
    
    T9_T10 <- T9$full_join(T10, "ref_date", "ra", "oa", "entty_id", "entty_riad_cd")
    
    T9_T10D <- T9_T10$copy()$prefix_all_except("deb_", "ref_date", "ra", "oa")
    T9_T10C <- T9_T10$copy()$prefix_all_except("cre_", "ref_date", "ra", "oa")
    
    creditors <- T4C$
        left_join(T1C, "cre_entty_riad_cd")$
        left_join(T9_T10C, "ref_date", "ra", "oa", "cre_entty_id", "cre_entty_riad_cd")

    debtors <- T4D$
        left_join(T1D, "deb_entty_riad_cd")$
        left_join(T9_T10D, "ref_date", "ra", "oa", "deb_entty_id", "deb_entty_riad_cd")
    
    table <- instruments$
        left_join(creditors, keys)$
        left_join(debtors, keys)$
        ignore_latest_error()
    
    table$create_column("obsrvd_agnt_country", "'FR'")
    
    compute_common_variables(table)
    
    compute_pge_envoi_bce(table, ref_date, ra, oa)
    
    adjust_interest_rate(table, c("ref_date", "ra", "oa", "ticket_id"))
    
    return (table)
}

compute_pge_envoi_bce <- function(table, ref_date, ra, oa)
{
    T7 <- get_envoi_bce_t7(ref_date, ra, oa)$
        select("ref_date", "ra", "oa", "prtctn_id", "prtctn_prvdr_cd")
    
    T8 <- get_envoi_bce_t8(ref_date, ra, oa)$
        select("ref_date", "ra", "oa", "prtctn_id", "cntrct_id", "instrmnt_id")
    
    pge <- T8$
        left_join(T7, "ref_date", "ra", "oa", "prtctn_id")$
        filter("prtctn_prvdr_cd = 'FR130019763'")$
        select("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")$
        unique()$
        set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")$
        create_column("pge", 1)
    
    table$left_join(pge, "ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
    table$create_column("pge", "CASE WHEN pge = 1 THEN 1 ELSE 0 END")
}
