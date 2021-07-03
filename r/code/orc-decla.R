
get_creditor_view_orc_decla <- function(ref_date = "", ra = "", oa = "")
{
    set_sgbd("HIVE")
    
    ref_date <- remove_null_values(ref_date)
    ra <- remove_null_values(ra)
    oa <- remove_null_values(oa)
    
    if (base::length(ref_date) == 0 & base::length(ra) == 0 & base::length(oa) == 0)
    {
        base::stop("This function cannot be called without at least one filter !")
    }
    
    T2               <- get_orc_decla_t2(ref_date, ra, oa)
    T3               <- get_orc_decla_t3(ref_date, ra, oa)
    T6               <- get_orc_decla_t6("", ra, oa)
    T4C              <- get_orc_decla_t4(ref_date, ra, oa)
    T4D              <- get_orc_decla_t4(ref_date, ra, oa)
    T5               <- get_orc_decla_t5(ref_date, ra, oa)
    code_riad_ci     <- get_code_riad_ci(ra)
    golden_record_t1 <- get_golden_record_t1()
    T1               <- get_orc_decla_t1(ra, oa)
    T9               <- get_orc_decla_t9(ref_date, ra, oa)
    T10              <- get_orc_decla_t10(ref_date, ra, oa)
    
    keys <- c("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
    
    instruments <- T3$left_join(T2, keys)
    
    add_accounting_table(instruments, T6, ref_date)
    
    T4C$
        drop_key("entty_rl", 1)$
        rename("entty_id", "cre_entty_id")
    
    compute_number_of_creditors(T4C, keys)
    
    T5$
        rename("jnt_lblty_amnt", "deb_jnt_lblty_amnt")$
        rename("entty_id", "deb_entty_id")
    
    T4D$
        drop_key("entty_rl", 2)$
        rename("entty_id", "deb_entty_id")$
        left_join(T5, keys, "deb_entty_id")
        
    compute_number_of_debtors        (T4D, keys)
    compute_total_deb_jnt_lblty_amnt (T4D, keys)
    compute_main_debtor              (T4D, keys)
    compute_number_of_main_debtors   (T4D, keys)
    
    code_riad_ci$
        select("ra", "entty_id", "entty_riad_cd")
    
    golden_record_t1$
        prefix_all_except("gr_", "entty_riad_cd")$
        inner_join(code_riad_ci, "entty_riad_cd")
    
    T1$
        create_column(
            "insttnl_sctr",
            "CASE",
            " WHEN insttnl_sctr IN ('S122_A', 'S122_B') THEN 'S122'",
            " WHEN insttnl_sctr IN ('S125_A', 'S125_B', 'S125_C', 'S125_NN') THEN 'S125'",
            " WHEN insttnl_sctr IN ('S126_A', 'S126_D') THEN 'S126'",
            " WHEN insttnl_sctr IN ('S13111', 'S13112') THEN 'S1311'",
            " WHEN insttnl_sctr IN ('S1313_A', 'S1313_B') THEN 'S1313'",
            " WHEN insttnl_sctr IN ('S1314_A', 'S1314_B') THEN 'S1314'",
            " WHEN insttnl_sctr = 'S14_A' THEN 'S14'",
            " ELSE insttnl_sctr",
            " END"
        )$
        prefix_all_except("orc_", "ra", "oa", "entty_id")$
        left_join(golden_record_t1, "ra", "entty_id")
    
    columns <- base::intersect(
        get_orc_decla_t1()$get_columns(),
        get_golden_record_t1()$get_columns()
    )
    
    for (column in columns)
    {
        T1$create_column(column, "CASE WHEN gr_", column, " IS NULL THEN orc_", column, " ELSE gr_", column, " END")
    }
    
    T1D <- T1$copy()$prefix_all_except("deb_", "ra", "oa")
    T1C <- T1$copy()$prefix_all_except("cre_", "ra", "oa")
    
    T9_T10 <- T9$full_join(T10, "ref_date", "ra", "oa", "entty_id")
    
    T9_T10D <- T9_T10$copy()$prefix_all_except("deb_", "ref_date", "ra", "oa")
    T9_T10C <- T9_T10$copy()$prefix_all_except("cre_", "ref_date", "ra", "oa")
    
    creditors <- T4C$
        left_join(T1C, "ra", "oa", "cre_entty_id")$
        left_join(T9_T10C, "ref_date", "ra", "oa", "cre_entty_id")
    
    debtors <- T4D$
        left_join(T1D, "ra", "oa", "deb_entty_id")$
        left_join(T9_T10D, "ref_date", "ra", "oa", "deb_entty_id")
    
    table <- instruments$
        left_join(creditors, keys)$
        left_join(debtors, keys)$
        ignore_latest_error()
    
    table$create_column("obsrvd_agnt_country", "'FR'")
    
    compute_common_variables(table)
    
    compute_pge_orc_decla(table, ref_date, ra, oa)
    
    adjust_interest_rate(table, c("ref_date", "ra", "oa", "ticket_id"))
    
    return (table)
}

compute_pge_orc_decla <- function(table, ref_date, ra, oa)
{
    T7 <- get_orc_decla_t7(ref_date, ra, oa)
    T8 <- get_orc_decla_t8(ref_date, ra, oa)
    code_riad_ci <- get_code_riad_ci(ra)
    
    code_riad_ci$
        select("ra", "entty_id", "entty_riad_cd")$
        rename("entty_id", "prtctn_prvdr_cd")$
        rename("entty_riad_cd", "prtctn_prvdr_riad_cd")
    
    pge <- T7$
        copy()$
        select("ref_date", "ra", "oa", "prtctn_id", "prtctn_prvdr_cd")$
        left_join(code_riad_ci, "ra", "prtctn_prvdr_cd")
    
    pge <- T8$
        copy()$
        select("ref_date", "ra", "oa", "prtctn_id", "cntrct_id", "instrmnt_id")$
        left_join(pge, "ref_date", "ra", "oa", "prtctn_id")$
        filter("prtctn_prvdr_riad_cd = 'FR130019763'")$
        select("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")$
        unique()$
        set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")$
        create_column("pge", 1)
    
    table$left_join(pge, "ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
    table$create_column("pge", "CASE WHEN pge = 1 THEN 1 ELSE 0 END")
}

