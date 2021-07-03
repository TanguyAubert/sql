
#### ORC DECLA ####

get_orc_decla_t1 <- function(ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t1",
        "ra",
        "oa",
        "actif",
        "entty_id",
        "accntg_stndrd",
        "addr_cntry",
        "addr_cty",
        "addr_dpt",
        "addr_pstcd",
        "anl_trnovr",
        "blnc_sht_tl",
        "code_amf",
        "code_opc",
        "code_ot",
        "dt_entrprse_sze",
        "dt_intlgl_prcdng",
        "ecnmc_actvty",
        "entrprse_sze",
        "hd_off_undrtkg_id",
        "immdt_prt_undrtkg_id",
        "insttnl_sctr",
        "lgl_entty_id",
        "lgl_frm",
        "name",
        "numbr_emply",
        "org_int",
        "sts_lgl_prcdng",
        "ult_prt_undrtkg_id",
        
        # Présent dans orc_decla_t1 mais pas dans golden_record_t1
        "addr_strt",
        "ntnl_id",
        "typ_ntnl_id",
        "descr_other"
        
        # "brth_cmmne",
        # "brth_cmmne__nme",
        # "brth_cntry",
        # "brth_dpt",
        # "brth_dte",
        # "frst_nme",
        # "sex",
        # "siren_bdf",
        # "usl_nme"
    )
    
    table$filter("actif = 1")
    table$drop("actif")
    table$set_keys("ra", "oa", "entty_id")
    
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    table$create_column("dt_intlgl_prcdng", "CAST (dt_intlgl_prcdng AS STRING)")
    
    return (table)
}

get_orc_decla_t2 <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t2",
        "ref_date",
        "ra",
        "oa",
        "cntrct_id",
        "instrmnt_id",
        "cmmtmnt_incptn",
        "crrncy_dnmntn",
        "dt_end_intrst_only",
        "dt_incptn",
        "dt_lgl_fnl_mtrty",
        "dt_sttlmnt",
        "fdcry",
        "fv_chng_cr_bfr_prchs",
        "intrst_rt_cp",
        "intrst_rt_flr",
        "intrst_rt_rst_frqncy",
        "intrst_rt_sprd",
        "prjct_fnnc_ln",
        "prps",
        "pymnt_frqncy",
        "rcrs",
        "rfrnc_rt",
        "rpymnt_rghts",
        "sbrdntd_dbt",
        "syndctd_cntrct_id",
        "typ_amrtstn",
        "typ_instrmnt",
        "typ_intrst_rt"
    )
    
    table$set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_orc_decla_t3 <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t3",
        "ref_date",
        "ra",
        "oa",
        "cntrct_id",
        "instrmnt_id",
        "accrd_intrst",
        "annlsd_agrd_rt",
        "arrrs",
        "dflt_stts",
        "dt_dflt_stts",
        "dt_nxt_intrst_rt_rst",
        "dt_pst_d",
        "off_blnc_sht_amnt",
        "otstndng_nmnl_amnt",
        "trnsfrrd_amnt",
        "typ_scrtstn",
        
        "ticket_id"
    )
    
    table$set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_orc_decla_t4 <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t4",
        "ref_date",
        "ra",
        "oa",
        "cntrct_id",
        "instrmnt_id",
        "entty_id",
        "entty_rl"
    )
    
    table$set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id", "entty_id", "entty_rl")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_orc_decla_t5 <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t5",
        "ref_date",
        "ra",
        "oa",
        "cntrct_id",
        "instrmnt_id",
        "entty_id",
        "jnt_lblty_amnt"
    )
    
    table$set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id", "entty_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_orc_decla_t6 <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t6",
        "ref_date",
        "ra",
        "oa",
        "cntrct_id",
        "instrmnt_id",
        "accmltd_chngs_fv_cr",
        "accmltd_imprmnt",
        "accmltd_wrtffs",
        "accntng_clssfctn",
        "cmltv_rcvrs_snc_dflt",
        "crryng_amnt",
        "dt_frbrnc_stts",
        "dt_prfrmng_stts",
        "frbrnc_stts",
        "imprmnt_assssmnt_mthd",
        "imprmnt_stts",
        "prdntl_prtfl",
        "prfrmng_stts",
        "prvsns_off_blnc_sht",
        "rcgntn_stts",
        "src_encmbrnc"
    )
    
    table$set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_orc_decla_t7 <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t7",
        "ref_date",
        "ra",
        "oa",
        "prtctn_id",
        "dt_mtrty_prtctn",
        "dt_orgnl_prtctn_vl",
        "dt_prtctn_vl",
        "orgnl_prtctn_vl",
        "prtctn_prvdr_cd",
        "prtctn_vl",
        "prtctn_vltn_apprch",
        "rl_estt_clltrl_lctn",
        "typ_prtctn",
        "typ_prtctn_vl"
    )
    
    table$set_keys("ref_date", "ra", "oa", "prtctn_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_orc_decla_t8 <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t8",
        "ref_date",
        "ra",
        "oa",
        "prtctn_id",
        "cntrct_id",
        "instrmnt_id",
        "prtctn_allctd_vl",
        "thrd_prty_prrty_clms"
    )
    
    table$set_keys("ref_date", "ra", "oa", "prtctn_id", "cntrct_id", "instrmnt_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_orc_decla_t9 <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t9",
        "ref_date",
        "ra",
        "oa",
        "entty_id",
        "pd"
    )
    
    table$set_keys("ref_date", "ra", "oa", "entty_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_orc_decla_t10 <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "orc_decla_t10",
        "ref_date",
        "ra",
        "oa",
        "entty_id",
        "dflt_stts",
        "dt_dflt_stts"
    )
    
    table$set_keys("ref_date", "ra", "oa", "entty_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_golden_record_t1 <- function()
{
    table <- SQL_Table(
        "golden_record_t1",
        "code_riad",
        "lei",
        "code_opc",
        "code_ot",
        "code_amf",
        "code_org_int",
        "identifiant_head_office",
        "identifiant_immediate_parent",
        "identifiant_ultimate_parent",
        "denomination",
        "code_postal",
        "ville",
        "code_nuts",
        "pays",
        "code_secteur",
        "code_nace",
        "taille_entreprise",
        "forme_juridique",
        "date_taille_entreprise",
        "nombre_employes",
        "total_bilan",
        "chiffre_affaires",
        "statut_procedure_judiciaire",
        "date_statut_procedure_judiciaire",
        "norme_comptable_indiv"
        
        # Présent dans golden_record_t1 mais pas dans orc_decla_t1
        # "code_riad_temp",
        # "siren",
        # "siren_fictif",
        # "cib",
        # "code_ncb",
        # "code_nis",
        # "identifiant_rci",
        # "identifiant_rna",
        # "code_bic",
        # "identifiant_etranger",
        # "type_identifiant_etranger",
        # "date_debut_entite",
        # "date_fin_entite",
        # "adresse_ligne1",
        # "adresse_ligne2",
        # "adresse_ligne3",
        # "adresse_ligne4",
        # "code_sous_secteur",
        # "actif",
        # "contrepartie_creation",
        # "contrepartie_lastupdated"
    )$
        rename("code_riad", "entty_riad_cd")$
        rename("lei", "lgl_entty_id")$
        rename("code_org_int", "org_int")$
        rename("identifiant_head_office", "hd_off_undrtkg_id")$
        rename("identifiant_immediate_parent", "immdt_prt_undrtkg_id")$
        rename("identifiant_ultimate_parent", "ult_prt_undrtkg_id")$
        rename("denomination", "name")$
        rename("code_postal", "addr_pstcd")$
        rename("ville", "addr_cty")$
        rename("code_nuts", "addr_dpt")$
        rename("pays", "addr_cntry")$
        rename("code_secteur", "insttnl_sctr")$
        rename("code_nace", "ecnmc_actvty")$
        rename("taille_entreprise", "entrprse_sze")$
        rename("forme_juridique", "lgl_frm")$
        rename("date_taille_entreprise", "dt_entrprse_sze")$
        rename("nombre_employes", "numbr_emply")$
        rename("total_bilan", "blnc_sht_tl")$
        rename("chiffre_affaires", "anl_trnovr")$
        rename("statut_procedure_judiciaire", "sts_lgl_prcdng")$
        rename("date_statut_procedure_judiciaire", "dt_intlgl_prcdng")$
        rename("norme_comptable_indiv", "accntg_stndrd")
    
    table$filter("entty_riad_cd IS NOT NULL")
    table$set_keys("entty_riad_cd")
    
    format_date_from_dd_mm_yyyy_to_yyyy_mm_dd(table, "dt_intlgl_prcdng")
    
    return (table)
}

get_code_riad_ci <- function(ra = "")
{
    table <- SQL_Table(
        "code_riad_ci",
        "ra",
        "conterparty_identifier",
        "code_riad"
    )
    
    table$rename("conterparty_identifier", "entty_id")
    table$rename("code_riad", "entty_riad_cd")
    table$set_keys("ra", "entty_id")
    
    filter_ra_if_not_empty(table, ra)
    
    return (table)
}

get_orc_decla <- function(i)
{
    if (i == 1) return (get_orc_decla_t1())
    if (i == 2) return (get_orc_decla_t2())
    if (i == 3) return (get_orc_decla_t3())
    if (i == 4) return (get_orc_decla_t4())
    if (i == 5) return (get_orc_decla_t5())
    if (i == 6) return (get_orc_decla_t6())
    if (i == 7) return (get_orc_decla_t7())
    if (i == 8) return (get_orc_decla_t8())
    if (i == 9) return (get_orc_decla_t9())
    if (i == 10) return (get_orc_decla_t10())
    
    base::stop("orc_decla_t", i, " does not exist !")
}

#### ENVOI BCE ####

get_envoi_bce <- function(i, ref_date, ra, oa)
{
    if (i < 2 || i > 10)
    {
        base::stop("orc_decla_t", i, " does not exist !")
    }
    
    orc_decla <- get_orc_decla(i)
    
    table <- SQL_Table(stringr::str_c("envoi_bce_t", i), orc_decla$get_columns(), "uniqueness_key")
    
    if (table$has_column("entty_id"))
    {
        table$rename("entty_id", "entty_riad_cd")
        table$create_column("entty_id", "regexp_extract(uniqueness_key, '.*\"ENTTY_ID\":\"([^\"]+)\".*', 1)")
    }
    
    table$filter("uniqueness_key IS NOT NULL")
    
    table$drop("uniqueness_key")
    
    table$set_keys(orc_decla$get_keys())
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_envoi_bce_t2 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_envoi_bce(2, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("cmmtmnt_incptn", "fv_chng_cr_bfr_prchs", "intrst_rt_cp", "intrst_rt_flr", "intrst_rt_sprd"))
    
    return (table)
}

get_envoi_bce_t3 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_envoi_bce(3, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("accrd_intrst", "annlsd_agrd_rt", "arrrs", "off_blnc_sht_amnt", "otstndng_nmnl_amnt", "trnsfrrd_amnt"))
    
    return (table)
}

get_envoi_bce_t4 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_envoi_bce(4, ref_date, ra, oa)
    
    return (table)
}

get_envoi_bce_t5 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_envoi_bce(5, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("jnt_lblty_amnt"))
    
    return (table)
}

get_envoi_bce_t6 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_envoi_bce(6, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("accmltd_chngs_fv_cr", "accmltd_imprmnt", "accmltd_wrtffs", "cmltv_rcvrs_snc_dflt", "crryng_amnt", "prvsns_off_blnc_sht"))
    
    return (table)
}

get_envoi_bce_t7 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_envoi_bce(7, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("orgnl_prtctn_vl", "prtctn_vl"))
    
    return (table)
}

get_envoi_bce_t8 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_envoi_bce(8, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("prtctn_allctd_vl", "thrd_prty_prrty_clms"))
    
    return (table)
}

get_envoi_bce_t9 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_envoi_bce(9, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("pd"))
    
    return (table)
}

get_envoi_bce_t10 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_envoi_bce(10, ref_date, ra, oa)
    
    return (table)
}

#### SECURE DESKTOP ####

get_d_entty_by_dt_rfrnc <- function(dt_rfrnc = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.d_entty_by_dt_rfrnc",
        "entty_acr_id",
        "dt_rfrnc",
        "entty_riad_cd",
        "lei",
        "hd_offc_undrt_id",
        "immdt_prnt_undrt_id",
        "ultmt_prnt_undrt_id",
        "nm_entty",
        "strt",
        "cty",
        "trrtrl_unt",
        "pstl_cd",
        "cntry",
        "lgl_frm",
        "instttnl_sctr",
        "ecnmc_actvty",
        "lgl_prcdng_stts",
        "dt_inttn_lgl_prcdngs",
        "entrprs_sz",
        "dt_entrprs_sz",
        "nmbr_emplys",
        "blnc_sht_ttl",
        "annl_trnvr",
        "accntng_frmwrk_sl",
        
        # En plus par rapport à la table orc_decla_t1
        "legalsetup"
    )
    
    table$set_keys("entty_acr_id", "dt_rfrnc")
    
    table$rename("entty_acr_id", "entty_id")
    table$rename("accntng_frmwrk_sl", "accntg_stndrd")
    table$rename("cntry", "addr_cntry")
    table$rename("cty", "addr_cty")
    table$rename("trrtrl_unt", "addr_dpt")
    table$rename("pstl_cd", "addr_pstcd")
    table$rename("annl_trnvr", "anl_trnovr")
    table$rename("blnc_sht_ttl", "blnc_sht_tl")
    table$rename("dt_entrprs_sz", "dt_entrprse_sze")
    table$rename("dt_inttn_lgl_prcdngs", "dt_intlgl_prcdng")
    table$rename("entrprs_sz", "entrprse_sze")
    table$rename("hd_offc_undrt_id", "hd_off_undrtkg_id")
    table$rename("immdt_prnt_undrt_id", "immdt_prt_undrtkg_id")
    table$rename("instttnl_sctr", "insttnl_sctr")
    table$rename("lei", "lgl_entty_id")
    table$rename("nm_entty", "name")
    table$rename("nmbr_emplys", "numbr_emply")
    table$rename("lgl_prcdng_stts", "sts_lgl_prcdng")
    table$rename("ultmt_prnt_undrt_id", "ult_prt_undrtkg_id")
    table$rename("strt", "addr_strt")
    
    # Colonnes manquantes dans orc_decla_t1
    # "ra",
    # "oa",
    # "actif",
    # "code_amf",
    # "code_opc",
    # "code_ot",
    # "org_int",
    # "ntnl_id",
    # "typ_ntnl_id",
    # "descr_other"
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_intlgl_prcdng")
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_entrprse_sze")
    
    return (table)
}

get_v_instrmnt_by_snpsht <- function(dt_rfrnc = "", obsrvd_agnt_acr_id = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.v_instrmnt_by_snpsht",
        "obsrvd_agnt_acr_id",
        "dt_rfrnc",
        "cntrct_id",
        "instrmnt_id",
        "typ_instrmnt",
        "typ_amrtstn",
        "crrncy_dnmntn",
        "fdcry",
        "dt_incptn",
        "dt_end_intrst_only",
        "intrst_rt_cp",
        "intrst_rt_flr",
        "intrst_rt_rst_frqncy",
        "intrst_rt_sprd",
        "typ_intrst_rt",
        "dt_lgl_fnl_mtrty",
        "cmmtmnt_incptn",
        "pymnt_frqncy",
        "prjct_fnnc_ln",
        "prps",
        "rcrs",
        "rfrnc_rt",
        "dt_sttlmnt",
        "sbrdntd_dbt",
        "syndctd_cntrct_id",
        "rpymnt_rghts",
        "fv_chng_cr_bfr_prchs",
        
        "snpsht"
    )
    
    table$set_keys("snpsht", "obsrvd_agnt_acr_id", "dt_rfrnc", "cntrct_id", "instrmnt_id")
    
    table$drop_key("snpsht", 1)
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_obsrvd_agnt_acr_id_if_not_empty(table, obsrvd_agnt_acr_id)
    
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_incptn")
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_end_intrst_only")
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_lgl_fnl_mtrty")
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_sttlmnt")
    
    cast_all_as_string(table, c("rfrnc_rt", "intrst_rt_rst_frqncy", "typ_intrst_rt", "rpymnt_rghts"))
    
    return (table)
}

get_v_fnncl_by_snpsht <- function(dt_rfrnc = "", obsrvd_agnt_acr_id = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.v_fnncl_by_snpsht",
        "obsrvd_agnt_acr_id",
        "dt_rfrnc",
        "cntrct_id",
        "instrmnt_id",
        "annlsd_agrd_rt",
        "dt_nxt_intrst_rt_rst",
        "dflt_stts",
        "dt_dflt_stts",
        "trnsfrrd_amnt",
        "arrrs",
        "dt_pst_d",
        "typ_scrtstn",
        "otstndng_nmnl_amnt",
        "accrd_intrst",
        "off_blnc_sht_amnt",
        
        "snpsht"
    )
    
    table$set_keys("snpsht", "obsrvd_agnt_acr_id", "dt_rfrnc", "cntrct_id", "instrmnt_id")
    
    table$drop_key("snpsht", 1)
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_obsrvd_agnt_acr_id_if_not_empty(table, obsrvd_agnt_acr_id)
    
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_dflt_stts")
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_nxt_intrst_rt_rst")
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_pst_d")
    
    cast_all_as_string(table, c("dflt_stts"))
    
    return (table)
}

get_v_entty_instrmnt_by_snpsht <- function(dt_rfrnc = "", obsrvd_agnt_acr_id = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.v_entty_instrmnt_by_snpsht",
        "obsrvd_agnt_acr_id",
        "dt_rfrnc",
        "entty_acr_id",
        "cntrct_id",
        "instrmnt_id",
        "entty_rl",
        "jnt_lblty_amnt",
        
        "snpsht"
    )
    
    table$set_keys("snpsht", "obsrvd_agnt_acr_id", "dt_rfrnc", "entty_acr_id", "cntrct_id", "instrmnt_id", "entty_rl")
    
    table$drop_key("snpsht", 1)
    
    table$rename("entty_acr_id", "entty_id")
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_obsrvd_agnt_acr_id_if_not_empty(table, obsrvd_agnt_acr_id)
    
    return (table)
}

get_v_accntng_by_snpsht <- function(dt_rfrnc = "", obsrvd_agnt_acr_id = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.v_accntng_by_snpsht",
        "obsrvd_agnt_acr_id",
        "dt_rfrnc",
        "cntrct_id",
        "instrmnt_id",
        "accntng_clssfctn",
        "rcgntn_stts",
        "accmltd_wrtffs",
        "accmltd_imprmnt",
        "imprmnt_stts",
        "imprmnt_assssmnt_mthd",
        "src_encmbrnc",
        "accmltd_chngs_fv_cr",
        "prfrmng_stts",
        "dt_prfrmng_stts",
        "prvsns_off_blnc_sht",
        "frbrnc_stts",
        "dt_frbrnc_stts",
        "cmltv_rcvrs_snc_dflt",
        "prdntl_prtfl",
        "crryng_amnt",
        
        "snpsht"
    )
    
    table$set_keys("snpsht", "obsrvd_agnt_acr_id", "dt_rfrnc", "cntrct_id", "instrmnt_id")
    
    table$drop_key("snpsht", 1)
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_obsrvd_agnt_acr_id_if_not_empty(table, obsrvd_agnt_acr_id)
    
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_prfrmng_stts")
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_frbrnc_stts")
    
    cast_all_as_string(table, c("src_encmbrnc", "prfrmng_stts", "imprmnt_stts",
                            "imprmnt_assssmnt_mthd", "frbrnc_stts",
                            "rcgntn_stts", "prdntl_prtfl", "accntng_clssfctn"))
    
    return (table)
}

get_v_prtctn_rcvd_by_snpsht <- function(dt_rfrnc = "", obsrvd_agnt_acr_id = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.v_prtctn_rcvd_by_snpsht",
        "obsrvd_agnt_acr_id",
        "dt_rfrnc",
        "prtctn_id",
        "prtctn_prvdr_acr_id",
        "typ_prtctn",
        "prtctn_vl",
        "typ_prtctn_vl",
        "prtctn_vltn_apprch",
        "rl_estt_clltrl_lctn",
        "dt_prtctn_vl",
        "dt_mtrty_prtctn",
        "orgnl_prtctn_vl",
        "dt_orgnl_prtctn_vl",
        
        "snpsht"
    )
    
    table$set_keys("snpsht", "obsrvd_agnt_acr_id", "dt_rfrnc", "prtctn_id")
    
    table$drop_key("snpsht", 1)
    
    table$rename("prtctn_prvdr_acr_id", "prtctn_prvdr_cd")
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_obsrvd_agnt_acr_id_if_not_empty(table, obsrvd_agnt_acr_id)
    
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_prtctn_vl")
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_mtrty_prtctn")
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_orgnl_prtctn_vl")
    
    return (table)
}

get_v_instrmnt_prtc_rcvd_by_snpsht <- function(dt_rfrnc = "", obsrvd_agnt_acr_id = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.v_instrmnt_prtc_rcvd_by_snpsht",
        "obsrvd_agnt_acr_id",
        "dt_rfrnc",
        "cntrct_id",
        "instrmnt_id",
        "prtctn_id",
        "prtctn_allctd_vl",
        "thrd_prty_prrty_clms",
        
        "snpsht"
    )
    
    table$set_keys("snpsht", "obsrvd_agnt_acr_id", "dt_rfrnc", "cntrct_id", "instrmnt_id", "prtctn_id")
    
    table$drop_key("snpsht", 1)
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_obsrvd_agnt_acr_id_if_not_empty(table, obsrvd_agnt_acr_id)
    
    return (table)
}

get_v_entty_rsk_by_snpsht <- function(dt_rfrnc = "", obsrvd_agnt_acr_id = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.v_entty_rsk_by_snpsht",
        "obsrvd_agnt_acr_id",
        "dt_rfrnc",
        "entty_acr_id",
        "pd",
        "dflt_stts",
        "dt_dflt_stts",
        
        "snpsht"
    )
    
    table$set_keys("snpsht", "obsrvd_agnt_acr_id", "dt_rfrnc", "entty_acr_id")
    
    table$drop_key("snpsht", 1)
    
    table$rename("entty_acr_id", "entty_id")
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_obsrvd_agnt_acr_id_if_not_empty(table, obsrvd_agnt_acr_id)
    
    format_date_from_yyyymmdd_to_yyyy_mm_dd(table, "dt_dflt_stts")
    
    cast_all_as_string(table, c("dflt_stts"))
    
    return (table)
}

get_v_oa_by_dt_rfrnc_typ_rprtng <- function(dt_rfrnc = "", obsrvd_agnt_acr_id = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.v_oa_by_dt_rfrnc_typ_rprtng",
        "snpsht",
        "dt_rfrnc",
        "obsrvd_agnt_acr_id",
        "obsrvd_agnt_riad_cd",
        "nm_entty",
        "cntry",
        "ra_cd",
        "ra_nm",
        "ra_cntry",
        "typ_rprtng",
        "stts",
        "ddln_typ",
        "ancrdt_rprtng_t1",
        "ancrdt_rprtng_t2",
        "expctd_rprtng"
    )
    
    table$filter("(snpsht = 1 OR snpsht IS NULL)")
    table$drop("snpsht")
    table$set_keys("dt_rfrnc", "obsrvd_agnt_acr_id", "typ_rprtng")
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_obsrvd_agnt_acr_id_if_not_empty(table, obsrvd_agnt_acr_id)
    
    return (table)
}

get_d_entty_cd_by_dt_rfrnc <- function(dt_rfrnc = "", entty_acr_id = "", typ_entty_cd = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.d_entty_cd_by_dt_rfrnc",
        "entty_acr_id",
        "entty_riad_id",
        "typ_entty_cd",
        "entty_cd",
        "dt_rfrnc"
    )
    
    table$set_keys("dt_rfrnc", "entty_acr_id", "typ_entty_cd")
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_dt_rfrnc_if_not_empty(table, entty_acr_id)
    filter_dt_rfrnc_if_not_empty(table, typ_entty_cd)
    
    return (table)
}

get_d_entty_cd_othr_by_dt_rfrnc <- function(dt_rfrnc = "", entty_acr_id = "", typ_entty_cd_othr = "")
{
    table <- SQL_Table(
        "ap_anacredit_core.d_entty_cd_othr_by_dt_rfrnc",
        "entty_acr_id",
        "entty_riad_id",
        "typ_entty_cd_othr",
        "entty_cd_othr",
        "typ_entty_cd_othr_dscrptn",
        "dt_rfrnc"
    )
    
    table$set_keys("dt_rfrnc", "entty_acr_id", "typ_entty_cd_othr")
    
    filter_dt_rfrnc_if_not_empty(table, dt_rfrnc)
    filter_dt_rfrnc_if_not_empty(table, entty_acr_id)
    filter_dt_rfrnc_if_not_empty(table, typ_entty_cd_othr)
    
    return (table)
}

#### BAC-A-SABLE ####

get_bac_a_sable <- function(i, ref_date, ra, oa)
{
    if (i < 2 || i > 10)
    {
        base::stop("orc_decla_t", i, " does not exist !")
    }
    
    orc_decla <- get_orc_decla(i)
    
    table <- SQL_Table(stringr::str_c("anacredit_muses_layer.orc_envoi_bce_t", i), orc_decla$get_columns(), "uniqueness_key")
    
    if (table$has_column("entty_id"))
    {
        table$rename("entty_id", "entty_riad_cd")
        table$create_column("entty_id", "regexp_extract(uniqueness_key, '.*\"ENTTY_ID\":\"([^\"]+)\".*', 1)")
    }
    
    table$filter("uniqueness_key IS NOT NULL")
    
    table$drop("uniqueness_key")
    
    table$set_keys(orc_decla$get_keys())
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_bac_a_sable_t2 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_bac_a_sable(2, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("cmmtmnt_incptn", "fv_chng_cr_bfr_prchs", "intrst_rt_cp", "intrst_rt_flr", "intrst_rt_sprd"))
    
    return (table)
}

get_bac_a_sable_t3 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_bac_a_sable(3, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("accrd_intrst", "annlsd_agrd_rt", "arrrs", "off_blnc_sht_amnt", "otstndng_nmnl_amnt", "trnsfrrd_amnt"))
    
    return (table)
}

get_bac_a_sable_t4 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_bac_a_sable(4, ref_date, ra, oa)
    
    return (table)
}

get_bac_a_sable_t5 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_bac_a_sable(5, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("jnt_lblty_amnt"))
    
    return (table)
}

get_bac_a_sable_t6 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_bac_a_sable(6, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("accmltd_chngs_fv_cr", "accmltd_imprmnt", "accmltd_wrtffs", "cmltv_rcvrs_snc_dflt", "crryng_amnt", "prvsns_off_blnc_sht"))
    
    return (table)
}

get_bac_a_sable_t7 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_bac_a_sable(7, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("orgnl_prtctn_vl", "prtctn_vl"))
    
    return (table)
}

get_bac_a_sable_t8 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_bac_a_sable(8, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("prtctn_allctd_vl", "thrd_prty_prrty_clms"))
    
    return (table)
}

get_bac_a_sable_t9 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_bac_a_sable(9, ref_date, ra, oa)
    
    cast_all_as_decimal(table, c("pd"))
    
    return (table)
}

get_bac_a_sable_t10 <- function(ref_date = "", ra = "", oa = "")
{
    table <- get_bac_a_sable(10, ref_date, ra, oa)
    
    return (table)
}

get_bac_a_sable_golden_record_t1 <- function()
{
    table <- SQL_Table(
        "anacredit_muses_layer.golden_record_t1",
        "code_riad",
        "lei",
        "code_opc",
        "code_ot",
        "code_amf",
        "code_org_int",
        "identifiant_head_office",
        "identifiant_immediate_parent",
        "identifiant_ultimate_parent",
        "denomination",
        "code_postal",
        "ville",
        "code_nuts",
        "pays",
        "code_secteur",
        "code_nace",
        "taille_entreprise",
        "forme_juridique",
        "date_taille_entreprise",
        "nombre_employes",
        "total_bilan",
        "chiffre_affaires",
        "statut_procedure_judiciaire",
        "date_statut_procedure_judiciaire",
        "norme_comptable_indiv"
    )$
        rename("code_riad", "entty_riad_cd")$
        rename("lei", "lgl_entty_id")$
        rename("code_org_int", "org_int")$
        rename("identifiant_head_office", "hd_off_undrtkg_id")$
        rename("identifiant_immediate_parent", "immdt_prt_undrtkg_id")$
        rename("identifiant_ultimate_parent", "ult_prt_undrtkg_id")$
        rename("denomination", "name")$
        rename("code_postal", "addr_pstcd")$
        rename("ville", "addr_cty")$
        rename("code_nuts", "addr_dpt")$
        rename("pays", "addr_cntry")$
        rename("code_secteur", "insttnl_sctr")$
        rename("code_nace", "ecnmc_actvty")$
        rename("taille_entreprise", "entrprse_sze")$
        rename("forme_juridique", "lgl_frm")$
        rename("date_taille_entreprise", "dt_entrprse_sze")$
        rename("nombre_employes", "numbr_emply")$
        rename("total_bilan", "blnc_sht_tl")$
        rename("chiffre_affaires", "anl_trnovr")$
        rename("statut_procedure_judiciaire", "sts_lgl_prcdng")$
        rename("date_statut_procedure_judiciaire", "dt_intlgl_prcdng")$
        rename("norme_comptable_indiv", "accntg_stndrd")
    
    table$filter("entty_riad_cd IS NOT NULL")
    table$set_keys("entty_riad_cd")
    
    format_date_from_dd_mm_yyyy_to_yyyy_mm_dd(table, "dt_intlgl_prcdng")
    
    return (table)
}

#### BAC-A-SABLE (TABLES INTERMEDIAIRES) ####

get_bac_a_sable_anacrdt_instrmnt <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "anacredit_muses_layer.anacrdt_instrmnt",
        "ref_date",
        "ra",
        "oa",
        "cntrct_id",
        "instrmnt_id",
        "cmmtmnt_incptn",
        "crrncy_dnmntn",
        "dt_end_intrst_only",
        "dt_incptn",
        "dt_lgl_fnl_mtrty",
        "dt_sttlmnt",
        "fdcry",
        "fv_chng_cr_bfr_prchs",
        "intrst_rt_cp",
        "intrst_rt_flr",
        "intrst_rt_rst_frqncy",
        "intrst_rt_sprd",
        "prjct_fnnc_ln",
        "prps",
        "pymnt_frqncy",
        "rcrs",
        "rfrnc_rt",
        "rpymnt_rghts",
        "sbrdntd_dbt",
        "syndctd_cntrct_id",
        "typ_amrtstn",
        "typ_instrmnt",
        "typ_intrst_rt",
        "ticket_id",
        "accrd_intrst",
        "annlsd_agrd_rt",
        "arrrs",
        "dflt_stts",
        "dt_dflt_stts",
        "dt_nxt_intrst_rt_rst",
        "dt_pst_d",
        "off_blnc_sht_amnt",
        "otstndng_nmnl_amnt",
        "trnsfrrd_amnt",
        "typ_scrtstn",
        "accmltd_chngs_fv_cr",
        "accmltd_imprmnt",
        "accmltd_wrtffs",
        "accntng_clssfctn",
        "cmltv_rcvrs_snc_dflt",
        "crryng_amnt",
        "dt_frbrnc_stts",
        "dt_prfrmng_stts",
        "frbrnc_stts",
        "imprmnt_assssmnt_mthd",
        "imprmnt_stts",
        "prdntl_prtfl",
        "prfrmng_stts",
        "prvsns_off_blnc_sht",
        "rcgntn_stts",
        "src_encmbrnc"
    )
    
    table$set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_bac_a_sable_creditors <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "anacredit_muses_layer.creditors",
        "ref_date",
        "ra",
        "oa",
        "cntrct_id",
        "instrmnt_id",
        "cre_entty_id",
        "cre_entty_riad_cd",
        "cre_lgl_entty_id",
        "cre_code_opc",
        "cre_code_ot",
        "cre_code_amf",
        "cre_org_int",
        "cre_hd_off_undrtkg_id",
        "cre_immdt_prt_undrtkg_id",
        "cre_ult_prt_undrtkg_id",
        "cre_name",
        "cre_addr_pstcd",
        "cre_addr_cty",
        "cre_addr_dpt",
        "cre_addr_cntry",
        "cre_lgl_frm",
        "cre_insttnl_sctr",
        "cre_ecnmc_actvty",
        "cre_entrprse_sze",
        "cre_dt_entrprse_sze",
        "cre_numbr_emply",
        "cre_blnc_sht_tl",
        "cre_anl_trnovr",
        "cre_sts_lgl_prcdng",
        "cre_dt_intlgl_prcdng",
        "cre_accntg_stndrd",
        "number_of_creditors",
        "cre_pd",
        "cre_dflt_stts",
        "cre_dt_dflt_stts"
    )
    
    table$set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id", "cre_entty_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_bac_a_sable_debtors <- function(ref_date = "", ra = "", oa = "")
{
    table <- SQL_Table(
        "anacredit_muses_layer.debtors",
        "ref_date",
        "ra",
        "oa",
        "cntrct_id",
        "instrmnt_id",
        "deb_entty_id",
        "deb_entty_riad_cd",
        "deb_lgl_entty_id",
        "deb_code_opc",
        "deb_code_ot",
        "deb_code_amf",
        "deb_org_int",
        "deb_hd_off_undrtkg_id",
        "deb_immdt_prt_undrtkg_id",
        "deb_ult_prt_undrtkg_id",
        "deb_name",
        "deb_addr_pstcd",
        "deb_addr_cty",
        "deb_addr_dpt",
        "deb_addr_cntry",
        "deb_lgl_frm",
        "deb_insttnl_sctr",
        "deb_ecnmc_actvty",
        "deb_entrprse_sze",
        "deb_dt_entrprse_sze",
        "deb_numbr_emply",
        "deb_blnc_sht_tl",
        "deb_anl_trnovr",
        "deb_sts_lgl_prcdng",
        "deb_dt_intlgl_prcdng",
        "deb_accntg_stndrd",
        "deb_jnt_lblty_amnt",
        "number_of_debtors",
        "total_deb_jnt_lblty_amnt",
        "main_debtor",
        "number_of_main_debtors",
        "deb_pd",
        "deb_dflt_stts",
        "deb_dt_dflt_stts"
    )
    
    table$set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id", "deb_entty_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}

get_bac_a_sable_anacrdt_full <- function(ref_date = "", ra = "", oa = "")
{
    columns <- c(
        
        get_bac_a_sable_anacrdt_instrmnt()$get_columns(),
        get_bac_a_sable_creditors()$get_columns(),
        get_bac_a_sable_debtors()$get_columns(),
        
        "obsrvd_agnt_country",
        "pge",
        "partially_transferred_flag",
        "partial_transfer_applicable",
        "impairment_flag",
        "fair_value_flag",
        "acquisition_price_flag",
        "pro_rata_debtor_share",
        "main_debtor_share",
        "creditor_share",
        "annlsd_agrd_rt_type",
        "non_fiduciary_flag",
        "recognition_flag",
        "intra_company_instrument_flag",
        "non_tradi_securitised_flag",
        "settled_flag",
        "prfrmng_stts_s",
        "bad_loan_flag",
        "maturity_in_months",
        "new_business",
        "amount_category",
        "deb_insttnl_sctr_s",
        "cre_insttnl_sctr_s",
        "deb_addr_cntry_s",
        "cre_addr_cntry_s",
        "deb_ecnmc_actvty_s",
        "deb_entrprse_sze_s",
        "crrncy_dnmntn_s",
        "aggregable_amount",
        "bsi_instrument",
        "mir_instrument",
        "maturity",
        "trnsfrrd_amnt_a",
        "arrrs_a",
        "otstndng_nmnl_amnt_a",
        "accrd_intrst_a",
        "off_blnc_sht_amnt_a",
        "encours",
        "annlsd_agrd_rt_weight",
        "annlsd_agrd_rt_divided_by_100",
        "annlsd_agrd_rt_a"
    )
    
    columns <- base::setdiff(columns, "ticket_id")
    
    table <- SQL_Table("anacredit_muses_layer.anacrdt_full", columns)
    
    table$set_keys("ref_date", "ra", "oa", "cntrct_id", "instrmnt_id", "cre_entty_id", "deb_entty_id")
    
    filter_ref_date_if_not_empty(table, ref_date)
    filter_ra_if_not_empty(table, ra)
    filter_oa_if_not_empty(table, oa)
    
    return (table)
}
