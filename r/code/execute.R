
DATABASE_ID <- base::list(
    OPTIMIZED_PROD = "SDDC2_ANACRDT_OPTIMIZED_PROD",
    OPTIMIZED_HOMOL = "SDDC2_ANACRDT_OPTIMIZED_HOMOL",
    OPTIMIZED_INT = "SDDC2_ANACRDT_OPTIMIZED_INT",
    MUSES_LAYER_PROD = "SDDC2_ANACREDIT_MUSES_PROD",
    MUSES_LAYER_HOMOL = "SDDC2_ANACRDT_MUSES_LAYER_HOMOL",
    MUSES_LAYER_INT = "SDDC2_ANACRDT_MUSES_LAYER_INT",
    WORK_PROD = "SDDC2_ANACRDT_WORK_PROD",
    WORK_HOMOL = "SDDC2_ANACRDT_WORK_HOMOL",
    WORK_INT = "SDDC2_ANACRDT_WORK_INT"
)

sql_execute <- function(
    query, 
    timer = TRUE,
    limit = NA,
    count = FALSE,
    connexion = NULL,
    database_id = DATABASE_ID[1])
{
    elapsed <- base::proc.time()
    
    query <- sql_limit(query, limit)
    query <- sql_count(query, count)
    table <- sql_run(query, connexion, database_id)
    
    sql_format_output(table)
    
    if (timer)
    {
        elapsed <- base::round((base::proc.time() - elapsed)[3], 1)
        
        base::print(stringr::str_c("Elapsed time : ", elapsed, " s"))
    }
    
    return (table)
}

sql_limit <- function(query, limit)
{
    if (!base::is.na(limit) & base::is.numeric(limit))
    {
        if (get_sgbd() == "ORACLE")
        {
            return (stringr::str_c(query, " OFFSET 0 ROWS FETCH NEXT ", limit, " ROWS ONLY"))
            
        } else
        {
            return (stringr::str_c(query, " LIMIT ", limit))
        }
    }
    
    return (query)
}

sql_count <- function(query, count)
{
    if (!base::is.na(count) & base::is.logical(count))
    {
        if (count)
        {
            return (stringr::str_c("SELECT COUNT(*) FROM (", query, ") AS ", stringr::str_c("A", sql_generate_unique_id())))
        }
    }
    
    return (query)
}

sql_connect <- function(database_id)
{
    if (get_sgbd() == "ORACLE")
    {
        tsn <- "pacr_svc.prd.tns"
        
        driver <- base::paste0("Oracle in OraClient12Home1; dbq=", tsn, ";")
        
        connexion <- DBI::dbConnect(odbc::odbc(), driver = driver)
        
    } else
    {
        if (stringr::str_detect(database_id, "_INT"))
        {
            password_id <- "Windows (Integration)"
            
        } else
        {
            password_id <- "Windows"
        }
        
        connexion <- Connexion::Connect_To_Database(database_id, password_id)
        
        secureDB::sendUpdate(connexion,"SET hive.vectorized.execution.enabled=false")
        
        base::print(stringr::str_c("Connexion à ", database_id))
    }
    
    return (connexion)
}

sql_run <- function(query, connexion, database_id)
{
    connexion_not_provided <- base::is.null(connexion)
    
    if (connexion_not_provided) connexion <- sql_connect(database_id)
    
    success <- tryCatch(
        {
            table <- sql_query(query, connexion)
            
            TRUE
        },
        error = function(e)
        {
            message <- base::as.character(e)
            
            base::message(stringr::str_sub(message, -5000, -1))
            
            FALSE
        },
        finally = 
            {
                if (connexion_not_provided) sql_disconnect(connexion)
            })
    
    if (success)
    {
        return (data.table::as.data.table(table))
        
    } else
    {
        return (data.table::data.table())
    }
}

sql_query <- function(query, connexion)
{
    if (get_sgbd() == "ORACLE")
    {
        return (DBI::dbGetQuery(connexion, query))
        
    } else
    {
        return (secureDB::getQuery(connexion, query))
    }
}

sql_disconnect <- function(connexion)
{
    if (get_sgbd() == "ORACLE")
    {
        DBI::dbDisconnect(connexion)
        
    } else
    {
        Connexion::Disconnect(connexion)
    }
}

sql_format_output <- function(table)
{
    # Supprime les éventuels alias de noms de colonnes
    colonnes <- base::colnames(table)
    colonnes <- stringr::str_replace(colonnes, "^[a-zA-Z_0-9]+\\.", "")
    data.table::setnames(table, colonnes)
    
    # Met tous les noms de colonnes en minuscules
    data.table::setnames(table, stringr::str_to_lower(base::colnames(table)))
}

sql_generate_unique_id <- (function()
{
    id <- 0
    
    return (function()
    {
        id <<- id + 1
        
        return (id)
    })
})()
