
(function()
{
    Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
    
    Rcpp::sourceCpp("code/api.cpp")
    
    include <- function(path)
    {
        base::source(base::paste0("code/", path), encoding = "UTF-8", local = FALSE)
    }
    
    include("api.R")
    include("execute.R")
    include("tables.R")
    include("commons.R")
    include("orc-decla.R")
    include("envoi-bce.R")
    include("secure-desktop.R")
    include("bac-a-sable.R")
    
})()
