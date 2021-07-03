
SQL_Table <- methods::setRefClass(
    
    Class = "SQL_Table",
    
    fields = c(
        table = "ANY"
    ),
    
    methods = base::list(
        
        initialize = function(name, ...)
        {
            columns <- base::as.character(base::unlist(base::list(...)))
            
            table <<- methods::new(SQLTable, name, columns)
        },
        
        get_keys = function(.self)
        {
            return (.self$table$get_keys())
        },
        
        set_keys = function(.self, ...)
        {
            keys <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$set_keys(keys)
            
            base::invisible(.self)
        },
        
        get_columns = function(.self)
        {
            return (.self$table$get_columns())
        },
        
        has_column = function(.self, name)
        {
            return (.self$table$has_column(name))
        },
        
        copy = function(.self)
        {
            other <- SQL_Table("", base::character())
            
            other$table <- .self$table$copy()
            
            return (other)
        },
        
        rename = function(.self, old_name, new_name)
        {
            .self$table$rename(old_name, new_name)
            
            base::invisible(.self)
        },
        
        prefix = function(.self, prefix, ...)
        {
            columns <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$prefix(prefix, columns)
            
            base::invisible(.self)
        },
        
        prefix_all_except = function(.self, prefix, ...)
        {
            exceptions <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$prefix_all_except(prefix, exceptions)
            
            base::invisible(.self)
        },
        
        select = function(.self, ...)
        {
            columns <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$select(columns)
            
            base::invisible(.self)
        },
        
        drop = function(.self, ...)
        {
            columns <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$drop(columns)
            
            base::invisible(.self)
        },
        
        drop_key = function(.self, name, value)
        {
            value <- base::as.character(value)
            
            .self$table$drop_key(name, value)
            
            base::invisible(.self)
        },
        
        filter = function(.self, ...)
        {
            tokens <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$filter(tokens)
            
            base::invisible(.self)
        },
        
        create_column = function(.self, name, ...)
        {
            tokens <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$create_column(name, tokens)
            
            base::invisible(.self)
        },
        
        aggregate = function(.self, variables, by)
        {
            .self$table$aggregate(variables, by)
            
            base::invisible(.self)
        },
        
        inner_join = function(.self, other, ...)
        {
            by <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$inner_join(other$table, by)
            
            base::invisible(.self)
        },
        
        left_join = function(.self, other, ...)
        {
            by <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$left_join(other$table, by)
            
            base::invisible(.self)
        },
        
        right_join = function(.self, other, ...)
        {
            by <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$right_join(other$table, by)
            
            base::invisible(.self)
        },
        
        full_join = function(.self, other, ...)
        {
            by <- base::as.character(base::unlist(base::list(...)))
            
            .self$table$full_join(other$table, by)
            
            base::invisible(.self)
        },
        
        unique = function(.self)
        {
            .self$table$unique()
            
            base::invisible(.self)
        },
        
        stack = function(.self, other)
        {
            .self$table$stack(other$table)
            
            base::invisible(.self)
        },
        
        render = function(.self)
        {
            return (.self$table$render())
        },
        
        print = function(.self)
        {
            .self$table$print()
        },
        
        has_errors = function(.self)
        {
            return (.self$table$has_errors())
        },
        
        display_errors = function(.self)
        {
            .self$table$display_errors()
        },
        
        ignore_latest_error = function(.self)
        {
            .self$table$ignore_latest_error()
            
            base::invisible(.self)
        },
        
        execute = function(.self,
                           force = FALSE, 
                           timer = TRUE,
                           limit = NA,
                           count = FALSE,
                           connexion = NULL,
                           database_id = DATABASE_ID[1])
        {
            if (.self$has_errors() & force == FALSE)
            {
                .self$display_errors()
                
            } else
            {
                query <- .self$render()
                
                table <- sql_execute(query, timer, limit, count, connexion, database_id)
                
                return (table)
            }
            
        }
    )
)
