
library(glue)
library(fs)
library(dplyr)
library(jsonlite)
library(httr)



update_aos_files <- function(box_dir, data_id, mysite, myglob){
# current files
  current_files <- fs::dir_ls(glue::glue('{box_dir}/{mysite}'), glob = glue::glue("*{myglob}*"))
  months_have <- basename(current_files) %>% 
    stringr::str_split(pattern = '\\.', simplify = FALSE) %>%
    purrr::map_chr(~.x[[8]])
  
  base_url <- 'http://data.neonscience.org/api/v0/'
  req_avail <- httr::GET(glue('{base_url}/products/{data_id}'))
  avail_resp <- httr::content(req_avail, as = 'text') %>% 
    jsonlite::fromJSON(simplifyDataFrame = TRUE, flatten = TRUE)

  # List of products by site code with month
  data_urls_list <- avail_resp$data$siteCodes$availableDataUrls
  # make table of urls with site and months
  avail_df <- data_urls_list %>% unlist() %>% as.data.frame() %>% dplyr::rename(url = 1) 
  # %>%
  avail_df$siteid = avail_df$url %>% stringr::str_split(pattern = "/") %>% purrr::map_chr(~.x[[8]])
  avail_df$month = avail_df$url %>% stringr::str_split(pattern = "/") %>% purrr::map_chr(~.x[[9]])
  avail_df <- avail_df %>% dplyr::select(siteid, month, url)

  my_site_to_get <- avail_df %>% 
    dplyr::filter(siteid == mysite) %>%
    dplyr::filter(!month %in% months_have) %>%
    dplyr::filter(as.numeric(substr(month, 1, 4)) > 2011)

  my_site_urls <- my_site_to_get %>% dplyr::pull(url)

# filter to just the my glob basic files
  # my_url <- my_site_urls[19] # for testing
  get_pattern_files <- function(my_url){
    data_files_req <- httr::GET(my_url)
    data_files <- httr::content(data_files_req, as = "text") %>%
      jsonlite::fromJSON(simplifyDataFrame = TRUE, flatten = TRUE)
    data_files_df <- data_files$data$files %>% 
      dplyr::filter(stringr::str_detect(name, glue::glue('{myglob}.*(basic)')))
  # filter(str_detect(name, "(externalLabData).*(basic)"))
  # future enhancement: check md5 sums for changes! 
    return_list <- NULL
    if(nrow(data_files_df) > 0){
      return_list <- list(files = data_files_df$name, urls = data_files_df$url)}
    return(return_list)
    }

  my_files_list <- my_site_urls %>% purrr::map(~get_pattern_files(.x))

  new_files <- my_files_list %>% purrr::map_lgl(~!is.null(.x))
  any_new <- any(new_files)
  if(!any_new){message(glue::glue('No new {myglob} data from {mysite}'))}
  months_newfiles <- my_site_to_get[['month']][which(new_files)]
  download_month <- function(my_files){
    my_files_local <- glue('{box_dir}/{mysite}/{my_files$files}')
    purrr::walk2(.x = my_files$urls, .y = my_files_local, ~download.file(.x, .y))
}
  my_files_list %>% purrr::walk(~download_month(.x))
  if(any_new){message(glue('new {myglob} data from {mysite} for: {glue_collapse(months_newfiles, ", ")}'))}

}
