library(tidyverse)
library(xml2)
library(rvest)

#--------------------------------------------------------
# Function to get the file name as per Date
#--------------------------------------------------------
get_filename <- function(website){
  
  time <- Sys.time()
  date <- substr(time,6,10)
  
  date <- sapply(lapply(strsplit(date, NULL), rev), paste, collapse="")
  
  time <- substr(time,12,16)
  time <- gsub(":","-",time)
  
  filename <- paste(website,paste(date,time,sep = "_"),sep = "_")
  filename <- paste(filename,".csv",sep = "")
  
  return(filename)
}

#--------------------------------------------------------
# Function to web scrape data from The Indian Express Feed
#--------------------------------------------------------

scrape_indianexpress <- function(){
  
  feed <- read_xml("http://indianexpress.com/section/india/feed/")
  
  # helper function to extract information from the item node
  item2vec <- function(item){
    tibble(title = xml_text(xml_find_first(item, "./title")),
           link = xml_text(xml_find_first(item, "./link")),
           pubDate = xml_text(xml_find_first(item, "./pubDate")))
  }
  
  dat <- feed %>% 
    xml_find_all("//item") %>% 
    map_df(item2vec)
  
  # The following takes a while
  dat <- dat %>% 
    mutate(desc = map_chr(dat$link, ~read_html(.) %>% html_node('.synopsis') %>% html_text))
  
  filename <- get_filename("indianexpress")
  filename <- paste("F:/BIG DATA/ISB/Assignments/Term 1/Big Data/Spark-Project/code/web-scraping/indianexpressdata/",filename,sep = "")
  
  write.csv(dat, filename,row.names = F)
}

#--------------------------------------------------------
# Function to web scrape data from The Hindu Feed
#--------------------------------------------------------
  
scrape_hindu <- function(){
  
  feed <- read_xml("http://www.thehindu.com/?service=rss")
  
  # helper function to extract information from the item node
  item2vec <- function(item){
    tibble(title = xml_text(xml_find_first(item, "./title")),
           link = xml_text(xml_find_first(item, "./link")),
           pubDate = xml_text(xml_find_first(item, "./pubDate")),
           desc = xml_text(xml_find_first(item, "./description")) )
  }
  
  dat <- feed %>% 
    xml_find_all("//item") %>% 
    map_df(item2vec)
  
  filename <- get_filename("hindu")
  filename <- paste("F:/BIG DATA/ISB/Assignments/Term 1/Big Data/Spark-Project/code/web-scraping/hindu/",filename,sep = "")
  
  write.csv(dat, filename,row.names = F)
}

scrape_indianexpress()
scrape_hindu()
