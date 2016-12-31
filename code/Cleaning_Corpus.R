
#=======================================================================
#   Reading Hindu Dataset
#=======================================================================

setwd("F:/BIG DATA/ISB/Assignments/Term 1/Big Data/Spark-Project/code/web-scraping/hindu")
file_list_hin <- list.files()
raw_hindudata <- data.frame(NULL)

for (file in file_list_hin){
    temp_dataset <- read.csv(file, stringsAsFactors = F)
    raw_hindudata <- rbind(raw_hindudata, temp_dataset)
    rm(temp_dataset)
}

raw_hindudata <- unique(raw_hindudata)
s <- seq(1, nrow(raw_hindudata), 1)
id <- paste("HIN", s, sep="")
id.df <- data.frame(id)
raw_hindudata <- cbind(id.df, raw_hindudata)
head(raw_hindudata)

#=======================================================================
#   Reading Indian Express Dataset
#=======================================================================

setwd("F:/BIG DATA/ISB/Assignments/Term 1/Big Data/Spark-Project/code/web-scraping/indianexpressdata")
file_list_IE <- list.files()
raw_IEdata <- data.frame(NULL)

for (file in file_list_IE){
  temp_dataset <- read.csv(file, stringsAsFactors = F)
  raw_IEdata <- rbind(raw_IEdata, temp_dataset)
  rm(temp_dataset)
}

raw_IEdata <- unique(raw_IEdata)
s <- seq(1, nrow(raw_IEdata), 1)
id <- paste("IE", s, sep="")
id.df <- data.frame(id)
raw_IEdata <- cbind(id.df,raw_IEdata)

#=======================================================================
#   Cleaning, PreProcessing Function
#=======================================================================

text.clean = function(x)                    # text data
{ require("tm")
  x  =  gsub("<.*?>", " ", x)               # regex for removing HTML tags
  x  =  iconv(x, "latin1", "ASCII", sub="") # Keep only ASCII characters
  x  =  gsub("[^[:alnum:]]", " ", x)        # keep only alpha numeric 
  x  =  tolower(x)                          # convert to lower case characters
  x  =  stripWhitespace(x)                  # removing white space
  x  =  gsub("^\\s+|\\s+$", "", x)          # remove leading and trailing white space
  return(x)
}

#==================================================================================
#   Applying PreProcessing Function on Title and Descriptions of the Two Dataset
#==================================================================================

raw_hindudata$title  = text.clean(raw_hindudata$title)
raw_hindudata$desc  = text.clean(raw_hindudata$desc)

raw_IEdata$title  = text.clean(raw_IEdata$title)
raw_IEdata$desc  = text.clean(raw_IEdata$desc)


#==================================================================================
#   Storing the Cleaned Corpus
#==================================================================================

write.csv(raw_hindudata,"F:/BIG DATA/ISB/Assignments/Term 1/Big Data/Spark-Project/code/web-scraping/hindu_cleaned.csv",
          row.names = F)
write.csv(raw_IEdata,"F:/BIG DATA/ISB/Assignments/Term 1/Big Data/Spark-Project/code/web-scraping/IE_cleaned.csv",
          row.names = F)

