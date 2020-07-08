library(dplyr)
library(lubridate)
library(xlsx)
library(stringr)

print("Tws and Finalreport Trade Count")

today_date <- Sys.Date()
today_date <- gsub("-","",today_date)
today_date
report_date <- Sys.Date() - 1
report_date <- gsub("-","",report_date)
report_date
input_dir <- paste0("D:/Qcollector_Data/tws_statments/",today_date,"/")
list.files(input_dir)
output_dir <- paste0("D:/Qcollector_Data/tws_trades_count/",today_date,"/")
dir.create(output_dir)
list.files(output_dir)

pairs_strategies <- read.csv("D:/utility_codes/tws_trade_mapping/input/pair_strategy_currency_timing_refernce.csv",stringsAsFactors = FALSE)
names(pairs_strategies)[1] <- "pair_strategy"
dim(pairs_strategies)
pairs_strategies$pair_strategy

#=================Mapping Close Trades===================
for (j in 1:nrow(pairs_strategies)) {
  
  print(j)
  temp_strategy <- pairs_strategies$pair_strategy[j]
  print(temp_strategy)
  temp_currency <- pairs_strategies$currency[j]
  temp_currency
  temp_timediff <- pairs_strategies$time_diff[j] 
  temp_timediff
  
  statement_file <- grep(paste0(temp_strategy,"_tws_statement"),list.files(input_dir),value = TRUE)
  statement_file
  if (length(statement_file) == 0) {
    print(paste0("file not found for ",temp_strategy," !!"))
    next
  }
  
  statement <-read.csv(paste0(input_dir,statement_file),stringsAsFactors = FALSE)
  dim(statement)
  
  statement2 <- statement %>% 
    dplyr::filter(`Currency` == temp_currency) %>% 
    dplyr::filter(`Symbol` != "") %>% 
    dplyr::filter(`Date.Time` != "") %>% 
    dplyr::mutate(`Date.Time` = as.POSIXct(`Date.Time`,format = "%Y-%m-%d, %H:%M:%S")) %>% 
    dplyr::mutate(`Date.Time` = `Date.Time` + hours(temp_timediff)) %>% 
    dplyr::mutate(`Date.Time` = as.character(`Date.Time`)) %>% 
    dplyr::mutate(`Date.Time` = gsub("-","",`Date.Time`)) %>% 
    dplyr::mutate(`Date.Time` = gsub(" ","\\|",`Date.Time`)) %>% 
    dplyr::mutate(`Quantity` = gsub(",","",`Quantity`)) %>% 
    dplyr::mutate(`Quantity` = as.numeric(`Quantity`)) %>% 
    dplyr::mutate(`Quantity` = as.numeric(`Quantity`)) %>% 
    dplyr::mutate(`Type` = ifelse(`Quantity` >=0,"BUY","SELL")) %>% 
    dplyr::rename(`TradePrice` = `T..Price`) %>% 
    dplyr::rename(`ClosePrice` = `C..Price` ) %>% 
    dplyr::mutate(`TradePrice` = as.numeric(`TradePrice`)) %>% 
    dplyr::mutate(`TradeDate` = substr(`Date.Time`,1,8)) 
  dim(statement2) 
  
  
  statement3 <- statement2 %>% 
    dplyr::group_by(`Symbol`,`TradeDate`,`Type`) %>% 
    dplyr::summarise(`TotalCount` = sum(`Quantity`)) %>% 
    dplyr::group_by(`lookup_symbol` = paste0(`Symbol`,"_",`Type`,"_",`TradeDate`)) %>% 
    dplyr::mutate(`TotalCount` = abs(`TotalCount`)) %>% 
    dplyr::filter(`TradeDate` == report_date)
  colnames(statement3) <- paste0("TWS_",colnames(statement3))
  
  closed_file <- grep(paste0(temp_strategy,"_finalpositionreport_closed"),list.files(input_dir),value = TRUE)
  closed_file
  
  closed_trades <- read.table(paste0(input_dir,closed_file),sep=",",stringsAsFactors = FALSE,header = TRUE)
  dim(closed_trades)
  
  closed_trades2 <- closed_trades %>% 
    dplyr::rename(`ShortOpenPrice` = `shortOpenPrice`) %>% 
    dplyr::rename(`ShortClosePrice` = `shortClosePrice`) %>% 
    dplyr::mutate(`Pair` = gsub("\\[","",`Pair`)) %>% 
    dplyr::mutate(`Pair` = gsub("\\]","",`Pair`)) %>% 
    dplyr::mutate(`LongQuantity` = as.numeric(`LongQuantity`)) %>% 
    dplyr::mutate(`ShortQuantity` = as.numeric(`ShortQuantity`)) %>% 
    dplyr::mutate(`LongQuantity` = round(`LongQuantity`)) %>% 
    dplyr::mutate(`ShortQuantity` = round(`ShortQuantity`)) %>% 
    dplyr::mutate(`LongOpenPrice` = as.numeric(`LongOpenPrice`)) %>% 
    dplyr::mutate(`LongClosePrice` = as.numeric(`LongClosePrice`)) %>% 
    dplyr::mutate(`ShortOpenPrice` = as.numeric(`ShortOpenPrice`)) %>% 
    dplyr::mutate(`ShortClosePrice` = as.numeric(`ShortClosePrice`)) %>% 
    tidyr::separate(`Pair`,c("stock_1","stock_2"),"\\|",remove=FALSE) %>% 
    unique()
  dim(closed_trades2)
  
  closed_trade_df <- data.frame()
  
  for(i in 1:nrow(closed_trades2)){
    
    print(i)
    
    stock_1_open <- closed_trades2$stock_1[i]
    stock_1_open_date <- substr(closed_trades2$TradeOpenTime[i],1,8)
    stock_1_open_quantity <- closed_trades2$LongQuantity[i]
    stock_1_open_price <- closed_trades2$LongOpenPrice[i]
    
    
    stock_1_close <- closed_trades2$stock_1[i]
    stock_1_close_date <- substr(closed_trades2$TradeCloseTime[i],1,8)
    stock_1_close_quantity <- closed_trades2$LongQuantity[i]
    stock_1_close_price <- closed_trades2$LongClosePrice[i]
    
    
    
    stock_2_open <- closed_trades2$stock_2[i]
    stock_2_open_date <- substr(closed_trades2$TradeOpenTime[i],1,8)
    stock_2_open_quantity <- closed_trades2$ShortQuantity[i]
    stock_2_open_price <- closed_trades2$ShortOpenPrice[i]
    
    
    stock_2_close <- closed_trades2$stock_2[i]
    stock_2_close_date <- substr(closed_trades2$TradeCloseTime[i],1,8)
    stock_2_close_quantity <- closed_trades2$ShortQuantity[i]
    stock_2_close_price <- closed_trades2$ShortClosePrice[i]
    
    
    
    stock_1_open_df <- data.frame("Symbol" = stock_1_open,
                                  "TradeDate" = stock_1_open_date,
                                  "Type" = "BUY",
                                  "Quantity" = stock_1_open_quantity,
                                  "TradePrice"  = stock_1_open_price)
    
    
    
    stock_1_close_df <- data.frame("Symbol" = stock_1_close,
                                   "TradeDate" = stock_1_close_date,
                                   "Type" = "SELL",
                                   "Quantity" = stock_1_close_quantity,
                                   "TradePrice"  = stock_1_close_price)
    
    
    stock_2_open_df <- data.frame("Symbol" = stock_2_open,
                                  "TradeDate" = stock_2_open_date,
                                  "Type" = "SELL",
                                  "Quantity" = stock_2_open_quantity,
                                  "TradePrice"  = stock_2_open_price)
    
    
    stock_2_close_df <- data.frame("Symbol" = stock_2_close,
                                   "TradeDate" = stock_2_close_date,
                                   "Type" = "BUY",
                                   "Quantity" = stock_2_close_quantity,
                                   "TradePrice"  = stock_2_close_price)
    
    
    closed_trade_df <- bind_rows(closed_trade_df,
                                 stock_1_open_df,stock_1_close_df,
                                 stock_2_open_df,stock_2_close_df)
    
  }
  
  
  
  
  carryforward_file <- grep(paste0(temp_strategy,"_finalpositionreport_carryforward"),list.files(input_dir),value = TRUE)
  carryforward_file
  
  carryforward_trades <- read.table(paste0(input_dir,carryforward_file),sep=",",stringsAsFactors = FALSE,header = TRUE)
  dim(carryforward_trades)
  
  carryforward_trades2 <- carryforward_trades %>% 
    dplyr::rename(`ShortOpenPrice` = `shortOpenPrice`) %>% 
    dplyr::filter(`StrategyName` == "INDEXBASE") %>% 
    dplyr::mutate(`Pair` = gsub("\\[","",`Pair`)) %>% 
    dplyr::mutate(`Pair` = gsub("\\]","",`Pair`)) %>% 
    dplyr::mutate(`LongQuantity` = as.numeric(`LongQuantity`)) %>% 
    dplyr::mutate(`ShortQuantity` = as.numeric(`ShortQuantity`)) %>% 
    dplyr::mutate(`LongQuantity` = round(`LongQuantity`)) %>% 
    dplyr::mutate(`ShortQuantity` = round(`ShortQuantity`)) %>% 
    dplyr::mutate(`LongOpenPrice` = as.numeric(`LongOpenPrice`)) %>% 
    dplyr::mutate(`ShortOpenPrice` = as.numeric(`ShortOpenPrice`)) %>% 
    tidyr::separate(`Pair`,c("stock_1","stock_2"),"\\|",remove=FALSE) %>% 
    unique()
  dim(carryforward_trades2)
  
  carryforward_trade_df <- data.frame()
  
  for(i in 1:nrow(carryforward_trades2)){
    
    print(i)
    
    stock_1_open <- carryforward_trades2$stock_1[i]
    stock_1_open_date <- substr(carryforward_trades2$TradeOpenTime[i],1,8)
    stock_1_open_quantity <- carryforward_trades2$LongQuantity[i]
    stock_1_open_price <- carryforward_trades2$LongOpenPrice[i]
    
    
    
    
    stock_2_open <- carryforward_trades2$stock_2[i]
    stock_2_open_date <- substr(carryforward_trades2$TradeOpenTime[i],1,8)
    stock_2_open_quantity <- carryforward_trades2$ShortQuantity[i]
    stock_2_open_price <- carryforward_trades2$ShortOpenPrice[i]
    
    
    
    stock_1_open_df <- data.frame("Symbol" = stock_1_open,
                                  "TradeDate" = stock_1_open_date,
                                  "Type" = "BUY",
                                  "Quantity" = stock_1_open_quantity,
                                  "TradePrice"  = stock_1_open_price)
    
    
    
    
    
    stock_2_open_df <- data.frame("Symbol" = stock_2_open,
                                  "TradeDate" = stock_2_open_date,
                                  "Type" = "SELL",
                                  "Quantity" = stock_2_open_quantity,
                                  "TradePrice"  = stock_2_open_price)
    
    
    
    
    carryforward_trade_df <- bind_rows(carryforward_trade_df,
                                       stock_1_open_df,
                                       stock_2_open_df)
    
  }
  
  
  
  all_trades <- bind_rows(closed_trade_df,carryforward_trade_df)
  all_trades2 <- all_trades %>% 
    dplyr::group_by(`Symbol`,`TradeDate`,`Type`) %>% 
    dplyr::summarise(`TotalCount` = sum(`Quantity`)) %>% 
    dplyr::group_by(`lookup_symbol` = paste0(`Symbol`,"_",`Type`,"_",`TradeDate`)) %>% 
    dplyr::filter(`TradeDate` == report_date)
  
  
  final_report <- all_trades2 %>% 
    dplyr::left_join(statement3,by=c("lookup_symbol" = "TWS_lookup_symbol")) %>% 
    dplyr::mutate(`TradeDifference` = `TotalCount` - `TWS_TotalCount`)
  
  
  write.csv(final_report,paste0(output_dir,today_date,"_",temp_strategy,"_trade_count.csv"),row.names = FALSE)
  
}





