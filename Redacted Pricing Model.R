#---Open Libraries----
script.start <- Sys.time()
open.lib.start <- Sys.time()
oldw <- getOption("warn")
options(warn = -1)
suppressPackagePrice3upMessages(library(data.table))
suppressPackagePrice3upMessages(library(openxlsx))
suppressPackagePrice3upMessages(library(RODBC))
suppressPackagePrice3upMessages(library(Rcpp))
# suppressPackagePrice3upMessages(library(RcppArmadillo))
suppressPackagePrice3upMessages(library(glue))
# suppressPackagePrice3upMessages(library(matrixStats))
source("price_model_Calcs.R")
source("price_model_Fxns.R")
source("price_model_Plot Fxns.R")
options(warn = oldw)
open.lib.end <- Sys.time()
options(scipen = 999)
# options(warn = 2)
setDTthreads(0)
Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe")

# Location for static Excel file
price.model.files <- glue("{getwd()}/InputFiles/ERP_Price_Model_Tables.xlsx")

#---Beginning of Script----
#---Get current price.model version----
v_long <- rstudioapi::getSourceEditorContext()$path
v_long <- substr(v_long, max(unlist(gregexpr("/", v_long)))+1, nchar(v_long))

# commit.info <- getCommitInfo()
# COMMIT: {commit.info$commit}
# COMMITDATE: {commit.info$commitdate}
# COMMITUSER: {commit.info$commituser}

#---Price3 a log file for the day----
Sys.setenv(log.path = glue("{getwd()}/Logs/ERP_Price_Model_Log_{format(Sys.Date(), \"%Y%m%d\")}.txt"))

if(!file.exists(Sys.getenv("log.path"))){
  invisible(file.create(Sys.getenv("log.path")))
  message(glue("Creating log file: {Sys.getenv(\"log.path\")}"))
  cat(glue("BEGIN LOG FOR PRICE MODEL
          VERSION: {v_long}
          LOGDATE: {format(Sys.time(), \"%Y-%m-%d %T UTC%z %Z\")}
          COMMIT: N/A
          COMMITDATE: N/A
          COMMITUSER: N/A
          CURRENTUSER: {as.character(Sys.info()[7])}
          ----
                 
          LOG [{format(script.start, \"%Y-%m-%d %T UTC%z %Z\")}]: Price3ing script
          LOG [{format(open.lib.end, \"%Y-%m-%d %T UTC%z %Z\")}]: Libraries opened
          
          "), file = Sys.getenv("log.path"), append = T)
} else {
  message(glue("Continuing log file: {Sys.getenv(\"log.path\")}"))
  cat(glue("
          COMMIT: N/A
          COMMITDATE: N/A
          COMMITUSER: N/A
          CURRENTUSER: {as.character(Sys.info()[7])}
          LOG [{format(Sys.time(), \"%Y-%m-%d %T UTC%z %Z\")}]: Restarting PRICE MODEL
          LOG [{format(script.start, \"%Y-%m-%d %T UTC%z %Z\")}]: Price3ing script
          LOG [{format(open.lib.end, \"%Y-%m-%d %T UTC%z %Z\")}]: Libraries opened
          
          "), file = Sys.getenv("log.path"), append = T)
}

sourceCpp("cppfunctions.cpp")
logger("C++ sourced")
# logger(glue("Starting Price Model - commit {commit.info$commit}"))
logger(glue("Starting Price Model"))
logger("Writing queries")

#---Write Queries----
#---Gets the row count for sales data. Getting the row count makes for a faster sales query.
cnt.query <- glue("{getwd()}/SQL/Price Model_Sales_Count_Query.sql")
cnt.query <- glue(glue_collapse(readLines(cnt.query), sep = "\n"))

#---Get sales data from DW
r.query <- glue("{getwd()}/SQL/Price Model_Sales_Query.sql")
r.query <- glue(glue_collapse(readLines(r.query), sep = "\n"))

#---Get item master info from DW
ItemMaster.query <- glue("{getwd()}/SQL/Price Model_Item_Query.sql")
ItemMaster.query <- glue(glue_collapse(readLines(ItemMaster.query), sep = "\n"))

#---Get customer info from DW
CustomerMaster.query <- glue("{getwd()}/SQL/Price Model_Customer_Query2.sql")
CustomerMaster.query <- glue(glue_collapse(readLines(CustomerMaster.query), sep = "\n"))

#---Get current Price Model file in PROD
prod.price.model.query <- glue("{getwd()}/SQL/Price Model_Current_File.sql")
prod.price.model.query <- glue(glue_collapse(readLines(prod.price.model.query), sep = "\n"))

#---Get current Price Model file in PROD
prod.price.model.2.query <- glue("{getwd()}/SQL/CurrentPrice Model.sql")
prod.price.model.2.query <- glue(glue_collapse(readLines(prod.price.model.2.query), sep = "\n"))

#---Get UOM Conversions
conv.query <- "
[REDACTED]
"

# #---Get lead account
LeadAcct.query <- "
[REDACTED]
"

#---Open ODBC connections----
lapply(list("SQL_SERVER_1","SQL_SERVER_2"), function(x){
  
  assign(glue("{x}.ch"), open.odbc.channel(x), envir = .GlobalEnv)
  
})

#--Check for errors in cnt----
logger("Price3ing data pulls")
sql.start <- Sys.time()

cnt <- as.numeric(executeSQL(SQL_SERVER_2.ch, cnt.query, "sales row count")[,1])

asis <- rep(F, 39)
asis[c(23, 25, 29, 37)] <- T

r <- executeSQL(SQL_SERVER_2.ch, r.query, query.desc = glue("sales data - {format(cnt, big.mark = \",\")} rows"), as.is = asis, keys = c("CustomerNumber","ProductCode"), buffsize = cnt)

ItemMaster <- executeSQL(SQL_SERVER_1.ch, ItemMaster.query, "item data", c(F,rep(T,13)), keys = c("ProductKey", "Warehouse"))
CustomerMaster <- executeSQL(SQL_SERVER_1.ch, CustomerMaster.query, "customer data", c(F,rep(T,13)), keys = "CustomerNumber")
LeadAccount <- executeSQL(SQL_SERVER_1.ch, LeadAcct.query, "lead account data", keys = "CustomerGroup")
UOM.conv <- executeSQL(SQL_SERVER_1.ch, conv.query, "UOM conversions", c(F, T, T, F))
prod.price.model <- executeSQL(SQL_SERVER_1.ch, prod.price.model.query, query.desc = "current prod file", as.is = c(T,F,F,F), keys = "PricingKey", buffsize = 2e5)
prod.price.model <- prod.price.model[, head(.SD, 1), by = "PricingKey"]

prod.price.model2 <- executeSQL(SQL_SERVER_1.ch, prod.price.model.2.query, query.desc = "current prod file", as.is = c(T,F,F,F), buffsize = 2e5)
prod.price.model2 <- prod.price.model2[, head(.SD, 1),]
# cost.hx <- executeSQL(SQL_SERVER_1.ch, cost.hx.query, query.desc = "cost history", as.is = c(F,F,T,F), buffsize = 3.5e6)

sql.end <- Sys.time()
data.elap <- difftime(sql.end, sql.start)

# ---Complete data pull----
logger(glue("Data pull finished - {round(data.elap, 2)} {units(data.elap)}"))

#---Price3 data prep----
logger("Price3ing data preparation")
logger("Formatting strings")
r[, names(r) := lapply(.SD, stringFormat)]
LeadAccount[, names(LeadAccount) := lapply(.SD, stringFormat)]
r[, CustomerNumber := as.integer(CustomerNumber)]
CustomerMaster[, names(CustomerMaster) := lapply(.SD, stringFormat)]
ItemMaster[, names(ItemMaster) := lapply(.SD, stringFormat)]
UOM.conv[, names(UOM.conv) := lapply(.SD, stringFormat)]

save.image(file.choose())

#---Data Input into DT----
logger("Copying into DT", print.to.console = F)
DT <- copy(r)

DT$ProductKey <- NULL

logger("Formatting DT", print.to.console = F)

# Add Customer group from the customer master
DT <- merge(DT, CustomerMaster[, c("CustomerNumber","Customer","CustomerGroup","CustomerGroupDesc")], by = "CustomerNumber")
gc()
DT <- merge(DT, ItemMaster[, NA, .(ProductKey, ProductCode, ProductDescription)][, -"V1"], by = "ProductCode")
gc()
# Filter the lead account data, getting rid of groups that do not have lead accts
LeadAccount <- LeadAccount[LeadAccount != ""]
LeadAccount[, LeadAccount := as.integer(LeadAccount)]

# Add lead acct to DT
DT <- merge(DT, LeadAccount, by = "CustomerGroup", all.x = T)
gc()
# If there is no lead acct, the lead acct becomes the AN8
DT[is.na(LeadAccount) | LeadAccount == "", LeadAccount := CustomerNumber]

# Add customer attributes by lead acct
DT <- merge(DT, CustomerMaster[, -c("Customer","CustomerGroup","CustomerGroupDesc","Warehouse")], by.x = "LeadAccount", by.y = "CustomerNumber")
gc()
#---Format input data----
logger("Formatting columns")

#---Filter out bad customer data----
DT <- DT[CustomerAttribute2 != ""]
DT <- DT[!is.na(CustomerAttribute2)]
DT <- DT[!CustomerAttribute2 %in% c("B2C","RD","RH","RI","RP","RS")]

DT <- DT[CustomerAttribute3 != ""]
DT <- DT[!is.na(CustomerAttribute3)]

DT[, CustomerAttribute4 := as.character(CustomerAttribute4)]

DT <- DT[ProductAttribute1 %in% 1:4]
DT[, ProductAttribute1 := as.character(ProductAttribute1)]

#---ProductCategory/Subclass filter----
DT <- DT[!ProductCategory %in% c("000","","999")]
DT <- DT[!ProductSubcategory %in% c("000","","999")]

#--CustomerAttribute1 Filter----
#--This filter keeps matching divisions with matching districts
logger("Filtering to proper district")
dt.CustomerAttribute1 <- setDT(read.xlsx(price.model.files, sheet = "Dist"))
dt.CustomerAttribute1[, Warehouse := as.integer(Warehouse)]

DT <- merge(DT, dt.CustomerAttribute1[, c("CustomerAttribute1", "SalesCompany", "Warehouse", "Keep")], by = c("CustomerAttribute1", "SalesCompany", "Warehouse"))
gc()

DT <- DT[Keep == "Y"]

DT[, Keep := NULL]

#---PriceList Filter----
logger("Filtering by Pricelist")
DT <- DT[!(SalesAdjSched %in% trimws(read.xlsx(price.model.files, sheet = "AdjExcl")$PriceList))]

#---Customer filter----
logger("Filtering out Customers")
cust.filter <- c("674734","685311","686356","692254","694410",
            "694516","694655","700701","714901","769151",
            "1067143","1129375","1142537","1144954","1258169",
            "1347671","1418652","1418661","1987548","8307710","30268163")

DT <- DT[!(CustomerNumber %in% cust.filter & left(ProductCode, 3) == "MFM")]

cust.filter <- c("V66990","V87115","M135340",
            "1067143","1129375","1418652")

DT <- DT[!(CustomerGroup %in% cust.filter & left(ProductCode, 3) == "MFM")]

#---Filter out errant invoices----
logger("Filtering out errant invoices")
DT <- DT[!(InvoiceNo %in% read.xlsx(price.model.files, sheet = "ErrantInv")$InvoiceNo)]

#---Remove Deviations----
logger("Removing deviations")
DT <- DT[is.na(Deviation) | Deviation == 0]
DT[, Deviation := NULL]

#---Revenue filter----
DT <- DT[Revenue > 0]

#---Cost filter----
DT <- DT[Cost > 0.02]
DT <- DT[Cost2 > 0.02]

#---PrimaryQty filter----
DT <- DT[PrimaryQty != 0]

#---UnitPrice filter----
DT <- DT[UnitPrice != 0]

#---Filter out nonsense items----
logger("Removing nonsense items")
DT <- DT[!(ProductCode %in% read.xlsx(price.model.files, sheet = "NonsenseItems")$ProductCode)]

#---Remove Upcharge----
logger("Removing upcharge")
DT[, UnitPrice_old := UnitPrice]
DT[, UnitPrice := NULL]

DT[PricingUOM == "KG", c("UnitPrice", "PricingUOM") := .(UnitPrice_old/2.2046001, "LB")]

# If the unit price is the same as the override price, then leave as is
DT[UnitPrice_old == OverridePrice, UnitPrice := UnitPrice_old]

# If the unit price is the same as the pricelock, then leave as is
DT[is.na(UnitPrice) & UnitPrice_old == Pricelock, UnitPrice := UnitPrice_old]

# Otherwise, back out the upcharge value
DT[is.na(UnitPrice), UnitPrice := ifelse(is.na(Upcharge), UnitPrice_old, floor((UnitPrice_old-Upcharge)*10000)/10000)]

DT[, Revenue_old := Revenue]

#---Convert from PrimaryQty to SecondaryQty
DT <- merge(DT, UOM.conv, by.x = c("ProductKey", "PrimaryUOM", "PricingUOM"), by.y = c("ProductKey", "FromUOM", "ToUOM"), all.x = T)
DT <- merge(DT, UOM.conv, by.x = c("ProductKey", "PrimaryUOM", "PricingUOM"), by.y = c("ProductKey", "ToUOM", "FromUOM"), all.x = T, suffixes = c(".From", ".To"))
gc()
# Remove bad UOM conversions
DT <- DT[!(is.na(Factor.From) & is.na(Factor.To) & PricingUOM != PrimaryUOM),]

# If catch weight, Secondary quantity is weight sold
DT[CatchWeight == "Y", SecondaryQty := Weight]

# If both conversion factors are NA, then the PricingUOM = PrimaryUOM and we can take PrimaryQty to be SecondaryQty
DT[is.na(SecondaryQty) & is.na(Factor.From) & is.na(Factor.To), SecondaryQty := PrimaryQty]

# Normal conversion here
DT[is.na(SecondaryQty) & is.na(Factor.From) & !is.na(Factor.To), SecondaryQty := PrimaryQty/Factor.To]
DT[is.na(SecondaryQty) & !is.na(Factor.From) & is.na(Factor.To), SecondaryQty := PrimaryQty*Factor.From]

DT[UnitPrice_old != UnitPrice, Revenue := floor((UnitPrice*SecondaryQty)*100)/100]

# Convert price to Primary UOM
DT[is.na(Factor.From) & is.na(Factor.To), PrimaryUnitPrice := UnitPrice]
DT[is.na(PrimaryUnitPrice) & !is.na(Factor.From) & is.na(Factor.To), PrimaryUnitPrice := UnitPrice*Factor.From]
DT[is.na(PrimaryUnitPrice) & is.na(Factor.From) & !is.na(Factor.To), PrimaryUnitPrice := UnitPrice/Factor.To]

logger("Calculating GP and Margin")
DT[, TGP := Revenue - Cost]      #---True GP
DT[, SC.GP := Revenue - Cost2]  #---Salesman GP

DT[, TMargin := TGP/Revenue]     #---True Cost Margin
DT[, SC.Margin := SC.GP/Revenue] #---Salesman Cost Margin

DT[, Margin := SC.Margin]
DT[, GP := SC.GP]

DT <- DT[is.finite(Margin)]

DT[PrimaryUOM == "CS", Cases := PrimaryQty]

DT[is.na(Cases) & PrimaryUOM == "PC" & PCinCS == 1, Cases := PrimaryQty]

DT[is.na(Cases) & PrimaryUOM == "PC" & PCinCS > 1, Cases := PrimaryQty/PCinCS]

DT[, GP.per.CS := GP/Cases]
DT[, GP.per.Primary := GP/PrimaryQty]
DT[, GP.per.Secondary := GP/SecondaryQty]

#---Division, Class filter---
logger("Calculating & applying filters")
DT <- DT[Margin >= 0.05 & Margin <= 0.85]
DT <- DT[TGP > 0]
DT <- DT[SC.GP > 0]

#---Calculate and Find Non-zProd and zProd for each Division----
zProdItems <- DT[, sum(GP), by = .(ProductCode, SalesCompany, Warehouse)]

#Read in zProd exclusion items
zProdExcl <- setDT(read.xlsx(price.model.files, sheet = "zProdExcl"))

#Remove 4-level zProd items
zProdItems <- zProdItems[!(ProductCode %in% zProdExcl[Record == "4", ProductCode])]

#Remove 5-level zProd items
zProdItems <- merge(zProdItems, zProdExcl[Record == "5"], by = c("SalesCompany", "ProductCode"), all.x = T, suffixes = c("",".y"))
gc()
zProdItems <- zProdItems[is.na(Record)]

zProdItems[, c("Record") := NULL]

#Get only P items
zProdItems <- merge(zProdItems, ItemMaster[, c("Warehouse","ProductCode","ProductDescription","StockingType")], by.x = c("Warehouse", "ProductCode"), by.y = c("Warehouse", "ProductCode"), all.x = T)
gc()
zProdItems <- zProdItems[StockingType == "P"]

# zProdItems[, c("StockingType","Warehouse") := NULL]
zProdItems[, c("StockingType") := NULL]

setnames(zProdItems, c("V1"), c("GP"))

# Get top N for each division
zProdItems <- zProdItems[order(SalesCompany, -GP)]

zProdItems <- rbind(zProdItems[SalesCompany %in% c("QIL","DAR","DDC","VSF","VLA"), head(.SD, 200), by = SalesCompany],
                    zProdItems[!SalesCompany %in% c("QIL","DAR","DDC","VSF","VLA"), head(.SD, 50), by = SalesCompany])

nz <- zProdItems[, .N, SalesCompany]

zProdItems[, SpecialProduct := ProductCode]

#---Introduce zProd column----
logger("Introduce zProd column")
DT <- merge(DT, zProdItems[, c("SpecialProduct","ProductCode","SalesCompany")], by = c("ProductCode","SalesCompany"), all.x = T, all.y = F)
gc()
DT[is.na(SpecialProduct), SpecialProduct := "0"]

#---De-dup zProd Items---
#---A zProd item can rise from C to A in the timeframe, causing duplicate zProds in the output
# DT[SpecialProduct != "0", c("ProductCategory","ProductSubcategory","ProductAttribute1") := NA]
DT[SpecialProduct != "0", c("ProductAttribute1") := NA]

DT[, ID := 1:.N]
gc()

# URCD goes here ----
DT[, URCD := "M"]

#---Compute Price Model---- 
logger("Starting Price Model")
price.model.Cols <- list(c("ProductCategory","ProductSubcategory","ProductAttribute1","SpecialProduct","CustomerAttribute1","CustomerAttribute2","CustomerAttribute3","CustomerAttribute4"))
price.model.start <- Sys.time()

price.model.Output <- data.table::rbindlist(sapply(sapply(1:length(price.model.Cols[[1]]), function(x) price.model.Cols[[1]][1:x]), price.model.qtile2, n.filt = 50, stdev.factor = c(0.25, 0, 0.8)), fill = T)

price.model.end <- Sys.time()

#---Declare col names
stop()
logger("Formatting Price Model")
price.model.Output.0 <- copy(price.model.Output)
# price.model.Output <- copy(price.model.Output.0)

for (p in 8L:4L){
  set(price.model.Output, which(price.model.Output[["Record"]] == p & !price.model.Output[["SpecialProduct"]] %in% c("0", "", NA)), "Priority", (9-p))
}

for (p in 8L:1L){
  set(price.model.Output, which(price.model.Output[["Record"]] == p & price.model.Output[["SpecialProduct"]] %in% c("0", "", NA)), "Priority", (14-p))
}

#---Remove errant 4-records----
# These aren't really "errant", but I want to reserve this scenario for when we need it.
price.model.Output <- price.model.Output[!(Record == 4 & !(SpecialProduct %in% c("0", "", NA)))]

#---Remove errant 1,2,3-records----
price.model.Output <- price.model.Output[!(Record == 3 & (ProductAttribute1 %in% c("0", "", NA)))]
price.model.Output <- price.model.Output[!(Record == 2 & (ProductCategory %in% c("0", "", NA)))]
price.model.Output <- price.model.Output[!(Record == 1 & (ProductCategory %in% c("0", "", NA)))]

#---Round to 6 places----
FTS <- c("Price1","Price2","Price3")

price.model.Output[URCD == "M", (FTS) := lapply(.SD, round2, 6), .SDcols = FTS]
price.model.Output[URCD == "G", (FTS) := lapply(.SD, round2, 4), .SDcols = FTS]

for(i in names(price.model.Output[ , .SD, .SDcols = is.numeric])){
  set(price.model.Output, which(is.na(price.model.Output[[i]])), i, 0)
}

for(i in names(price.model.Output)){
  set(price.model.Output, which(is.na(price.model.Output[[i]])), i, "")
}

# Double check this line to account for URCD
price.model.Output <- price.model.Output[order(-Record)][, head(.SD, 1), by = c(price.model.Cols[[1]], "URCD")]

setkeyv(price.model.Output, c(price.model.Cols[[1]], "URCD"))

logger("Check for incomplete cases")
if(nrow(price.model.Output[!complete.cases(price.model.Output)])){
  logger("Found incomplete cases.", log.type = "ERROR")
}

all.cols <- c("Record","ProductCategory","ProductSubcategory","ProductAttribute1",
              "SpecialProduct","CustomerAttribute1","CustomerAttribute2","CustomerAttribute3",
              "CustomerAttribute4","URCD","Price1","Price2","Price3")

setcolorder(price.model.Output, union(all.cols, names(price.model.Output)))

price.model.Output <- price.model.Output[Price1 < Price3]
price.model.Output <- price.model.Output[Price1 < Price2]
price.model.Output <- price.model.Output[Price2 < Price3]

price.model.Output <- price.model.Output[(URCD == "M" & Price1 > 0.05) | URCD == "G"]
price.model.Output <- price.model.Output[(URCD == "M" & Price2 > 0.05) | URCD == "G"]
price.model.Output <- price.model.Output[(URCD == "M" & Price3 > 0.05) | URCD == "G"]

price.model.Output <- price.model.Output[(URCD == "M" & (Price1 < 0.70 | CustomerAttribute1 == "SLP")) | URCD == "G"]
price.model.Output <- price.model.Output[(URCD == "M" & (Price2 < 0.70 | CustomerAttribute1 == "SLP")) | URCD == "G"]
price.model.Output <- price.model.Output[(URCD == "M" & (Price3 < 0.70 | CustomerAttribute1 == "SLP")) | URCD == "G"]

#---Convert to upload format----
logger("Reading conversion sheets")

#---Need to make sure BDS4_2, D200_2, & SCRADDR_2 don't get coerced to NA_character_, so I chose a superior fruit
dt.ProductCategory <- setDT(read.xlsx(price.model.files, sheet = "IC", na.strings = "coconut"))
dt.ProductSubcategory <- setDT(read.xlsx(price.model.files, sheet = "ISC", na.strings = "coconut"))
dt.CustomerAttribute1 <- setDT(read.xlsx(price.model.files, sheet = "Dist", na.strings = "coconut"))

dt.Theme <- CustomerMaster[, NA, .(CustomerAttribute3, CustomerAttribute3Desc)][, -"V1"]
dt.Type <- CustomerMaster[, NA, .(CustomerAttribute2, CustomerAttribute2Desc)][, -"V1"]

logger("Formatting upload")
upload.cols <- c("C75RNAM","BDS4","BDS4_2","D200","D200_2","FIL2",
                 "OMRSTRNG","SCRADDR","SCRADDR_2","RPDDVLOC",
                 "SCMPTH","CMMENT","DL011","STRVAR","STFU9")

#---Bring in Item Class and Subclass codes----
logger("Formatting item attributes")
price.model.Output.class <- merge(price.model.Output[Record == 1L], dt.ProductCategory[trimws(ProductCategory) != ""], by = "ProductCategory", all = T)
price.model.Output.class <- price.model.Output.class[is.na(Record)]
price.model.Output.class[, c("ProductCategoryDesc", "BDS4_2") := .(NULL)]
price.model.Output.class[, c("Record", "Price1", "Price2", "Price3", "Priority", "URCD") := .(1L, 0.25, 0.30, 0.35, 13, "M")]

price.model.Output <- rbind(price.model.Output, price.model.Output.class)

if(nrow(price.model.Output[Record == 1L]) != nrow(dt.ProductCategory)-1){
  logger("Item Class row count invalid.", log.type = "ERROR")
}

price.model.Output <- merge(price.model.Output, dt.ProductCategory[trimws(ProductCategory) != ""], by = "ProductCategory", all = T)

price.model.Output <- merge(price.model.Output, dt.ProductSubcategory, by = "ProductSubcategory", all.x = T)
price.model.Output <- price.model.Output[!(ProductCategory != SPHD & ProductSubcategory != "") | (Record == 1)]
price.model.Output[, SPHD := NULL]

#---Convert from ABC to 123
price.model.Output[, FIL2 := ProductAttribute1]

#---Convert Customer attributes
logger("Formatting customer attributes")
price.model.Output <- merge(price.model.Output, dt.CustomerAttribute1[, c("CustomerAttribute1", "CustomerAttribute1Desc", "SCRADDR_2")], by = "CustomerAttribute1", all.x = T)
price.model.Output <- merge(price.model.Output, dt.Theme, by = "CustomerAttribute3", all.x = T)
price.model.Output <- merge(price.model.Output, dt.Type, by = "CustomerAttribute2", all.x = T)
gc()

for(i in names(price.model.Output)[sapply(price.model.Output, is.character)]){
  set(price.model.Output,
      which(is.na(price.model.Output[[i]])), i, "")
}

#---Create upload columns----
logger("Create upload columns")
price.model.Output[, (upload.cols) := .(Record, ProductCategory, BDS4_2, ProductSubcategory, D200_2, FIL2, SpecialProduct, CustomerAttribute1, SCRADDR_2, CustomerAttribute2, CustomerAttribute3, CustomerAttribute4, Price1, Price2, Price3)]

#---Bring in Short Item Number----
price.model.Output <- merge(price.model.Output, ItemMaster[, NA, .(ProductCode, ProductKey)][,-"V1"], by.x =  "SpecialProduct", by.y = "ProductCode", all.x = T)
price.model.Output[, ProductKey := as.character(ProductKey)][is.na(ProductKey), ProductKey := "0"]

setcolorder(price.model.Output, union(c("Priority","Record","ProductCategory","ProductCategoryDesc","BDS4","BDS4_2",
                                "ProductSubcategory","ProductSubcategoryDesc","D200","D200_2",
                                "ProductAttribute1","FIL2",
                                "SpecialProduct","OMRSTRNG","ProductKey",
                                "CustomerAttribute1","CustomerAttribute1Desc","SCRADDR","SCRADDR_2",
                                "CustomerAttribute2","CustomerAttribute2Desc","RPDDVLOC",
                                "CustomerAttribute3","CustomerAttribute3Desc","SCMPTH",
                                "CustomerAttribute4","CMMENT"), names(price.model.Output)))

price.model.Output <- price.model.Output[order(Priority)]

price.model.Output2 <- copy(price.model.Output)

#---Strip away unused attributes----
logger("Strip away unused attributes")
price.model.Output[!OMRSTRNG %in% c("0",""), c("BDS4","BDS4_2","D200","D200_2","ProductAttribute1","FIL2") := ""]
price.model.Output[C75RNAM %in% c(1,2,3), ProductKey := ""]

#---Add URRF----
logger("Add URRF")
price.model.Output[, URRF := paste0(C75RNAM, BDS4_2, D200_2, FIL2, ProductKey, SCRADDR_2, RPDDVLOC, SCMPTH, CMMENT, tolower(URCD))]

logger("Check URRF length")
price.model.Output[, URRF.len := nchar(URRF)]

if(any(price.model.Output$URRF.len > 15)){
  logger("Length of URRF exceeds 15", log.type = "ERROR")
}

#---Prepare Upload_Output----
logger("Prepare upload output")
upload.cols <- c("C75RNAM","BDS4","D200","FIL2",
                 "OMRSTRNG","SCRADDR","RPDDVLOC",
                 "SCMPTH","CMMENT","DL011","STRVAR","STFU9","URRF","URCD")

Upload_Output <- price.model.Output[, upload.cols, with = F]

logger("Check for duplicate URRF")
setkeyv(Upload_Output, "URRF")
setkeyv(price.model.Output, "URRF")

if(anyDuplicated(Upload_Output) | anyDuplicated(price.model.Output)){
  logger("Found duplicate URRF", log.type = "ROW ERROR")
}

logger("Check for duplicate URRF")
dup.check.2 <- c("C75RNAM", "BDS4", "D200", "FIL2", "OMRSTRNG", "SCRADDR", "RPDDVLOC", "SCMPTH", "CMMENT")
dup.check.3 <- c("Record", "ProductCategory", "ProductSubcategory", "ProductAttribute1", "SpecialProduct", "CustomerAttribute1", "CustomerAttribute2", "CustomerAttribute3", "CustomerAttribute4")
setkeyv(Upload_Output, dup.check.2)
setkeyv(price.model.Output, dup.check.3)

if(anyDuplicated(Upload_Output) | anyDuplicated(price.model.Output)){
  logger("Found duplicate URRF", log.type = "ROW ERROR")
}

logger("Add columns for upload")
all.cols.upld.1 <- c("UKID","RCTT","EV01","EV02","IGID","APMM",
                     "C75RNAM","BDS4","D200","FIL2",
                     "OMRSTRNG","SCRADDR","RPDDVLOC","SCMPTH",
                     "CMMENT","DL011","STRVAR","STFU9","URCD","URDT",
                     "URAT","URAB","URRF","USER","PID","JOBN","UPMJ","UPMT","EDSP")

Upload_Output[, UKID := format(Sys.time(), "%Y%m%d%H%M32")]
Upload_Output[, RCTT := 1:.N]
Upload_Output[, EV01 := " "]
Upload_Output[, EV02 := " "]
Upload_Output[, IGID := 0L]
Upload_Output[, APMM := " "]
# Upload_Output[, URCD := " "]
Upload_Output[, URDT := 0L]
Upload_Output[, URAT := 0L]
Upload_Output[, URAB := 0L]
Upload_Output[, USER := "INFO"]
Upload_Output[, PID := "INFO"]
Upload_Output[, JOBN := "INFO"]
Upload_Output[, UPMJ := convertToJulian(Sys.Date())]
Upload_Output[, UPMT := 120000L]
Upload_Output[, EDSP := NA_character_]

Upload_Output <- Upload_Output[, head(.SD, 1), URRF]

Upload_Output <- Upload_Output[, all.cols.upld.1, with = F]

setnames(Upload_Output, c("UKID","RCTT","EV01","EV02","IGID","APMM",
                          "Record","ProductCategory","ProductSubcategory","ProductAttribute1",
                          "SpecialProduct","CustomerAttribute1","CustomerAttribute2","CustomerAttribute3",
                          "CustomerAttribute4","Price1","Price2","Price3","URCD","URDT",
                          "URAT","URAB","URRF","USER","PID","JOBN","UPMJ","UPMT","EDSP"))

logger("Prepare Price Model for review")
setnames(price.model.Output, "URRF", "PricingKey")

output.cols <- c("PricingKey","Record","ProductCategoryDesc","ProductSubcategoryDesc","ProductAttribute1",
                 "SpecialProduct","CustomerAttribute1Desc","CustomerAttribute2Desc", 
                 "CustomerAttribute3Desc","CustomerAttribute4","Price1",
                 "Price2","Price3","URCD","Revenue.In","GP.In",
                 "Margin.In","Placements.In","UniquePlacements.In","Customers.In")

price.model.Final <- price.model.Output[, output.cols, with = F]

price.model.Final <- price.model.Final[order(-Record, -Revenue.In)]

price.model.Final <- merge(price.model.Final, prod.price.model, "PricingKey", all.x = T)

price.model.Final[, c("Price1Var", "Price2Var", "Price3Var") := .(Price1-CurrentPrice1, Price2-CurrentPrice2, Price3-CurrentPrice3)]

for(i in names(price.model.Output)[sapply(price.model.Output, is.character)]){
  set(price.model.Final,
      which(is.na(price.model.Final[[i]])), i, "")
}
