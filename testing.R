source("Michał_Taczała_assignment_1.R")
library(sqldf)
library(compare)
library(dplyr)
library(data.table)
library(microbenchmark)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

Users <- read.csv("Users.csv")
Posts <- read.csv("Posts.csv")
Comments <- read.csv("Comments.csv")
PostLinks <- read.csv("PostLinks.csv")

#1 DONE ALL
sqldf1 <- sqldf_1(Posts, Users)
base1 <- base_1(Posts, Users)
dplyr1 <- dplyr_1(Posts, Users)
data.table1 <- data.table_1(Posts, Users)

compare(sqldf1, base1, allowAll=TRUE)
compare(sqldf1, dplyr1, allowAll = TRUE)
compare(sqldf1, data.table1, allowAll = TRUE)

microbenchmark::microbenchmark(
  sqldf1 = sqldf_1(Posts, Users),
  base1 = base_1(Posts, Users),
  dplyr1 = dplyr_1(Posts, Users),
  data.table1 = data.table_1(Posts, Users),
  times = 10
)

#2
sqldf2 <- sqldf_2(Posts, PostLinks)
base2 <- base_2(Posts, PostLinks)
dplyr2 <- dplyr_2(Posts, PostLinks)
data.table2 <- data.table_2(Posts, PostLinks)

compare(sqldf2, base2, allowAll=TRUE)
compare(sqldf2, dplyr2, allowAll = TRUE)
compare(sqldf2, data.table2, allowAll = TRUE)

microbenchmark::microbenchmark(
  sqldf=sqldf_2(Posts, PostLinks),
  base=base_2(Posts, PostLinks),
  dplyr=dplyr_2(Posts, PostLinks),
  data.table=data.table_2(Posts, PostLinks),
  times=10
)
#3.1 DONE ALL
sqldf3 <- sqldf_3(Posts, Users, Comments)
base3 <- base_3(Posts, Users, Comments)
dplyr3 <- dplyr_3(Posts, Users, Comments)
data.table3 <- data.table_3(Posts, Users, Comments)
compare(sqldf3, base3, allowAll=TRUE)
compare(sqldf3, dplyr3, allowAll=TRUE)
compare(sqldf3, data.table3, allowAll = TRUE)
microbenchmark::microbenchmark(
  sqldf=sqldf_3(Posts, Users, Comments),
  base=base_3(Posts, Users, Comments),
  dplyr=dplyr_3(Posts, Users, Comments),
  data.table=data.table_3(Posts, Users, Comments),
  times=10
)
#4 DONE ALL
sqldf4 <- sqldf_4(Posts, Users)
base4 <- base_4(Posts, Users)
dplyr4 <- dplyr_4(Posts, Users)
data.table4 <- data.table_4(Posts, Users)
compare(sqldf4, base4, allowAll=TRUE)
compare(sqldf4, dplyr4, allowAll=TRUE)
compare(sqldf4, data.table4, allowAll = TRUE)
microbenchmark::microbenchmark(
  sqldf=sqldf_4(Posts, Users),
  base=base_4(Posts, Users),
  dplyr=dplyr_4(Posts, Users),
  data.table=data.table_4(Posts, Users),
  times=10
)
#5 DONE ALL
sqldf5 <- sqldf_5(Posts)
base5 <- base_5(Posts)
dplyr5 <- dplyr_5(Posts)
data.table5 <- data.table_5(Posts)
compare(sqldf5, base5, allowAll=TRUE)
compare(sqldf5, dplyr5, allowAll = TRUE)
compare(sqldf5, data.table5, allowAll= TRUE)
microbenchmark::microbenchmark(
  sqldf=sqldf_5(Posts),
  base=base_5(Posts),
  dplyr=dplyr_5(Posts),
  data.table=data.table_5(Posts),
  times=10
)