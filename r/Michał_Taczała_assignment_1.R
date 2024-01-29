### Data Processing in R and Python 2023Z
### Homework Assignment no. 1
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
###
### Report should include:
### * source() of this file at the beggining,
### * reading the data, 
### * attaching the libraries, 
### * execution time measurements (with microbenchmark),
### * and comparing the equivalence of the results,
### * interpretation of queries.



##################
#LOADING DATA
##################
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
Users <- read.csv("Users.csv")
Posts <- read.csv("Posts.csv")
Comments <- read.csv("Comments.csv")
PostsLinks <- read.csv("PostLinks.csv")
##################
library(dplyr)
library(data.table)

# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#


sqldf_1 <- function(Posts, Users){
  # Input the solution here
  # 
  return(sqldf("SELECT Location, COUNT(*) AS Count
FROM (
SELECT Posts.OwnerUserId, Users.Id, Users.Location
FROM Users
JOIN Posts ON Users.Id = Posts.OwnerUserId
)
WHERE Location NOT IN ('')
GROUP BY Location
ORDER BY Count DESC
LIMIT 10"))
}

base_1 <- function(Posts, Users){
  # Input the solution here
  # 
  merged <- merge(Users, Posts, by.x = "Id", by.y = "OwnerUserId")
  filtered <- merged[merged$Location != "",]
  counted <- table(filtered$Location)
  selected <- data.frame(Location=names(counted), Count = as.numeric(counted))
  ordered <- selected[order(selected$Count, decreasing = TRUE),]
  return(ordered[1:10,])
}

dplyr_1 <- function(Posts, Users){
  # Input the solution here
  # 
  return(inner_join(Users, Posts, by=join_by(Id == OwnerUserId)) %>% 
    filter(Location != "") %>% 
    count(Location, name="Count") %>%
    select(Location, Count) %>% 
    arrange(desc(Count)) %>% 
    slice(1:10))
}

data.table_1 <- function(Posts, Users){
  # Input the solution here
  # 
  setDT(Posts)
  setDT(Users)
  merged <- Users[Posts, on=.(Id=OwnerUserId)]
  filtered <- merged[Location != ""]
  counted <- filtered[,.(Count = .N), by = Location]
  ordered <- counted[order(Count, decreasing = TRUE)]
  return(ordered[1:10,])
}

# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

sqldf_2 <- function(Posts, PostsLinks){
  # Input the solution here
  # 
  sqldf("
    SELECT RelatedPostId AS PostId, COUNT(*) AS NumLinks
    FROM PostsLinks
    GROUP BY RelatedPostId
    ") -> RelatedTab
  
  return(sqldf("
    SELECT Posts.Title, RelatedTab.NumLinks
    FROM RelatedTab
    JOIN Posts ON RelatedTab.PostId=Posts.Id
    WHERE Posts.PostTypeId=1
    ORDER BY NumLinks DESC
    ")) 
}

base_2 <- function(Posts, PostsLinks){
  # Input the solution here
  # 
  relatedTab <- aggregate(PostLinks$Id, by=list(PostLinks$RelatedPostId), FUN=length)
  colnames(relatedTab) <- c("PostId", "NumLinks")
  merged <- merge(relatedTab, Posts, by.x = "PostId", by.y="Id")
  filtered <- merged[merged$PostTypeId==1,]
  ordered <- filtered[order(filtered$NumLinks, decreasing = TRUE),]
  return(data.frame(
    Title=ordered$Title,
    RelatedTab=ordered$NumLinks
  ))
  
}

dplyr_2 <- function(Posts, PostsLinks){
  # Input the solution here
  # 
  return(PostsLinks %>%
    group_by(RelatedPostId) %>%
    summarize(NumLinks=n()) %>%
    rename(PostId=RelatedPostId) %>%
    inner_join(Posts, by=join_by(PostId == Id)) %>%
    filter(PostTypeId==1) %>%
    arrange(desc(NumLinks)) %>%
    select(Title, NumLinks))
}

data.table_2 <- function(Posts, PostsLinks){
  # Input the solution here
  # 
  setDT(Posts)
  setDT(PostsLinks)
  RelatedTab <- PostsLinks[,.(NumLinks = .N), by = RelatedPostId]
  setnames(RelatedTab, old="RelatedPostId", new="PostId")
  merged <- RelatedTab[Posts, on=.(PostId=Id)]
  filtered <- merged[PostTypeId==1]
  res <- filtered[order(NumLinks, decreasing = TRUE)]
  return(res[,.(Title, NumLinks)])
}

# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#

sqldf_3 <- function(Posts, Users, Comments){
  sqldf("
        SELECT Title, CommentCount, ViewCount, CommentsTotalScore, 
               DisplayName, Reputation, Location
        FROM (
                SELECT Posts.OwnerUserId, Posts.Title, 
                       Posts.CommentCount, Posts.ViewCount, 
                       CmtTotScr.CommentsTotalScore
                FROM (
                        SELECT PostId, SUM(Score) AS CommentsTotalScore
                        FROM Comments
                        GROUP BY PostId
                     ) AS CmtTotScr
                JOIN Posts ON Posts.Id = CmtTotScr.PostId
                WHERE Posts.PostTypeId=1
            ) AS PostsBestComments
        JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
        ORDER BY CommentsTotalScore DESC
    ") -> tab
  # selecting LIMIT separately will improve time 
  return(sqldf("SELECT * FROM tab LIMIT 10"))
}

base_3 <- function(Posts, Users, Comments){
  # Input the solution here
  # 
  grouped <- aggregate(Comments$Score, by=list(Comments$PostId), FUN=sum)
  colnames(grouped) <- c("PostId", "CommentsTotalScore")
  merged <- merge(grouped, Posts, by.x="PostId", by.y="Id")
  filtered <- merged[merged$PostTypeId==1,]
  PostsBestComments <- data.frame(
    filtered$OwnerUserId,
    filtered$Title,
    filtered$CommentCount,
    filtered$ViewCount,
    filtered$CommentsTotalScore
  )
  final_merged <- merge(PostsBestComments, Users, by.x = "filtered.OwnerUserId", by.y = "Id")
  ordered <- final_merged[order(final_merged$filtered.CommentsTotalScore, decreasing = TRUE),]
  limited <- head(ordered, n = 10)
  return(data.frame(
    Title=limited$filtered.Title,
    CommentCount=limited$filtered.CommentCount,
    ViewCount=limited$filtered.ViewCount,
    CommentsTotalScore=limited$filtered.CommentsTotalScore,
    DisplayName=limited$DisplayName,
    Reputation=limited$Reputation,
    Location=limited$Location
  ))
}


dplyr_3 <- function(Posts, Users, Comments){
  # Input the solution here
  # 
  return(Comments %>%
    group_by(PostId) %>%
    summarize(CommentsTotalScore=sum(Score)) %>%
    inner_join(Posts, by=join_by(PostId == Id)) %>%
    filter(PostTypeId==1) %>%
    select(OwnerUserId, Title, CommentCount, ViewCount, CommentsTotalScore) %>%
    inner_join(Users, by=join_by(OwnerUserId == Id)) %>%
    arrange(desc(CommentsTotalScore)) %>%
    slice(1:10) %>%
    select(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location)
  )

}

data.table_3 <- function(Posts, Users, Comments){
  # Input the solution here
  # 
  setDT(Posts)
  setDT(Users)
  setDT(Comments)
  CmtToSrc <- Comments[,.(CommentsTotalScore=sum(Score)), by=PostId]
  merged <- CmtToSrc[Posts,on=.(PostId=Id)]  
  PostsBestComments <- merged[PostTypeId==1]
  merged2 <- PostsBestComments[Users, on=.(OwnerUserId=Id)]
  ordered <- merged2[order(CommentsTotalScore, decreasing = TRUE)][1:10]
  return(ordered[, .(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location)])
}

# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#

sqldf_4 <- function(Posts, Users){
  # Input the solution here
  # 
  return(sqldf("SELECT DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes
FROM (
SELECT *
FROM (
SELECT COUNT(*) as AnswersNumber, OwnerUserId
FROM Posts
WHERE PostTypeId = 2
GROUP BY OwnerUserId
) AS Answers
JOIN
(
SELECT COUNT(*) as QuestionsNumber, OwnerUserId
FROM Posts
WHERE PostTypeId = 1
GROUP BY OwnerUserId
) AS Questions
ON Answers.OwnerUserId = Questions.OwnerUserId
WHERE AnswersNumber > QuestionsNumber
ORDER BY AnswersNumber DESC
LIMIT 5
) AS PostsCounts
JOIN Users
ON PostsCounts.OwnerUserId = Users.Id")
  )
}

base_4 <- function(Posts, Users){
  # Input the solution here
  # 
  filtered_posts_1 <- Posts[Posts$PostTypeId==1,]
  filtered_posts_2 <- Posts[Posts$PostTypeId==2,]
  Questions <- aggregate(filtered_posts_1$PostTypeId, by=list(filtered_posts_1$OwnerUserId), FUN = length)
  colnames(Questions) <- c("OwnerUserId", "QuestionsNumber")
  Answers <- aggregate(filtered_posts_2$PostTypeId, by=list(filtered_posts_2$OwnerUserId), FUN = length)
  colnames(Answers) <- c("OwnerUserId", "AnswersNumber")
  merged <- merge(Answers, Questions, by.x = "OwnerUserId", by.y = "OwnerUserId")
  filtered <- merged[merged$AnswersNumber>merged$QuestionsNumber,]
  ordered <- filtered[order(filtered$AnswersNumber, decreasing = TRUE),]
  PostsCounts <- head(ordered, 5)
  final_merged <- merge(PostsCounts, Users, by.x="OwnerUserId", by.y="Id")
  return(data.frame(
    DisplayName = final_merged$DisplayName,
    QuestionsNumber = final_merged$QuestionsNumber,
    AnswersNumber = final_merged$AnswersNumber,
    Location = final_merged$Location,
    Reputation = final_merged$Reputation,
    UpVotes = final_merged$UpVotes,
    DownVotes = final_merged$DownVotes
  ))
}

dplyr_4 <- function(Posts, Users){
  # Input the solution here
  # 
  Answers <- Posts %>%
    filter(PostTypeId==2) %>%
    count(OwnerUserId, name = "AnswersNumber")
  Questions <- Posts %>%
    filter(PostTypeId==1) %>%
    count(OwnerUserId, name = "QuestionsNumber")
  PostsCounts <- inner_join(Answers, Questions, by=join_by(OwnerUserId==OwnerUserId)) %>%
    filter(AnswersNumber>QuestionsNumber) %>%
    arrange(desc(AnswersNumber)) %>%
    slice(1:5)
  return(inner_join(PostsCounts, Users, by=join_by(OwnerUserId==Id)) %>%
    select(DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes)
  )
    
}

data.table_4 <- function(Posts, Users){
  # Input the solution here
  # 
  setDT(Posts)
  setDT(Users)
  filtered_answers <- Posts[PostTypeId==2]
  Answers1 <- filtered_answers[!is.na(OwnerUserId), .(AnswersNumber = .N), by = .(OwnerUserId)]
  filtered_questions <- Posts[PostTypeId==1]
  Questions1 <- filtered_questions[!is.na(OwnerUserId), .(QuestionsNumber = .N), by = .(OwnerUserId)]
  merged1 <- Answers1[Questions1, on =.(OwnerUserId), nomatch = NULL]
  filtered <- merged1[AnswersNumber > QuestionsNumber]
  PostsCounts <- filtered[order(AnswersNumber, decreasing = TRUE)][1:5]
  merged2 <- PostsCounts[Users, on = .(OwnerUserId=Id), nomatch = NULL]
  return(
    merged2[,.(DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes)]
  )
}

# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

sqldf_5 <- function(Posts){
  # Input the solution here
  # 
  return(
  sqldf("SELECT
Questions.Id,
Questions.Title,
BestAnswers.MaxScore,
Posts.Score AS AcceptedScore,
BestAnswers.MaxScore-Posts.Score AS Difference
FROM (
SELECT ParentId, MAX(Score) AS MaxScore
FROM Posts
WHERE PostTypeId==2
GROUP BY ParentId
) AS BestAnswers
JOIN (
SELECT * FROM Posts
WHERE PostTypeId==1
) AS Questions
ON Questions.Id=BestAnswers.ParentId
JOIN Posts ON Questions.AcceptedAnswerId=Posts.Id
WHERE Difference>50
ORDER BY Difference DESC")
  )
}

base_5 <- function(Posts){
  # Input the solution here
  # 
  filtered <- Posts[Posts$PostTypeId==2,]
  BestAnswers <- aggregate(filtered$Score, by=list(filtered$ParentId), FUN = max)
  colnames(BestAnswers) <- c("ParentId", "MaxScore")
  Questions <- Posts[Posts$PostTypeId==1,]
  merged <- merge(Questions, BestAnswers, by.x="Id", by.y="ParentId", suffixes = c("Questions", "BestAnswers"))
  merged2 <- merge(merged, Posts, by.x="AcceptedAnswerId", by.y="Id", suffixes = c("Merged", "Posts"))
  merged2[,"Difference"] <- merged2[,"MaxScore"]-merged2[,"ScorePosts"]
  filtered2 <- merged2[merged2$Difference>50,]
  ordered <- filtered2[order(filtered2$Difference, decreasing = TRUE),]
  return(data.frame(
    Id=ordered$Id,
    Title=ordered$TitleMerged,
    MaxScore=ordered$MaxScore,
    AcceptedScore=ordered$ScorePosts,
    Difference=ordered$Difference
  ))
}

dplyr_5 <- function(Posts){
  # Input the solution here
  # 
  BestAnswers <- Posts %>%
    filter(PostTypeId==2) %>%
    group_by(ParentId) %>%
    summarize(MaxScore=max(Score)) 
  Questions <- Posts %>%
    filter(PostTypeId==1)
  
  return(
    Questions %>%
    inner_join(BestAnswers, by= join_by(Id==ParentId)) %>%
    inner_join(Posts, by=join_by(AcceptedAnswerId==Id),suffix = c("Questions", "Posts")) %>%
    mutate(Difference=MaxScore-ScorePosts) %>%
    filter(Difference>50) %>%
    arrange(desc(Difference)) %>%
    select(Id, Title=TitleQuestions, MaxScore, AcceptedScore=ScorePosts, Difference)
  )
}

data.table_5 <- function(Posts){
  # Input the solution here
  # 
  setDT(Posts)
  filtered_answers <- Posts[PostTypeId==2]
  BestAnswers <- filtered_answers[,.(MaxScore=max(Score)), by=ParentId]
  Questions <- Posts[PostTypeId==1]

  merged <- Questions[BestAnswers, on = .(Id=ParentId), nomatch = NULL]
  merged2 <- merged[Posts, on = .(AcceptedAnswerId=Id), nomatch = NULL]
  expandedd <- merged2[,Difference:=MaxScore-i.Score]
  filtered <- expandedd[Difference > 50]
  sorted <- filtered[order(Difference, decreasing = TRUE)]
  return(sorted[, .(Id,  TitleQuestions = Title, MaxScore, ScorePosts = i.Score, Difference)])
}

