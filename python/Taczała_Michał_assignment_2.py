### Data Processing in R and Python 2023Z
### Homework Assignment no. 2
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
###
#
# Include imports here
import pandas as pd
import numpy as np

# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#


def query_1_sql(conn):
    return pd.read_sql_query(
        """
SELECT Location, COUNT(*) AS Count
FROM (
SELECT Posts.OwnerUserId, Users.Id, Users.Location
FROM Users
JOIN Posts ON Users.Id = Posts.OwnerUserId
)
WHERE Location NOT IN ('')
GROUP BY Location
ORDER BY Count DESC
LIMIT 10
""",
        conn,
    )


def solution_1(Posts, Users):
    # """Input the solution here"""

    merged = Users.merge(
        Posts,
        left_on="Id",
        right_on="OwnerUserId",
    )
    return (
        merged.loc[merged["Location"] != ""]
        .groupby("Location")
        .size()
        .reset_index(name="Count")
        .sort_values(by=["Count"], ascending=False)
        .head(10)
    )


# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#


def query_2_sql(conn):
    return pd.read_sql_query(
        """
SELECT Posts.Title, RelatedTab.NumLinks
FROM
(
SELECT RelatedPostId AS PostId, COUNT(*) AS NumLinks
FROM PostLinks
GROUP BY RelatedPostId
) AS RelatedTab
JOIN Posts ON RelatedTab.PostId=Posts.Id
WHERE Posts.PostTypeId=1
ORDER BY NumLinks DESC
""",
        conn,
    )


def solution_2(Posts, PostsLinks):
    merged = (
        PostsLinks.groupby("RelatedPostId")
        .size()
        .reset_index(name="NumLinks")
        # .rename(columns={"RelatedPostId": "PostId"})
        .merge(Posts, left_on="RelatedPostId", right_on="Id")
    )
    return merged.loc[merged["PostTypeId"] == 1].sort_values(
        by=["NumLinks"],
        ascending=False,
        kind="mergesort",
    )[["Title", "NumLinks"]]
    # ...
    # ...


# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#


def query_3_sql(conn):
    return pd.read_sql_query(
        """
SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
FROM (
SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,
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
LIMIT 10
""",
        conn,
    )


def solution_3(Posts, Users, Comments):
    grouped = Comments.groupby("PostId")
    summed = (
        grouped.sum(grouped.Score)
        .reset_index()[["PostId", "Score"]]
        .rename(columns={"Score": "CommentsTotalScore"})
    ).merge(Posts, left_on="PostId", right_on="Id")
    filtered = summed.loc[summed["PostTypeId"] == 1]
    # .rename(columns={"Sum": "CommentsTotalScore"})

    withNa = (
        filtered.loc[filtered["PostTypeId"] == 1][
            ["OwnerUserId", "Title", "CommentCount", "ViewCount", "CommentsTotalScore"]
        ]
        .merge(Users, left_on="OwnerUserId", right_on="Id")
        .sort_values(by=["CommentsTotalScore"], ascending=False)
        .head(10)[
            [
                "Title",
                "CommentCount",
                "ViewCount",
                "CommentsTotalScore",
                "DisplayName",
                "Reputation",
                "Location",
            ]
        ]
    )
    return withNa.replace({np.nan: None})


# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#
def query_4_sql(conn):
    return pd.read_sql_query(
        """
SELECT DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes
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
ON PostsCounts.OwnerUserId = Users.Id
""",
        conn,
    )


def solution_4(Posts, Users):
    answers = (
        Posts.loc[Posts["PostTypeId"] == 2]
        .groupby("OwnerUserId")
        .size()
        .reset_index(name="AnswersNumber")
    )
    questions = (
        Posts.loc[Posts["PostTypeId"] == 1]
        .groupby("OwnerUserId")
        .size()
        .reset_index(name="QuestionsNumber")
    )
    merged = answers.merge(questions, left_on="OwnerUserId", right_on="OwnerUserId")
    return (
        merged.loc[merged["AnswersNumber"] > merged["QuestionsNumber"]]
        .sort_values(by="AnswersNumber", ascending=False)
        .head(5)
        .merge(Users, left_on="OwnerUserId", right_on="Id")[
            [
                "DisplayName",
                "QuestionsNumber",
                "AnswersNumber",
                "Location",
                "Reputation",
                "UpVotes",
                "DownVotes",
            ]
        ]
    )


# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#
def query_5_sql(conn):
    return pd.read_sql_query(
        """
SELECT
Questions.Id,
Questions.Title,
BestAnswers.MaxScore,
Posts.Score AS AcceptedScore,
BestAnswers.MaxScore-Posts.Score AS Difference
FROM (
SELECT Id, ParentId, MAX(Score) AS MaxScore
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
ORDER BY Difference DESC
""",
        conn,
    )


def solution_5(Posts):
    best_answers = (
        Posts.loc[Posts["PostTypeId"] == 2]
        .groupby("ParentId")["Score"]
        .max()
        .reset_index(name="MaxScore")
    )
    questions = Posts.loc[Posts["PostTypeId"] == 1]
    merged = best_answers.merge(questions, left_on="ParentId", right_on="Id").merge(
        Posts, left_on="AcceptedAnswerId", right_on="Id"
    )
    # cls = merged.columns.values.tolist()
    merged["Difference"] = merged["MaxScore"] - merged["Score_y"]
    sorted = merged.loc[merged["Difference"] > 50].sort_values(
        by="Difference",
        ascending=False,
        kind="mergesort",
    )
    selected = sorted[["Id_x", "Title_x", "MaxScore", "Score_y", "Difference"]]
    selected.columns = ["Id", "Title", "MaxScore", "AcceptedScore", "Difference"]
    return selected
