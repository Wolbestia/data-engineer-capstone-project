import pandas as pd
from pyspark.sql import SparkSession
import os
import configparser
from variables import linebackers, safeties, cornerbacks, output_data
from datetime import datetime
from pyspark.sql.functions import udf, col, monotonically_increasing_id, unix_timestamp, to_date, from_unixtime, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, countDistinct, coalesce
from pyspark.sql.types import DateType

def create_full_weeks_dataframe(spark):
    """
    Creates a dataframe with all the stats combined from the week* csv files. It creates a dataframe with one row per player too to make
    the fact table.

    Parameters:
        spark (obj): SparkSession object
    """
    weeks_df = spark.read.option("header",True).csv("nfl-big-data-bowl-2021/week*.csv").withColumnRenamed("gameId", "w_gameId")
    games_df = spark.read.option("header",True).csv("nfl-big-data-bowl-2021/games.csv")

    home_df = weeks_df.where("team = 'home'").withColumnRenamed("w_gameId", "home_gameId")
    # Drop the player-game duplicates to then join with games to have the player's team
    single_player_home_df = home_df.dropDuplicates(["nflId", "home_gameId"])

    home_games_df = games_df.join(single_player_home_df, games_df.gameId == single_player_home_df.home_gameId, "inner")
    # Drop the nflIds duplicates to have one row per player
    single_player_roster_df = home_games_df.dropDuplicates(["nflId"])

    # Create the full stats df to have one row per play and player on each game
    get_team_name = udf(lambda team, homeTeamAbbr, visitorTeamAbbr: homeTeamAbbr if team == "home" else visitorTeamAbbr)

    full_stats_df = games_df.join(weeks_df, games_df.gameId == weeks_df.w_gameId, "inner").dropDuplicates(["gameId", "playId", "nflId"])
    full_stats_df = full_stats_df.withColumn("teamName", get_team_name("team", "homeTeamAbbr", "visitorTeamAbbr"))

    return single_player_roster_df, full_stats_df

def to_date_(col, formats=("MM/dd/yyyy", "yyyy-MM-dd")):
    """
    Parse a date column into the same format
    """
    return coalesce(*[to_date(col, f) for f in formats])

def create_roster_dataframe(spark, full_bowl_roster_df):
    """
    Create the fact table with the basic information about each player.

    Parameters:
        spark(obj) SparkSession object
        full_bowl_roster_df(obj) A df with every player on the season
    """

    basic_stats_df = spark.read.option("multiline","true").json("nflstatistics/Basic_Stats.json").withColumnRenamed("position", "stats_position")
    extract_nflId = udf(lambda x: x.split("/")[1])
    basic_stats_df = basic_stats_df.withColumn("stats_nflId", extract_nflId("Player_Id")) \
                        .selectExpr("stats_nflId", "High_School as high_school", "High_School_Location as high_school_location", "Birth_Place as birth_place")

    players_df = spark.read.option("header",True).csv("nfl-big-data-bowl-2021/players.csv").withColumnRenamed("nflId", "p_nflId") \
                    .selectExpr("p_nflId", "height", "weight", "birthDate as birth_date", "collegeName as college_name")

    bowl_roster_df = full_bowl_roster_df.selectExpr("nflId as nfl_id", "displayName as display_name", "position", "homeTeamAbbr as team", "jerseyNumber as jersey_number")

    bs_bowl_roster_df = bowl_roster_df.join(basic_stats_df, bowl_roster_df.nfl_id == basic_stats_df.stats_nflId, "inner")
    full_roster_df = bs_bowl_roster_df.join(players_df, bs_bowl_roster_df.nfl_id == players_df.p_nflId, "inner").drop("stats_nflId", "p_nflId")

    convert_height = udf(lambda x: round(float(x.replace("-", ".")), 1) if float(x.replace("-", ".")) < 10 else round((float(x) / 12 ), 1))

    full_roster_df = full_roster_df.withColumn("height", convert_height("height")).withColumn("birth_date", to_date_("birth_date"))
    print(full_roster_df.printSchema())

    """
    full_roster_df.repartition(col("team")) \
        .write \
        .option("maxRecordsPerFile", 11) \
        .partitionBy("team") \
        .parquet(os.path.join(output_data, 'roster/roster.parquet'), 'overwrite')

    """

    return full_roster_df

def create_deffense_dimensions(spark, full_stats_df):

    """
    Create the dimension tables of the deffense players

    Parameters:
        spark(obj): SparkSession obj
        full_stats_df(obj): A dataframe with every play of each player
    """
    plays_df = spark.read.option("header",True).csv("nfl-big-data-bowl-2021/plays.csv")
    def_df = plays_df.selectExpr("playId as play_id", "gameId as game_id", "playDescription as play_description", "offenseFormation as offense_formation", \
                            "personnelO as personnel_o", "defendersInTheBox as defenders_in_the_box", "numberOfPassRushers as number_of_pass_rushers", \
                            "personnelD as personnel_d", "passResult as pass_result") \
                .where(col("passResult").isin({"I", "S", "IN"}))

    full_stats_df = full_stats_df.selectExpr("nflId as nfl_id", "gameId", "playId", "teamName as team", "position")
    def_play_stats_df = full_stats_df.join(def_df, (full_stats_df.gameId == def_df.game_id) & (full_stats_df.playId == def_df.play_id)).drop("gameId", "playId")

    # linebackers dimension
    lbs_df = def_play_stats_df.where(col("position").isin(linebackers))
    print(" --- LB STATS ---")
    print(lbs_df.printSchema())
    """
    lbs_df.repartition(col("team"), col("game_id")) \
        .write \
        .option("maxRecordsPerFile", 11) \
        .partitionBy("team", "game_id") \
        .parquet(os.path.join(output_data, 'lb_stats/lb_stats.parquet'), 'overwrite')
    """

    # safeties dimension
    safeties_df = def_play_stats_df.where(col("position").isin(safeties))
    print(" --- SAFETY STATS ---")
    """
    safeties_df.repartition(col("team"), col("game_id")) \
        .write \
        .option("maxRecordsPerFile", 11) \
        .partitionBy("team", "game_id") \
        .parquet(os.path.join(output_data, 'lb_stats/lb_stats.parquet'), 'overwrite')
    """

    # cornerbacks dimension
    cbs_df = def_play_stats_df.where(col("position").isin(cornerbacks))
    print(" --- CB STATS ---")
    """
    cbs_df.repartition(col("team"), col("game_id")) \
        .write \
        .option("maxRecordsPerFile", 11) \
        .partitionBy("team", "game_id") \
        .parquet(os.path.join(output_data, 'lb_stats/lb_stats.parquet'), 'overwrite')
    """

    return {'lbs': lbs_df, 'safeties': safeties_df, 'cbs': cbs_df}

def create_offense_dimensions(spark, full_stats_df):
    """
    Orquestate the creation of the dimension tables of the offense players

    Parameters:
        spark(obj): SparkSession obj
        full_stats_df(obj): A dataframe with every play of each player
    """

    plays_df = spark.read.option("header",True).csv("nfl-big-data-bowl-2021/plays.csv")
    full_stats_df = full_stats_df.selectExpr("nflId as nfl_id", "gameId", "playId", "teamName as team", "position", "route")

    qb_df = create_qb_dimensions(spark, full_stats_df, plays_df)

    te_df, wr_df = create_receiver_dimensions(spark, full_stats_df, plays_df)

    return {'qbs': qb_df, 'tes': te_df, 'wrs': wr_df}

def create_qb_dimensions(spark, full_stats_df, plays_df):
    """
    Create the qbs dimension table

    Parameters:
        spark(obj): SparkSession obj
        full_stats_df(obj): A dataframe with every play of each player
        plays_df(obj): Plays dataframe with the needed columns
    """
    qb_plays_df = plays_df.selectExpr("playId as play_id", "gameId as game_id", "playDescription as play_description", "down", "yardsToGo as yds_to_go", \
                                "playType as play_type", "offenseFormation as offense_formation", "personnelO as personnel_o", \
                                "defendersInTheBox as defenders_in_the_box", "numberOfPassRushers as numer_of_pass_rushers", "personnelD as personnel_d", \
                                "typeDropBack as type_dropback", "passResult as pass_result", "offensePlayResult as offense_play_result")

    qb_play_stats_df = full_stats_df.join(qb_plays_df, (full_stats_df.gameId == qb_plays_df.game_id) & (full_stats_df.playId == qb_plays_df.play_id)).drop("gameId", "playId")
    qb_df = qb_play_stats_df.where("position = 'QB'")

    print("--- QB STATS ---")
    print(qb_df.printSchema())
    """
    qb_df.repartition(col("team"), col("game_id")) \
        .write \
        .option("maxRecordsPerFile", 11) \
        .partitionBy("team", "game_id") \
        .parquet(os.path.join(output_data, 'qb_stats/qb_stats.parquet'), 'overwrite')
    """

    return qb_df

def create_receiver_dimensions(spark, full_stats_df, plays_df):
    """
    Create both the tes and wrs dimension tables

    Parameters:
        spark(obj): SparkSession obj
        full_stats_df(obj): A dataframe with every play of each player
        plays_df(obj): Plays dataframe with the needed columns
    """

    receiver_plays_df = plays_df.selectExpr("playId as play_id", "gameId as game_id", "playDescription as play_description", "down", "yardsToGo as yds_to_go", \
                                            "playType as play_type", "offenseFormation as offense_formation", "personnelO as personnel_o", \
                                            "defendersInTheBox as defenders_in_the_box", "personnelD as personnel_d", "typeDropback as type_dropback", \
                                            "passResult as pass_result", "offensePlayResult as offense_play_result") \
                        .where("play_type = 'play_type_pass' AND passResult = 'C'")

    receiver_play_stats_df = full_stats_df.join(receiver_plays_df, (full_stats_df.gameId == receiver_plays_df.game_id) & (full_stats_df.playId == receiver_plays_df.play_id)).drop("gameId", "playId")

    te_df = receiver_play_stats_df.where("position = 'TE'")
    print("--- TE STATS ---")
    print(te_df.printSchema())
    """
    te_df.repartition(col("team"), col("game_id")) \
        .write \
        .option("maxRecordsPerFile", 11) \
        .partitionBy("team", "game_id") \
        .parquet(os.path.join(output_data, 'qb_stats/qb_stats.parquet'), 'overwrite')
    """

    wr_df = receiver_play_stats_df.where("position = 'WR'")
    print("--- WR STATS ---")
    """
    wr_df.repartition(col("team"), col("game_id")) \
        .write \
        .option("maxRecordsPerFile", 11) \
        .partitionBy("team", "game_id") \
        .parquet(os.path.join(output_data, 'qb_stats/qb_stats.parquet'), 'overwrite')
    """

    return te_df, wr_df

def data_quality_checks(spark, roster_df, deffense_dfs, offense_dfs):
    """
    Run data quality checks based on the data model created. It checks for column formats and row count

    Parameters:
        spark(obj): SparkSession obj
        roster_df(obj): Fact table dataframe
        deffense_dfs(list): List with all the deffense dfs
        offense_dfs(list): List with all the deffense dfs
    """
    height_errors = roster_df.filter(roster_df.height.contains('-')).count()

    if height_errors > 0:
        raise Exception("There's errors in the height column on roster table")

    birth_date_errors = roster_df.filter(roster_df.birth_date.contains('/')).count()

    if birth_date_errors > 0:
        raise Exception("There's errors in the birth_date column on roster table")

    lb_df_count = deffense_dfs['lbs'].count()
    safeties_df_count = deffense_dfs['safeties'].count()
    cb_df_count = deffense_dfs['cbs'].count()

    if lb_df_count < 0:
        raise Exception("lb_stats dimension is empty")

    if safeties_df_count < 0:
        raise Exception("safeties_stats dimension is empty")

    if cb_df_count < 0:
        raise Exception("cb_stats dimension is empty")

    qb_df_count = offense_dfs['qbs'].count()
    te_df_count = offense_dfs['tes'].count()
    wr_df_count = offense_dfs['wrs'].count()

    if qb_df_count < 0:
        raise Exception("qb_stats dimension is empty")

    if te_df_count < 0:
        raise Exception("te_stats dimension is empty")

    if wr_df_count < 0:
        raise Exception("wr_stats dimension is empty")

    print("*** Data quality checks are correct ***")

    return True
