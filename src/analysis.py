from pyspark.sql import functions as F

from pyspark.sql.window import Window


def final_analysis(df, show_output: bool = False):
    """
    Performs user session segmentation and identifies the top 10 most played
    tracks from the top 50 largest listening sessions.

    Parameters:-
    df : pyspark.sql.DataFrame
        Must contain columns:
        - user_id
        - timestamp
        - track_name

    show_output : bool
        If True, prints the top 10 songs in the console.

    Returns:-
    pyspark.sql.DataFrame
        DataFrame containing columns: track_name, count
        (Top 10 tracks from top 50 sessions)
    """
    
    # Timestamp conversion
    df = df.withColumn("timestamp_ts", F.to_timestamp("timestamp"))

    # Window ordered by timestamp per user
    w = Window.partitionBy("user_id").orderBy("timestamp_ts")

    # Compute time gaps and session flags
    df = df.withColumn("prev_ts", F.lag("timestamp_ts").over(w))

    df = df.withColumn(
        "gap_minutes",
        (F.unix_timestamp("timestamp_ts") - F.unix_timestamp("prev_ts")) / 60
    )

    df = df.withColumn(
        "new_session",
        F.when((F.col("gap_minutes") > 20) | F.col("gap_minutes").isNull(), 1).otherwise(0)
    )

    df = df.withColumn(
        "session_id",
        F.sum("new_session").over(w)
    )

    # Identify top 50 sessions
    session_sizes = (
        df.groupBy("user_id", "session_id")
          .count()
          .withColumnRenamed("count", "track_count")
    )

    top_sessions = session_sizes.orderBy(F.col("track_count").desc()).limit(50)

    # Filter original DF for only those sessions
    df_top50 = df.join(top_sessions, on=["user_id", "session_id"], how="inner")

    # Rank top songs
    top_songs = (
        df_top50.groupBy("track_name")
                .count()
                .orderBy(F.col("count").desc())
    )

    top_10_songs = top_songs.limit(10)

    # Print statement
    if show_output:
        print("\n TOP 10 SONGS ")
        top_10_songs.show(truncate=False)

    return top_10_songs