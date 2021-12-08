



top_players = spark.sql("""
  select player_id, sum(1) as games, sum(goals) as goals
  from stats
  group by 1
  order by 3 desc
  limit 5
""")
top_players.createOrReplaceTempView("top_players")
names.createOrReplaceTempView("names")
display(spark.sql("""
  select p.player_id, goals, _c1 as First, _c2 as Last
  from top_players p
  join names n
    on p.player_id = n._c0
  order by 2 desc
"""))



