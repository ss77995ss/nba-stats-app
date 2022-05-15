import pandas as pd
from datetime import date

df = pd.read_csv('./basic_per_game_player_stats_2013_2018.csv')

df = df.fillna(0)

stats_column_names = 'GS,MP,FG,FGA,"FG%","2P","2PA","2P%","3P","3PA","3P%",FT,FTA,"FT%",ORB,DRB,TRB,AST,STL,BLK,TOV,PF,PTS'

season_dict = {
    "13-14": {
        "start": "2013-10-29",
        "end": "2014-05-12"
    },
    "14-15": {
        "start": "2014-10-21",
        "end": "2015-05-04"
    },
    "15-16": {
        "start": "2015-10-14",
        "end": "2016-04-26"
    },
    "16-17": {
        "start": "2016-10-05",
        "end": "2017-04-18"
    },
    "17-18": {
        "start": "2017-09-28",
        "end": "2018-04-11"
    },
}

match_list = []


def get_season(target_date):
    for season in season_dict:
        start_date = season_dict[season]["start"]
        end_date = season_dict[season]["end"]
        if (date.fromisoformat(start_date) <= date.fromisoformat(target_date) <= date.fromisoformat(end_date)):
            return season


def check_match_existence(target_date, home_team, away_team):
    if len(match_list) == 0:
        return False

    for match in match_list:
        if date.fromisoformat(match["date"]) == date.fromisoformat(target_date) and home_team == match["home_team"] and away_team == match["away_team"]:
            return True

    return False


with open('load.sql', 'w') as file:
    # Insert Team data
    teams = pd.unique(df['Tm'])

    file.write("delete from match;\n")
    file.write("delete from player;\n")
    file.write("delete from playerstats;\n")
    file.write("delete from season;\n")
    file.write("delete from team;\n\n")

    for team in teams:
        file.write(f"insert into Team(name) values ('{team}');")
        file.write("\n")

    file.write("\n")

    # Insert Season data
    for season in season_dict:
        start_date = season_dict[season]["start"]
        end_date = season_dict[season]["end"]
        file.write(
            f"insert into Season(name, start_date, end_date) values ('{season}', '{start_date}', '{end_date}');")
        file.write("\n")

    file.write("\n")

    # Insert Player data
    players = pd.unique(df['Player'])

    for player in players:
        player_wo_nickname = player.split("\\")[0]

        if "'" in player_wo_nickname:
            split_name = player_wo_nickname.split("'")
            player_wo_nickname = split_name[0] + "''" + split_name[1]

        file.write(
            f"insert into Player(name) values ('{player_wo_nickname}');")
        file.write("\n")

    file.write("\n")

    # Insert Match data
    for index, row in df.iterrows():
        player_wo_nickname = row["Player"].split("\\")[0]
        home_team = ''
        away_team = ''
        target_date = row["Date"]
        home_or_away = ''
        age = row["Age"]

        if "'" in player_wo_nickname:
            split_name = player_wo_nickname.split("'")
            player_wo_nickname = split_name[0] + "''" + split_name[1]

        if row['At'] == '@':
            home_team = row['Opp']
            away_team = row['Tm']
            home_or_away = 'away'
        else:
            home_team = row['Tm']
            away_team = row['Opp']
            home_or_away = 'home'

        season = get_season(target_date)
        if not check_match_existence(target_date, home_team, away_team):
            match_list.append({
                "date": target_date,
                "home_team": home_team,
                "away_team": away_team
            })
            file.write(
                (
                    f'insert into Match (date, season_id, home_team_id, away_team_id)\n'
                    f"select '{target_date}', s.id, t1.id, t2.id\n"
                    f'from Season s, Team t1, Team t2\n'
                    f"where s.name = '{season}' AND t1.name = '{home_team}' AND t2.name = '{away_team}';\n\n"
                )
            )

        # Insert Player Stats
        stats_str = ','.join(list(row[9:32].apply(str)))
        # print(",".join(row[9:33].index.tolist()))
        file.write(
            (
                f'insert into PlayerStats (player_id, match_id, home_or_away, age, {stats_column_names})\n'
                f"select p.id, m.id, '{home_or_away}', '{age}', {stats_str}\n"
                f'from Player p, Match m, Team t1, Team t2\n'
                f'where m.home_team_id = t1.id AND m.away_team_id = t2.id\n'
                f"AND m.date = '{target_date}'\n"
                f"AND p.name = '{player_wo_nickname}' AND t1.name = '{home_team}' AND t2.name = '{away_team}';\n\n"
            )
        )

        file.write('\n')
