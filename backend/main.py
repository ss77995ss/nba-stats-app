import datetime
import psycopg2

from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

try:
    conn = psycopg2.connect(database="nba_player_stats",
                            user="lihsuan.hsieh",
                            password="Nash77995!",
                            port="5432")
    print("Database connected successfully")
except:
    print("Database not connected successfully")

app = FastAPI()


origins = [
    "http://localhost:3000",
    "localhost:3000"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


class Item(BaseModel):
    name: str
    price: float
    is_offer: Optional[bool] = None


class Test(BaseModel):
    id: int
    name: str


class NameInsert(BaseModel):
    name: str


class NameUpdate(BaseModel):
    id: int
    name: str


class DataDelete(BaseModel):
    id: int


class SeasonInsert(BaseModel):
    name: str
    start_date: datetime.date
    end_date: datetime.date


class SeasonUpdate(BaseModel):
    id: int
    name: str
    start_date: datetime.date
    end_date: datetime.date


class MatchInsert(BaseModel):
    date: datetime.date
    season_id: int
    home_team_id: int
    away_team_id: int


class MatchUpdate(BaseModel):
    id: int
    date: datetime.date
    season_id: int
    home_team_id: int
    away_team_id: int


class StatsInsert(BaseModel):
    player_id: int
    match_id: int
    home_or_away: str
    age: str
    GS: Optional[int] = 0
    MP: Optional[int] = 0
    FG: Optional[int] = 0
    FGA: Optional[int] = 0
    FGP: Optional[float] = 0
    TWO: Optional[int] = 0
    TWOA: Optional[int] = 0
    TWOP: Optional[float] = 0
    THR: Optional[int] = 0
    THRA: Optional[int] = 0
    THRP: Optional[float] = 0
    FT: Optional[int] = 0
    FTA: Optional[int] = 0
    FTP: Optional[float] = 0
    ORB: Optional[int] = 0
    DRB: Optional[int] = 0
    TRB: Optional[int] = 0
    AST: Optional[int] = 0
    BLK: Optional[int] = 0
    STL: Optional[int] = 0
    TOV: Optional[int] = 0
    PF: Optional[int] = 0
    PTS: Optional[int] = 0


class StatsUpdate(BaseModel):
    id: int
    player_id: int
    match_id: int
    home_or_away: str
    age: str
    GS: Optional[int] = 0
    MP: Optional[int] = 0
    FG: Optional[int] = 0
    FGA: Optional[int] = 0
    FGP: Optional[float] = 0
    TWO: Optional[int] = 0
    TWOA: Optional[int] = 0
    TWOP: Optional[float] = 0
    THR: Optional[int] = 0
    THRA: Optional[int] = 0
    THRP: Optional[float] = 0
    FT: Optional[int] = 0
    FTA: Optional[int] = 0
    FTP: Optional[float] = 0
    ORB: Optional[int] = 0
    DRB: Optional[int] = 0
    TRB: Optional[int] = 0
    AST: Optional[int] = 0
    BLK: Optional[int] = 0
    STL: Optional[int] = 0
    TOV: Optional[int] = 0
    PF: Optional[int] = 0
    PTS: Optional[int] = 0


with conn:

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.get("/items/{item_id}")
    def read_item(item_id: int, q: Optional[str] = None):
        return {"item_id": item_id, "q": q}

    @app.get("/player")
    def read_all_players():
        cur = conn.cursor()
        cur.execute("SELECT * FROM player order by name")

        players = cur.fetchall()
        cur.close()
        return {"players": players}

    @app.get("/player/name/{name}")
    def read_player_name(name: str):
        cur = conn.cursor()
        cur.execute(
            f"SELECT name FROM player where name = '{name}' order by name")

        players = cur.fetchall()
        cur.close()
        return {"players": players}

    @app.get("/player/stats/{player_id}")
    def read_player_stats(player_id: str):
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM playerStats where player_id = '{player_id}'")

        player_stats = cur.fetchall()
        cur.close()
        return {"player_stats": player_stats}

    @app.post("/player")
    def insert_one_player(player: NameInsert):
        cur = conn.cursor()
        cur.execute(f"INSERT INTO Player(name) values ('{player.name}')")

        conn.commit()
        cur.close()
        return f"Insert player's name {player.name} into Player Table"

    @app.put("/player")
    def update_one_player_name(player: NameUpdate):
        cur = conn.cursor()
        cur.execute(
            f"Update Player SET name = '{player.name}' where id = {player.id}")

        conn.commit()
        cur.close()
        return f"Update {player.id}'s name {player.name} in Player Table"

    @app.delete("/player")
    def delete_one_player(player: DataDelete):
        cur = conn.cursor()
        cur.execute(
            f"Delete from Player where id = {player.id}")

        conn.commit()
        cur.close()
        return f"Delete player: {player.id} in Player Table"

    @app.get("/team")
    def read_all_teams():
        cur = conn.cursor()
        cur.execute("SELECT * FROM team order by name")

        teams = cur.fetchall()
        cur.close()
        return {"teams": teams}

    @app.post("/team")
    def insert_one_team(team: NameInsert):
        cur = conn.cursor()
        cur.execute(f"INSERT INTO team(name) values ('{team.name}')")

        conn.commit()
        cur.close()
        return f"Insert team's name {team.name} into Team Table"

    @app.put("/team")
    def update_one_team_name(team: NameUpdate):
        cur = conn.cursor()
        cur.execute(
            f"Update team SET name = '{team.name}' where id = {team.id}")

        conn.commit()
        cur.close()
        return f"Update {team.id}'s name {team.name} in team Table"

    @app.delete("/team")
    def delete_one_team(team: DataDelete):
        cur = conn.cursor()
        cur.execute(
            f"Delete from team where id = {team.id}")

        conn.commit()
        cur.close()
        return f"Delete team: {team.id} in team Table"

    @app.get("/season")
    def read_all_seasons():
        cur = conn.cursor()
        cur.execute("SELECT * FROM season order by start_date")

        seasons = cur.fetchall()
        cur.close()
        return {"seasons": seasons}

    @app.post("/season")
    def insert_one_season(season: SeasonInsert):
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO Season(name, start_date, end_date) values ('{season.name}', '{season.start_date}', '{season.end_date}')")

        conn.commit()
        cur.close()
        return f"Insert season's name {season.name} into Season Table"

    @app.put("/season")
    def update_one_season(season: SeasonUpdate):
        cur = conn.cursor()
        cur.execute(
            f"UPDATE Season SET name = '{season.name}', start_date = '{season.start_date}', end_date = '{season.end_date}' where id = '{season.id}'")

        conn.commit()
        cur.close()
        return f"Update {season.name}'s data  into Season Table"

    @app.delete("/season")
    def delete_one_season(season: DataDelete):
        cur = conn.cursor()
        cur.execute(
            f"Delete from Season where id = {season.id}")

        conn.commit()
        cur.close()
        return f"Delete Season: {season.id} in Season Table"

    @app.get("/match")
    def read_all_matches():
        cur = conn.cursor()
        cur.execute("""SELECT m.id, m.date, s.name as season, t1.name as home, t2.name as away,
        m.season_id, m.home_team_id, m.away_team_id
        FROM match m, season s, team t1, team t2
        where m.season_id = s.id and m.home_team_id = t1.id and m.away_team_id = t2.id
        order by m.date""")

        matches = cur.fetchall()
        cur.close()
        return {"matches": matches}

    @app.post("/match")
    def insert_one_match(match: MatchInsert):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO Match(date, season_id, home_team_id, away_team_id) values (%s, %s, %s, %s)
        """, (match.date, match.season_id, match.home_team_id, match.away_team_id))

        conn.commit()
        cur.close()
        return f"Insert match's into Match Table"

    @app.put("/match")
    def update_one_match(match: MatchUpdate):
        cur = conn.cursor()
        cur.execute(
            """
            Update Match SET date = %s, season_id = %s, home_team_id = %s, away_team_id = %s
            where id = %s
        """, (match.date, match.season_id, match.home_team_id, match.away_team_id, match.id))

        conn.commit()
        cur.close()
        return f"Insert match's into Match Table"

    @app.delete("/match")
    def delete_one_season(match: DataDelete):
        cur = conn.cursor()
        cur.execute(
            f"Delete from Match where id = {match.id}")

        conn.commit()
        cur.close()
        return f"Delete Match: {match.id} in Match Table"

    @app.get("/playerstats/match/{match_id}")
    def read_stats_by_match(match_id: int):
        cur = conn.cursor()

        cur.execute(f"SELECT * FROM playerStats where match_id = {match_id}")

        stats = cur.fetchall()
        cur.close()
        return {"stats": stats}

    @app.get("/playerstats/player/{player_name}")
    def read_stats_by_match(player_name: str):
        cur = conn.cursor()

        cur.execute(f"""SELECT ps.id, m.date as "Match Date", t1.name as "Home", t2.name as "Away", ps.home_or_away, ps.age, ps.pts, ps.ast, ps.trb, ps.stl, ps.blk,
		ps.tov
        FROM playerStats ps, player p, match m, season s, team t1, team t2
                where ps.player_id = p.id and ps.match_id = m.id and
                m.home_team_id = t1.id and m.away_team_id = t2.id and
                m.season_id = s.id and
                p.name = '{player_name}'
        order by m.date""")

        stats = cur.fetchall()

        cur.close()
        return {"stats": stats}

    @app.post("/playerstats")
    def insert_one_player_stats(stats: StatsInsert):
        cur = conn.cursor()

        cur.execute(f"""
        insert into PlayerStats (player_id, match_id, home_or_away, age, GS,MP,FG,FGA,"FG%","2P","2PA","2P%","3P","3PA","3P%",FT,FTA,"FT%",ORB,DRB,TRB,AST,STL,BLK,TOV,PF,PTS)
        values ('{stats.player_id}', '{stats.match_id}', '{stats.home_or_away}', '{stats.age}',
        {stats.GS}, {stats.MP}, {stats.FG}, {stats.FGA}, {stats.FGP}, {stats.TWO}, {stats.TWOA}, {stats.TWOP},
        {stats.THR}, {stats.THRA}, {stats.THRP}, {stats.FT}, {stats.FTA}, {stats.FTP}, {stats.ORB}, {stats.DRB}, {stats.TRB},
        {stats.AST}, {stats.STL}, {stats.BLK}, {stats.TOV}, {stats.PF}, {stats.PTS})""")

        conn.commit()
        cur.close()
        return "Insert player stats success"

    @app.put("/playerstats")
    def update_one_player_stats(stats: StatsUpdate):
        cur = conn.cursor()
        cur.execute(f"""update PlayerStats SET player_id={stats.player_id}, match_id={stats.match_id},
        home_or_away='{stats.home_or_away}', age='{stats.age}', GS={stats.GS},MP={stats.MP},
        FG={stats.FG},FGA={stats.FGA},"FG%"={stats.FGP},"2P"={stats.TWO},"2PA"={stats.TWOA},"2P%"={stats.TWOP},
        "3P"={stats.THR},"3PA"={stats.THRA},"3P%"={stats.THRP},FT={stats.FT},FTA={stats.FTA},"FT%"={stats.FTP},
        ORB={stats.ORB},DRB={stats.DRB},TRB={stats.TRB},AST={stats.AST},STL={stats.STL},
        BLK={stats.BLK},TOV={stats.TOV},PF={stats.PF},PTS={stats.PTS}
        where id = {stats.id}""")

        conn.commit()
        cur.close()
        return "Update player stats success"

    @app.delete("/playerstats")
    def delete_one_player_stats(stats: DataDelete):
        cur = conn.cursor()
        cur.execute("""delete from PlayerStats where id = %s""", [stats.id])

        conn.commit()
        cur.close()
        return f"Delete player stats {stats.id}  success"

    @app.get("/top/sum")
    def read_top_ten_by_sum():
        cur = conn.cursor()
        cur.execute("""
        SELECT a.id,a.Name,Sum(b.pts) FROM Player a
            inner join playerstats b on a.id=b.player_id
            Group by a.id,a.name
            order by sum(b.pts) desc
            Limit 10
        """)

        stats = cur.fetchall()

        cur.close()
        return {"stats": stats}

    @app.get("/top/avg")
    def read_top_ten_by_avg():
        cur = conn.cursor()
        cur.execute("""
        SELECT a.id,a.Name, ROUND(avg(b.pts), 2) FROM Player a
            inner join playerstats b on a.id=b.player_id
            Group by a.id,a.name
            order by avg(b.pts) desc
            Limit 10
        """)

        stats = cur.fetchall()

        cur.close()
        return {"stats": stats}

    @app.get("/player/with/{player_name}")
    def read_stats_with_team(player_name: str):
        cur = conn.cursor()
        cur.execute(f"""
        select t.name,
            ROUND(AVG(ps.pts), 2) as points_avg,
            ROUND(AVG(ps.ast), 2) as assists_avg,
            ROUND(AVG(ps.trb), 2) as rebounds_avg
            from playerStats ps, player p, match m, team t
            where ps.player_id = p.id and p.name = '{player_name}'
            		and ps.match_id = m.id and (
            			case
            				when ps.home_or_away = 'home' then m.home_team_id = t.id
            				else m.away_team_id = t.id
            			end)
            group by t.name
        """)

        stats = cur.fetchall()

        cur.close()
        return {"stats": stats}

    @app.get("/player/against/{player_name}")
    def read_stats_against_team(player_name: str):
        cur = conn.cursor()
        cur.execute(f"""
        select t.name,
            ROUND(AVG(ps.pts), 2) as points_avg,
            ROUND(AVG(ps.ast), 2) as assists_avg,
            ROUND(AVG(ps.trb), 2) as rebounds_avg
            from playerStats ps, player p, match m, team t
            where ps.player_id = p.id and p.name = '{player_name}'
            		and ps.match_id = m.id and (
            			case
            				when ps.home_or_away = 'home' then m.away_team_id = t.id
            				else m.home_team_id = t.id
            			end)
            group by t.name
        """)

        stats = cur.fetchall()

        cur.close()
        return {"stats": stats}

    @app.get("/player/month/{player_name}")
    def read_stats_with_month(player_name: str):
        cur = conn.cursor()
        cur.execute(f"""
        select to_char(m.date,'Mon'),
            ROUND(AVG(ps.pts), 2) as points_avg,
            ROUND(AVG(ps.ast), 2) as assists_avg,
            ROUND(AVG(ps.trb), 2) as rebounds_avg
            from playerStats ps, player p, match m, team t
            where ps.player_id = p.id and p.name = '{player_name}'
            		and ps.match_id = m.id
            group by to_char(m.date,'Mon')
        """)

        stats = cur.fetchall()

        cur.close()
        return {"stats": stats}

default_stats = {
    "player_id": 1,
    "match_id": 1,
    "home_or_away": "home",
    "age": "20-145",
    "GS": 0,
    "MP": 0,
    "FG": 0,
    "FGA": 0,
    "FGP": 0,
    "TWO": 0,
    "TWOA": 0,
    "TWOP": 0,
    "THR": 0,
    "THRA": 0,
    "THRP": 0,
    "FT": 0,
    "FTA": 0,
    "FTP": 0,
    "ORB": 0,
    "DRB": 0,
    "TRB": 0,
    "AST": 0,
    "BLK": 0,
    "STL": 0,
    "TOV": 0,
    "PF": 0,
    "PTS": 0
}
