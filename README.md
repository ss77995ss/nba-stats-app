# nba-stats-app

### How we built tables

Initially, we only have one relation `PlayerStats` with a set of our raw data. To separate the data, we find some functional dependencies that hold true in the relation. In order to get rid of functional dependencies, we divided the `PlayerStats` table into
`Player`, `Team`, `Match` and `PlayerStats` table. We also added `Season` table since we can select stats data by season
but not by year. We add primary key `id` to represent each table and let them satisfy BCNF. To create the relationship between
each table, we use foreign key to have reference and set constraint on the it. We all use set cascade to ensure when we
update the data it'll not have serious damage.

PlayerStats has player's, match's reference.
Match has season's and team's reference.

### How we imported data

We downloaded the data from Kaggle(https://www.kaggle.com/quexington/nba-each-game-player-stats-2013-2018) as csv file.
Then, we used python script to load the csv and change them into insert queries. First, we used pandas to get all the
team name. Second, we created season object by each season's start date and end date. Third, I got all the players' name
(Need to solve symbol like `'`). Finally, we created match and playerstats insert queries together. Use column 'home_or_away'
to decide which team is home team and which team is away team. Furthermore, since we use 'id' as foreign key, we need to
use 'select' in our insert queries so that we can get the 'id' to represent related tuple.

Steps on how to load the data,
1.Run the queries in create.sql in PostgreSQL to create the tables
2.Run the python script which will create load.sql file in the same directory which will contain the queries to load data into the tables that were just created. (Make sure that the python script and the .csv files are in the same directory)
3.Open load.sql that just got created, copy the queries within the file and paste it in PostgreSQL, the data will then be added to each relation in the database.

### How to test the UI

- Open terminal or command line
- Go to backend folder and execute the command uvicorn main:app --reload (Might need to pip install all the packages)
- Create another tab and go to frontend folder then type command yarn install then yarn start
- Go to website and go to url localhost:3000
- Test the UI
