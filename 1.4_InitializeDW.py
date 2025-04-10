import sqlite3
from datetime import datetime, timedelta

class BaseballDataWarehouse:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to the SQLite database"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        print(f"Connected to database: {self.db_path}")
        
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed")
            
    def execute_query(self, query, params=None, fetch=True):
        """Execute a SQL query and optionally return results"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.conn.commit()  # Ensure changes are saved
            return self.cursor.fetchall() if fetch else None
        except sqlite3.Error as e:
            print(f"Error executing query: {e}\nQuery: {query}")
            self.conn.rollback()  # Rollback on error
            return None

    def build_data_warehouse(self):
        """Build the complete data warehouse"""
        print("\nStarting data warehouse build...")
        start_time = datetime.now()
        
        self.connect()
        
        try:
            # Verify source tables exist first
            if not self._verify_source_tables():
                print("Aborting build due to missing source tables")
                return
            
            # Create all tables with explicit error checking
            self._create_dimension_tables()
            self._create_fact_tables()
            self._create_indexes()
            
            # Verify counts
            self._verify_table_counts()
            
        except Exception as e:
            print(f"Error during build: {e}")
            self.conn.rollback()
        finally:
            self.close()
        
        duration = datetime.now() - start_time
        print(f"\nData warehouse build completed in {duration.total_seconds():.2f} seconds")

    def _verify_source_tables(self):
        """Verify all required source tables exist"""
        required_tables = ['gameinfo', 'batting', 'pitching', 'allplayers', 'teamstats', 'fielding']
        missing_tables = []
        
        for table in required_tables:
            result = self.execute_query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", 
                (table,)
            )
            if not result:
                missing_tables.append(table)
        
        if missing_tables:
            print(f"ERROR: Missing required source tables: {', '.join(missing_tables)}")
            return False
        return True

    def _create_dimension_tables(self):
        """Create all dimension tables with error checking"""
        print("\nCreating dimension tables...")
        
        # Drop existing tables first to ensure clean slate
        self.execute_query("DROP TABLE IF EXISTS DimPlayer;", fetch=False)
        self.execute_query("DROP TABLE IF EXISTS DimTeam;", fetch=False)
        self.execute_query("DROP TABLE IF EXISTS DimGame;", fetch=False)
        self.execute_query("DROP TABLE IF EXISTS DimDate;", fetch=False)
        
        # DimPlayer
        print("Creating DimPlayer...")
        self.execute_query("""
        CREATE TABLE DimPlayer AS
        SELECT DISTINCT
            id AS player_id,
            first || ' ' || last AS player_name,
            first AS first_name,
            last AS last_name,
            bat AS batting_hand,
            throw AS throwing_hand
        FROM allplayers;
        """, fetch=False)
        
        # DimTeam
        print("Creating DimTeam...")
        self.execute_query("""
        CREATE TABLE DimTeam AS
        SELECT DISTINCT team AS team_id FROM (
            SELECT team FROM allplayers UNION
            SELECT visteam FROM gameinfo UNION
            SELECT hometeam FROM gameinfo UNION
            SELECT team FROM teamstats UNION
            SELECT team FROM batting UNION
            SELECT team FROM pitching UNION
            SELECT team FROM fielding
        );
        """, fetch=False)
        
        # DimGame
        print("Creating DimGame...")
        self.execute_query("""
        CREATE TABLE DimGame AS
        SELECT 
            gid AS game_id,
            date AS game_date,
            season,
            site AS ballpark,
            gametype,
            daynight,
            usedh,
            innings AS scheduled_innings,
            tiebreaker,
            htbf AS home_team_bat_first,
            timeofgame AS game_duration_minutes,
            attendance,
            fieldcond AS field_condition,
            precip AS precipitation,
            sky AS sky_conditions,
            temp AS temperature,
            winddir AS wind_direction,
            windspeed AS wind_speed,
            forfeit,
            suspend AS suspended,
            vruns AS visitor_runs,
            hruns AS home_runs,
            wteam AS winning_team,
            lteam AS losing_team,
            visteam AS visiting_team_id,
            hometeam AS home_team_id
        FROM gameinfo;
        """, fetch=False)
        
        # DimDate
        print("Creating DimDate...")
        min_date_result = self.execute_query("SELECT MIN(date) FROM gameinfo;")
        max_date_result = self.execute_query("SELECT MAX(date) FROM gameinfo;")
        
        if not min_date_result or not max_date_result:
            print("Warning: Could not determine date range for DimDate")
            return
            
        min_date = min_date_result[0][0]
        max_date = max_date_result[0][0]
        
        # Handle different date formats
        try:
            if isinstance(min_date, int):
                min_date = datetime.fromtimestamp(min_date).strftime("%Y-%m-%d")
                max_date = datetime.fromtimestamp(max_date).strftime("%Y-%m-%d")
            else:
                # Try to parse as string
                datetime.strptime(min_date, "%Y-%m-%d")
        except ValueError:
            print(f"Warning: Unsupported date format: {min_date}")
            return
        
        self.execute_query("""
        CREATE TABLE DimDate (
            date_id TEXT PRIMARY KEY,
            date TEXT,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_week INTEGER,
            week_of_year INTEGER,
            season_period TEXT
        );
        """, fetch=False)
        
        current_date = datetime.strptime(min_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(max_date, "%Y-%m-%d").date()
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            year = current_date.year
            month = current_date.month
            day = current_date.day
            day_of_week = current_date.weekday()
            week_of_year = current_date.isocalendar()[1]
            season_period = "Regular" if 3 <= month <= 10 else "Offseason"
            
            self.execute_query("""
            INSERT INTO DimDate VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (date_str, date_str, year, month, day, day_of_week, week_of_year, season_period), fetch=False)
            
            current_date += timedelta(days=1)
        
        print("Created all dimension tables")

    def _create_fact_tables(self):
        """Create all fact tables with explicit error checking"""
        print("\nCreating fact tables...")
        
        # Drop existing tables first
        self.execute_query("DROP TABLE IF EXISTS FactBatting;", fetch=False)
        self.execute_query("DROP TABLE IF EXISTS FactPitching;", fetch=False)
        self.execute_query("DROP TABLE IF EXISTS FactGameOutcomes;", fetch=False)
        
        # FactBatting
        print("Creating FactBatting...")
        self.execute_query("""
        CREATE TABLE FactBatting AS
        SELECT
            b.gid AS game_id,
            b.id AS player_id,
            b.team AS team_id,
            g.date AS game_date,
            b.b_pa AS plate_appearances,
            b.b_ab AS at_bats,
            b.b_r AS runs,
            b.b_h AS hits,
            b.b_d AS doubles,
            b.b_t AS triples,
            b.b_hr AS home_runs,
            b.b_rbi AS rbi,
            b.b_sh AS sacrifice_hits,
            b.b_sf AS sacrifice_flies,
            b.b_hbp AS hit_by_pitch,
            b.b_w AS walks,
            b.b_iw AS intentional_walks,
            b.b_k AS strikeouts,
            b.b_sb AS stolen_bases,
            b.b_cs AS caught_stealing,
            b.b_gdp AS grounded_into_double_plays,
            b.b_xi AS catcher_interference,
            b.b_roe AS reached_on_error,
            CASE WHEN b.dh = 1 THEN 1 ELSE 0 END AS is_dh,
            CASE WHEN b.ph = 1 THEN 1 ELSE 0 END AS is_pinch_hitter,
            CASE WHEN b.pr = 1 THEN 1 ELSE 0 END AS is_pinch_runner
        FROM batting b
        JOIN gameinfo g ON b.gid = g.gid;
        """, fetch=False)
        
        # Verify FactBatting was created
        result = self.execute_query("SELECT COUNT(*) FROM FactBatting;")
        print(f"FactBatting created with {result[0][0] if result else 0} records")
        
        # FactPitching
        print("Creating FactPitching...")
        self.execute_query("""
        CREATE TABLE FactPitching AS
        SELECT
            p.gid AS game_id,
            p.id AS player_id,
            p.team AS team_id,
            g.date AS game_date,
            p.p_ipouts AS outs_recorded,
            p.p_bfp AS batters_faced,
            p.p_h AS hits_allowed,
            p.p_r AS runs_allowed,
            p.p_er AS earned_runs,
            p.p_w AS walks_allowed,
            p.p_iw AS intentional_walks_allowed,
            p.p_k AS strikeouts,
            p.p_hbp AS hit_batters,
            p.p_wp AS wild_pitches,
            p.p_bk AS balks,
            p.p_sh AS sacrifice_hits_allowed,
            p.p_sf AS sacrifice_flies_allowed,
            p.p_hr AS home_runs_allowed,
            CASE WHEN p.wp = 1 THEN 1 ELSE 0 END AS is_winning_pitcher,
            CASE WHEN p.lp = 1 THEN 1 ELSE 0 END AS is_losing_pitcher,
            CASE WHEN p.save = 1 THEN 1 ELSE 0 END AS is_save
        FROM pitching p
        JOIN gameinfo g ON p.gid = g.gid;
        """, fetch=False)
        
        # Verify FactPitching was created
        result = self.execute_query("SELECT COUNT(*) FROM FactPitching;")
        print(f"FactPitching created with {result[0][0] if result else 0} records")
        
        # FactGameOutcomes
        print("Creating FactGameOutcomes...")
        self.execute_query("""
        CREATE TABLE FactGameOutcomes AS
        SELECT
            g.gid AS game_id,
            g.date AS game_date,
            g.season,
            g.visteam AS visiting_team_id,
            g.hometeam AS home_team_id,
            g.vruns AS visiting_runs,
            g.hruns AS home_runs,
            CASE WHEN g.vruns > g.hruns THEN 1 ELSE 0 END AS visiting_win,
            CASE WHEN g.hruns > g.vruns THEN 1 ELSE 0 END AS home_win,
            CASE WHEN g.vruns = g.hruns THEN 1 ELSE 0 END AS tie,
            g.timeofgame AS game_duration_minutes,
            g.attendance
        FROM gameinfo g;
        """, fetch=False)
        
        # Verify FactGameOutcomes was created
        result = self.execute_query("SELECT COUNT(*) FROM FactGameOutcomes;")
        print(f"FactGameOutcomes created with {result[0][0] if result else 0} records")
        
        print("Created all fact tables")

    def _create_indexes(self):
        """Create performance indexes"""
        print("\nCreating indexes...")
        
        # DimPlayer indexes
        self.execute_query("CREATE INDEX IF NOT EXISTS idx_player_id ON DimPlayer(player_id);", fetch=False)
        
        # DimTeam indexes
        self.execute_query("CREATE INDEX IF NOT EXISTS idx_team_id ON DimTeam(team_id);", fetch=False)
        
        # DimGame indexes
        self.execute_query("CREATE INDEX IF NOT EXISTS idx_game_id ON DimGame(game_id);", fetch=False)
        self.execute_query("CREATE INDEX IF NOT EXISTS idx_game_date ON DimGame(game_date);", fetch=False)
        
        # Fact tables indexes
        self.execute_query("CREATE INDEX IF NOT EXISTS idx_fact_batting_game ON FactBatting(game_id);", fetch=False)
        self.execute_query("CREATE INDEX IF NOT EXISTS idx_fact_batting_player ON FactBatting(player_id);", fetch=False)
        self.execute_query("CREATE INDEX IF NOT EXISTS idx_fact_pitching_game ON FactPitching(game_id);", fetch=False)
        self.execute_query("CREATE INDEX IF NOT EXISTS idx_fact_pitching_player ON FactPitching(player_id);", fetch=False)
        self.execute_query("CREATE INDEX IF NOT EXISTS idx_fact_outcomes_game ON FactGameOutcomes(game_id);", fetch=False)
        
        print("Created indexes")

    def _verify_table_counts(self):
        """Verify table record counts"""
        print("\nVerifying table counts:")
        
        tables = [
            "DimPlayer", "DimTeam", "DimGame", "DimDate",
            "FactBatting", "FactPitching", "FactGameOutcomes"
        ]
        
        for table in tables:
            try:
                count = self.execute_query(f"SELECT COUNT(*) FROM {table};")
                if count:
                    print(f"{table:20}: {count[0][0]:,} records")
                else:
                    print(f"{table:20}: Could not retrieve count")
            except sqlite3.Error as e:
                print(f"{table:20}: ERROR - {str(e)}")

    def query_player_stats(self, player_name=None, season=None, limit=5):
        """Get player statistics with optional filters"""
        # First check if FactBatting exists
        result = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='FactBatting';"
        )
        if not result:
            print("Error: FactBatting table does not exist")
            return []
        
        query = """
        SELECT 
            p.player_name,
            p.batting_hand,
            SUM(b.plate_appearances) AS PA,
            SUM(b.at_bats) AS AB,
            SUM(b.hits) AS H,
            SUM(b.home_runs) AS HR,
            SUM(b.rbi) AS RBI,
            SUM(b.stolen_bases) AS SB,
            ROUND(SUM(b.hits)*1.0/NULLIF(SUM(b.at_bats), 0), 3) AS AVG
        FROM DimPlayer p
        JOIN FactBatting b ON p.player_id = b.player_id
        WHERE 1=1
        """
        
        params = []
        if player_name:
            query += " AND p.player_name LIKE ?"
            params.append(f"%{player_name}%")
        if season:
            query += " AND strftime('%Y', b.game_date) = ?"
            params.append(str(season))
            
        query += " GROUP BY p.player_id ORDER BY SUM(b.hits) DESC LIMIT ?;"
        params.append(limit)
        
        results = self.execute_query(query, params)
        if not results:
            print("No player stats found")
            return []
        
        return [dict(row) for row in results]

    def query_team_performance(self, team_id, season=None):
        """Get team performance summary"""
        # First check if FactGameOutcomes exists
        result = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='FactGameOutcomes';"
        )
        if not result:
            print("Error: FactGameOutcomes table does not exist")
            return None
        
        query = """
        SELECT 
            t.team_id,
            COUNT(*) AS games_played,
            SUM(CASE WHEN (g.visiting_team_id = t.team_id AND g.visiting_win = 1) OR 
                        (g.home_team_id = t.team_id AND g.home_win = 1) THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN (g.visiting_team_id = t.team_id AND g.visiting_win = 0 AND g.tie = 0) OR 
                        (g.home_team_id = t.team_id AND g.home_win = 0 AND g.tie = 0) THEN 1 ELSE 0 END) AS losses,
            SUM(g.tie) AS ties,
            SUM(CASE WHEN g.visiting_team_id = t.team_id THEN g.visiting_runs ELSE g.home_runs END) AS runs_scored,
            SUM(CASE WHEN g.visiting_team_id = t.team_id THEN g.home_runs ELSE g.visiting_runs END) AS runs_allowed,
            ROUND(AVG(g.attendance)) AS avg_attendance
        FROM DimTeam t
        JOIN FactGameOutcomes g ON t.team_id IN (g.visiting_team_id, g.home_team_id)
        WHERE t.team_id = ?
        """
        
        params = [team_id]
        if season:
            query += " AND g.season = ?"
            params.append(season)
            
        query += " GROUP BY t.team_id;"
        
        results = self.execute_query(query, params)
        return dict(results[0]) if results else None


if __name__ == "__main__":
    dw = BaseballDataWarehouse("baseball-database.db")
    dw.build_data_warehouse()
    
    # Example queries
    dw.connect()
    try:
        print("\nTop 5 players by hits:")
        players = dw.query_player_stats(limit=5)
        if players:
            for player in players:
                print(f"{player['player_name']:20} {player['H']:4} hits ({player['AVG']:.3f} AVG)")
        else:
            print("No player stats available")
        
        print("\nTeam performance:")
        team_perf = dw.query_team_performance("ANA")  # Use actual team ID from your data
        if team_perf:
            print(f"Team {team_perf['team_id']}: {team_perf['wins']}-{team_perf['losses']}-{team_perf['ties']}")
            print(f"Runs: {team_perf['runs_scored']} scored, {team_perf['runs_allowed']} allowed")
        else:
            print("No team performance data available")
    finally:
        dw.close()