import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize visualization style
sns.set(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

class BaseballAnalyzer:
    def __init__(self, db_path="baseball-database.db"):
        self.db_path = db_path
    
    def run_analysis(self):
        """Run two key analyses without date joins"""
        self.plot_top_batters()
        self.plot_pitching_performance()
        plt.show()
    
    def plot_top_batters(self):
        """
        Analyze top 10 batters by batting average
        Use Case: Quickly identify your best hitters for lineup optimization
        """
        with sqlite3.connect(self.db_path) as conn:
            query = """
            SELECT 
                p.player_name,
                SUM(b.at_bats) AS at_bats,
                SUM(b.hits) AS hits,
                ROUND(SUM(b.hits)*1.0/SUM(b.at_bats), 3) AS batting_avg
            FROM FactBatting b
            JOIN DimPlayer p ON b.player_id = p.player_id
            WHERE b.at_bats > 0
            GROUP BY p.player_id
            HAVING SUM(b.at_bats) > 100
            ORDER BY batting_avg DESC
            LIMIT 10;
            """
            df = pd.read_sql(query, conn)
        
        # Plotting
        plt.figure()
        sns.barplot(x='batting_avg', y='player_name', data=df, palette='viridis')
        plt.title('Top 10 Batters by Batting Average')
        plt.xlabel('Batting Average')
        plt.ylabel('Player')
        
        # Add value annotations
        for i, val in enumerate(df['batting_avg']):
            plt.text(val, i, f"{val:.3f}", va='center')
    
    def plot_pitching_performance(self):
        """
        Analyze pitcher performance using ERA
        Use Case: Evaluate starting pitchers for rotation decisions
        """
        with sqlite3.connect(self.db_path) as conn:
            query = """
            SELECT 
                pl.player_name,  -- Changed from p.player_name to pl.player_name
                SUM(p.outs_recorded)/3.0 AS innings_pitched,
                SUM(p.earned_runs) AS earned_runs,
                CASE WHEN SUM(p.outs_recorded) > 0 
                     THEN 9*SUM(p.earned_runs)/(SUM(p.outs_recorded)/3.0) 
                     ELSE 0 END AS ERA
            FROM FactPitching p
            JOIN DimPlayer pl ON p.player_id = pl.player_id  -- Correct alias used
            GROUP BY pl.player_id
            HAVING SUM(p.outs_recorded) >= 30
            ORDER BY ERA ASC
            LIMIT 15;
            """
            df = pd.read_sql(query, conn)
        
        # Plotting
        plt.figure()
        sns.barplot(x='ERA', y='player_name', data=df, palette='rocket')
        plt.title('Top 15 Pitchers by ERA (Lower is Better)')
        plt.xlabel('ERA')
        plt.ylabel('Pitcher')
        
        # Add value annotations
        for i, val in enumerate(df['ERA']):
            plt.text(val, i, f"{val:.2f}", va='center')

# Run the analysis
if __name__ == "__main__":
    analyzer = BaseballAnalyzer()
    analyzer.run_analysis()