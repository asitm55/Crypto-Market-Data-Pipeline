import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob

# Set the style for seaborn
sns.set_theme(style="whitegrid")

# Create output directory for visuals
VISUALS_DIR = os.path.join("data", "visualizations")
os.makedirs(VISUALS_DIR, exist_ok=True)

def plot_top_winners():
    """Reads the top_winners.parquet and generates a bar chart."""
    path = os.path.join("data", "processed", "rankings", "top_winners.parquet")
    if not os.path.exists(path):
        print("No top winners data found. Run transform.py first.")
        return

    df = pd.read_parquet(path)
    
    plt.figure(figsize=(10, 6))
    barplot = sns.barplot(
        x="changePercent24Hr", 
        y="name", 
        data=df, 
        palette="viridis",
        orient='h'
    )
    
    plt.title("Top 5 Crypto Winners (24h % Change)", fontsize=16, pad=15)
    plt.xlabel("24h Percentage Change (%)", fontsize=12)
    plt.ylabel("Asset Name", fontsize=12)
    
    # Add exact values to the end of each bar
    for index, value in enumerate(df["changePercent24Hr"]):
        plt.text(value, index, f" +{value:.2f}%", va='center')

    plt.tight_layout()
    out_file = os.path.join(VISUALS_DIR, "top_winners.png")
    plt.savefig(out_file, dpi=300)
    plt.close()
    print(f"✅ Saved Top Winners chart to: {out_file}")

def plot_historical_sma():
    """Reads the sma.parquet files for each coin and plots their 7-day SMAs."""
    sma_files = glob.glob(os.path.join("data", "processed", "sma", "**", "*.parquet"), recursive=True)
    
    if not sma_files:
        print("No SMA data found. Run transform.py first.")
        return

    dfs = [pd.read_parquet(f) for f in sma_files]
    df_all = pd.concat(dfs, ignore_index=True)
    
    df_all["date"] = pd.to_datetime(df_all["date"])
    df_all = df_all.sort_values(by="date")

    plt.figure(figsize=(12, 6))
    
    sns.lineplot(
        x="date", 
        y="sma_7d", 
        hue="coin_id", 
        data=df_all,
        linewidth=2.5,
        marker='o'
    )

    plt.title("7-Day Simple Moving Average (SMA) Over Time", fontsize=16, pad=15)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Price (USD) - SMA", fontsize=12)
    plt.xticks(rotation=45)
    plt.legend(title="Coin")
    
    # Use logarithmic scale to make it readable across vastly different bounds
    plt.yscale('log')
    plt.tight_layout()
    
    out_file = os.path.join(VISUALS_DIR, "historical_sma_log.png")
    plt.savefig(out_file, dpi=300)
    plt.close()
    print(f"✅ Saved Historical SMA chart to: {out_file}")

if __name__ == "__main__":
    print(f"Generating visualizations from processed metrics...")
    plot_top_winners()
    plot_historical_sma()
    print(f"Visualizations complete! Check the '{VISUALS_DIR}' directory.")
