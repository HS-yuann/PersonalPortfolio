"""
Real-time Crypto Insights Dashboard Visualization

Visualizes data from enhanced_crypto_insights.db:
- Price movements with volatility
- Volume spikes
- Market sentiment (Fear & Greed Index)
- Sentiment-price correlations
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import seaborn as sns

# Set style
sns.set_style("darkgrid")
plt.rcParams['figure.figsize'] = (15, 10)

def load_data(db_path='enhanced_crypto_insights.db'):
    """Load all data from database."""
    conn = sqlite3.connect(db_path)

    # Load all tables
    price_movements = pd.read_sql_query("SELECT * FROM price_movements ORDER BY window_end", conn)
    volume_spikes = pd.read_sql_query("SELECT * FROM volume_spikes ORDER BY spike_time", conn)
    market_sentiment = pd.read_sql_query("SELECT * FROM market_sentiment ORDER BY recorded_time", conn)
    sentiment_correlation = pd.read_sql_query("SELECT * FROM sentiment_price_correlation ORDER BY window_end", conn)

    conn.close()

    # Convert timestamps
    if not price_movements.empty:
        price_movements['window_end'] = pd.to_datetime(price_movements['window_end'])
    if not volume_spikes.empty:
        volume_spikes['spike_time'] = pd.to_datetime(volume_spikes['spike_time'])
    if not market_sentiment.empty:
        market_sentiment['recorded_time'] = pd.to_datetime(market_sentiment['recorded_time'])
    if not sentiment_correlation.empty:
        sentiment_correlation['window_end'] = pd.to_datetime(sentiment_correlation['window_end'])

    return {
        'price_movements': price_movements,
        'volume_spikes': volume_spikes,
        'market_sentiment': market_sentiment,
        'sentiment_correlation': sentiment_correlation
    }

def plot_price_movements(df):
    """Plot price movements with volatility."""
    if df.empty:
        print("No price movement data available yet.")
        return

    fig, axes = plt.subplots(3, 1, figsize=(15, 10))
    fig.suptitle('Crypto Price Movements & Volatility', fontsize=16, fontweight='bold')

    symbols = df['symbol'].unique()
    colors = {'BTCUSDT': 'orange', 'ETHUSDT': 'blue', 'BNBUSDT': 'yellow'}

    for i, symbol in enumerate(symbols[:3]):
        ax = axes[i]
        symbol_data = df[df['symbol'] == symbol].copy()

        # Plot price change percentage
        ax2 = ax.twinx()
        ax.plot(symbol_data['window_end'], symbol_data['avg_price'],
                color=colors.get(symbol, 'gray'), linewidth=2, label=f'{symbol} Price')
        ax2.bar(symbol_data['window_end'], symbol_data['volatility'],
                alpha=0.3, color='red', label='Volatility')

        # Highlight significant moves
        significant = symbol_data[abs(symbol_data['price_change_pct']) > 0.5]
        if not significant.empty:
            ax.scatter(significant['window_end'], significant['avg_price'],
                      color='red', s=100, zorder=5, label='Significant Move')

        ax.set_ylabel('Price (USD)', fontsize=10)
        ax2.set_ylabel('Volatility', fontsize=10, color='red')
        ax.set_title(f'{symbol} Price Movements', fontsize=12, fontweight='bold')
        ax.legend(loc='upper left')
        ax2.legend(loc='upper right')
        ax.grid(True, alpha=0.3)

        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()
    return fig

def plot_volume_spikes(df):
    """Plot volume spike events."""
    if df.empty:
        print("No volume spike data available yet.")
        return

    fig, ax = plt.subplots(figsize=(15, 6))

    symbols = df['symbol'].unique()
    colors = {'BTCUSDT': 'orange', 'ETHUSDT': 'blue', 'BNBUSDT': 'yellow'}

    for symbol in symbols:
        symbol_data = df[df['symbol'] == symbol]
        ax.scatter(symbol_data['spike_time'], symbol_data['spike_multiplier'],
                  s=symbol_data['volume']/10000, alpha=0.6,
                  color=colors.get(symbol, 'gray'), label=symbol)

    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Spike Multiplier (vs Baseline)', fontsize=12)
    ax.set_title('Volume Spikes (Size = Volume)', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()
    return fig

def plot_sentiment(df):
    """Plot Fear & Greed Index over time."""
    if df.empty:
        print("No sentiment data available yet.")
        return

    fig, ax = plt.subplots(figsize=(15, 6))

    # Plot Fear & Greed Index
    ax.plot(df['recorded_time'], df['fear_greed_index'],
            linewidth=3, color='purple', marker='o', markersize=8)

    # Color zones
    ax.axhspan(0, 30, alpha=0.2, color='red', label='Extreme Fear')
    ax.axhspan(30, 45, alpha=0.1, color='orange', label='Fear')
    ax.axhspan(45, 55, alpha=0.1, color='gray', label='Neutral')
    ax.axhspan(55, 70, alpha=0.1, color='lightgreen', label='Greed')
    ax.axhspan(70, 100, alpha=0.2, color='green', label='Extreme Greed')

    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Fear & Greed Index', fontsize=12)
    ax.set_title('Market Sentiment (Fear & Greed Index)', fontsize=14, fontweight='bold')
    ax.set_ylim(0, 100)
    ax.legend(loc='upper right')
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()
    return fig

def plot_sentiment_correlation(df):
    """Plot sentiment-price correlation signals."""
    if df.empty:
        print("No sentiment correlation data available yet.")
        return

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))

    # Plot 1: Price changes vs Fear & Greed
    ax1.scatter(df['fear_greed_index'], df['btc_price_change'],
               s=100, alpha=0.6, color='orange', label='BTC')
    ax1.scatter(df['fear_greed_index'], df['eth_price_change'],
               s=100, alpha=0.6, color='blue', label='ETH')

    ax1.set_xlabel('Fear & Greed Index', fontsize=12)
    ax1.set_ylabel('Price Change %', fontsize=12)
    ax1.set_title('Sentiment vs Price Movement Correlation', fontsize=14, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=0, color='black', linestyle='--', linewidth=1)
    ax1.axvline(x=50, color='black', linestyle='--', linewidth=1)

    # Plot 2: Trading signals over time
    signal_colors = {
        'STRONG_BUY': 'darkgreen',
        'BUY_SIGNAL': 'lightgreen',
        'CORRELATION': 'gray',
        'NEUTRAL': 'lightgray',
        'SELL_SIGNAL': 'orange',
        'STRONG_SELL': 'red',
        'REVERSAL_UP': 'cyan',
        'REVERSAL_DOWN': 'purple'
    }

    for signal_type in df['correlation_signal'].unique():
        signal_data = df[df['correlation_signal'] == signal_type]
        ax2.scatter(signal_data['window_end'], signal_data['fear_greed_index'],
                   s=150, alpha=0.7, color=signal_colors.get(signal_type, 'gray'),
                   label=signal_type, marker='D')

    ax2.set_xlabel('Time', fontsize=12)
    ax2.set_ylabel('Fear & Greed Index', fontsize=12)
    ax2.set_title('Trading Signals Timeline', fontsize=14, fontweight='bold')
    ax2.legend(loc='upper right', ncol=2)
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, 100)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()
    return fig

def plot_summary_dashboard(data):
    """Create summary dashboard with all metrics."""
    fig = plt.figure(figsize=(18, 12))
    gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

    fig.suptitle('🔍 Real-Time Crypto Insights Dashboard',
                 fontsize=18, fontweight='bold', y=0.995)

    # Summary stats
    pm = data['price_movements']
    vs = data['volume_spikes']
    ms = data['market_sentiment']
    sc = data['sentiment_correlation']

    # Stats text
    stats_text = f"""
    📊 DATA SUMMARY

    Price Movements: {len(pm)} records
    Volume Spikes: {len(vs)} events
    Sentiment Updates: {len(ms)} records
    Correlation Signals: {len(sc)} signals

    Latest Prices:
    """

    if not pm.empty:
        for symbol in pm['symbol'].unique():
            latest = pm[pm['symbol'] == symbol].iloc[-1]
            stats_text += f"\n{symbol}: ${latest['avg_price']:.2f}"
            stats_text += f" ({latest['price_change_pct']:+.2f}%)"

    if not ms.empty:
        latest_sentiment = ms.iloc[-1]
        stats_text += f"\n\nSentiment: {latest_sentiment['fear_greed_class']}"
        stats_text += f" ({latest_sentiment['fear_greed_index']}/100)"

    # Add stats box
    ax_stats = fig.add_subplot(gs[0, 0])
    ax_stats.text(0.1, 0.5, stats_text, fontsize=11, family='monospace',
                  verticalalignment='center')
    ax_stats.axis('off')

    # Recent price changes
    ax_changes = fig.add_subplot(gs[0, 1])
    if not pm.empty:
        recent = pm.groupby('symbol').tail(10)
        for symbol in recent['symbol'].unique():
            symbol_data = recent[recent['symbol'] == symbol]
            ax_changes.plot(range(len(symbol_data)), symbol_data['price_change_pct'],
                          marker='o', label=symbol)
        ax_changes.set_title('Recent Price Changes', fontweight='bold')
        ax_changes.set_ylabel('Change %')
        ax_changes.set_xlabel('Recent Updates')
        ax_changes.legend()
        ax_changes.grid(True, alpha=0.3)
        ax_changes.axhline(y=0, color='black', linestyle='--', linewidth=1)

    # Volatility heatmap
    ax_vol = fig.add_subplot(gs[1, :])
    if not pm.empty and len(pm) > 20:
        pivot_data = pm.pivot_table(values='volatility',
                                    index='symbol',
                                    columns=pm.groupby('symbol').cumcount(),
                                    aggfunc='mean')
        sns.heatmap(pivot_data, cmap='YlOrRd', ax=ax_vol, cbar_kws={'label': 'Volatility'})
        ax_vol.set_title('Volatility Heatmap Over Time', fontweight='bold')
        ax_vol.set_xlabel('Time Window')

    # Signal distribution
    ax_signals = fig.add_subplot(gs[2, 0])
    if not sc.empty:
        signal_counts = sc['correlation_signal'].value_counts()
        signal_counts.plot(kind='barh', ax=ax_signals, color='steelblue')
        ax_signals.set_title('Trading Signal Distribution', fontweight='bold')
        ax_signals.set_xlabel('Count')
    else:
        ax_signals.text(0.5, 0.5, 'No correlation signals yet',
                       ha='center', va='center', fontsize=12)
        ax_signals.axis('off')

    # Market direction pie
    ax_direction = fig.add_subplot(gs[2, 1])
    if not sc.empty:
        direction_counts = sc['market_direction'].value_counts()
        colors_dir = {'BULLISH': 'green', 'BEARISH': 'red', 'MIXED': 'gray'}
        ax_direction.pie(direction_counts.values, labels=direction_counts.index,
                        colors=[colors_dir.get(d, 'gray') for d in direction_counts.index],
                        autopct='%1.1f%%', startangle=90)
        ax_direction.set_title('Market Direction Distribution', fontweight='bold')
    else:
        ax_direction.text(0.5, 0.5, 'No market direction data yet',
                         ha='center', va='center', fontsize=12)
        ax_direction.axis('off')

    return fig

def main():
    """Generate all visualizations."""
    print("="*60)
    print("📊 CRYPTO INSIGHTS VISUALIZATION DASHBOARD")
    print("="*60)
    print(f"\nLoading data from: enhanced_crypto_insights.db")
    print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Load data
    data = load_data()

    # Print summary
    print(f"Data Summary:")
    print(f"  Price Movements: {len(data['price_movements'])} records")
    print(f"  Volume Spikes: {len(data['volume_spikes'])} events")
    print(f"  Market Sentiment: {len(data['market_sentiment'])} records")
    print(f"  Sentiment Correlations: {len(data['sentiment_correlation'])} signals")
    print()

    # Generate visualizations
    figures = []

    print("Generating visualizations...")

    # 1. Summary Dashboard
    print("  1. Summary Dashboard")
    fig_summary = plot_summary_dashboard(data)
    figures.append(('summary_dashboard', fig_summary))

    # 2. Price Movements
    if not data['price_movements'].empty:
        print("  2. Price Movements & Volatility")
        fig_price = plot_price_movements(data['price_movements'])
        if fig_price:
            figures.append(('price_movements', fig_price))

    # 3. Volume Spikes
    if not data['volume_spikes'].empty:
        print("  3. Volume Spikes")
        fig_vol = plot_volume_spikes(data['volume_spikes'])
        if fig_vol:
            figures.append(('volume_spikes', fig_vol))

    # 4. Sentiment
    if not data['market_sentiment'].empty:
        print("  4. Market Sentiment")
        fig_sent = plot_sentiment(data['market_sentiment'])
        if fig_sent:
            figures.append(('market_sentiment', fig_sent))

    # 5. Sentiment Correlation
    if not data['sentiment_correlation'].empty:
        print("  5. Sentiment-Price Correlation")
        fig_corr = plot_sentiment_correlation(data['sentiment_correlation'])
        if fig_corr:
            figures.append(('sentiment_correlation', fig_corr))

    # Save figures
    print(f"\nSaving {len(figures)} visualizations...")
    for name, fig in figures:
        filename = f"insights_{name}.png"
        fig.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"  ✓ Saved: {filename}")

    print("\n" + "="*60)
    print("✅ Visualization complete! Opening dashboard...")
    print("="*60)

    # Show all plots
    plt.show()

if __name__ == "__main__":
    main()
