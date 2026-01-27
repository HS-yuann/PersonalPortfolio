import sqlite3
from datetime import datetime

conn = sqlite3.connect('enhanced_crypto_insights.db')
cursor = conn.cursor()

# Get total records
cursor.execute('SELECT COUNT(*) FROM price_movements')
total = cursor.fetchone()[0]

print("=" * 70)
print(f"CRYPTO STREAM ANALYTICS - LIVE RESULTS")
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)
print(f"\nTOTAL PRICE MOVEMENT RECORDS: {total:,}")
print("\n" + "=" * 70)
print("LATEST 20 PRICE MOVEMENTS")
print("=" * 70)

# Get latest movements
cursor.execute('''
    SELECT symbol, price_change_pct, avg_price, window_end
    FROM price_movements
    ORDER BY id DESC
    LIMIT 20
''')

for row in cursor.fetchall():
    symbol, change, price, window_end = row
    direction = "UP  " if change > 0 else "DOWN"
    arrow = "^" if change > 0 else "v"
    print(f"{arrow} {direction} | {symbol:8} | {change:+7.3f}% | Avg: ${price:10,.2f} | {window_end}")

# Get summary statistics
print("\n" + "=" * 70)
print("SUMMARY STATISTICS (Last 100 Records)")
print("=" * 70)

for symbol in ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']:
    cursor.execute('''
        SELECT
            AVG(price_change_pct) as avg_change,
            MIN(avg_price) as min_price,
            MAX(avg_price) as max_price,
            AVG(avg_price) as avg_price_overall
        FROM (
            SELECT * FROM price_movements
            WHERE symbol = ?
            ORDER BY id DESC
            LIMIT 100
        )
    ''', (symbol,))

    result = cursor.fetchone()
    if result[0]:
        avg_change, min_price, max_price, avg_price = result
        print(f"\n{symbol}:")
        print(f"  Avg Change: {avg_change:+.3f}%")
        print(f"  Price Range: ${min_price:,.2f} - ${max_price:,.2f}")
        print(f"  Avg Price: ${avg_price:,.2f}")

conn.close()
print("\n" + "=" * 70)
