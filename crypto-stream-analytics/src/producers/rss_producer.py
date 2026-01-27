"""
News producer for crypto news and sentiment.

Monitors multiple crypto news sources (NewsAPI + RSS feeds) and streams to Kafka.
"""
import os
import feedparser
import json
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer
import socket
import hashlib
import requests
from dotenv import load_dotenv

load_dotenv()


class RSSFeedProducer:
    """
    Streams crypto news from NewsAPI + RSS feeds to Kafka.

    Monitors NewsAPI and multiple RSS feeds, publishes articles to Kafka
    for sentiment analysis and correlation with price movements.
    """

    def __init__(self, kafka_topic='crypto-news', newsapi_key=None):
        """
        Initialize news producer with NewsAPI + RSS feeds.

        Args:
            kafka_topic (str): Kafka topic for news data
            newsapi_key (str): NewsAPI key (uses env var if not provided)
        """
        self.topic = kafka_topic
        self.newsapi_key = newsapi_key or os.getenv('NEWSAPI_KEY')
        self.message_count = 0
        self.start_time = time.time()
        self.seen_ids = set()

        self.feeds = {
            'CoinDesk': 'https://www.coindesk.com/arc/outboundfeeds/rss/',
            'Cointelegraph': 'https://cointelegraph.com/rss',
            'Decrypt': 'https://decrypt.co/feed',
            'Bitcoin Magazine': 'https://bitcoinmagazine.com/.rss/full/',
            'CryptoSlate': 'https://cryptoslate.com/feed/',
        }

        self.newsapi_queries = [
            'bitcoin OR btc',
            'ethereum OR eth',
            'cryptocurrency OR crypto'
        ]

        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        conf = {
            'bootstrap.servers': kafka_servers,
            'client.id': socket.gethostname(),
            'linger.ms': 10,
            'compression.type': 'snappy',
        }

        self.kafka_producer = Producer(conf)
        print(f"[OK] Kafka producer initialized")
        print(f"[OK] Monitoring {len(self.feeds)} RSS feeds")
        print(f"[OK] Using NewsAPI with {len(self.newsapi_queries)} crypto queries\n")

    def delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            print(f'[ERROR] Delivery failed: {err}')
        else:
            if self.message_count % 10 == 0:
                print(f'[OK] {self.message_count} articles sent to Kafka')

    def generate_article_id(self, entry):
        """Generate unique ID for article using MD5 hash of link."""
        link = entry.get('link', '')
        return hashlib.md5(link.encode()).hexdigest()

    def process_entry(self, entry, source):
        """Extract relevant data from RSS entry."""
        pub_time = entry.get('published_parsed') or entry.get('updated_parsed')
        if pub_time:
            created_utc = time.mktime(pub_time)
            human_time = datetime.fromtimestamp(created_utc).isoformat()
        else:
            created_utc = time.time()
            human_time = datetime.now().isoformat()

        return {
            'type': 'news_article',
            'id': self.generate_article_id(entry),
            'source': source,
            'title': entry.get('title', ''),
            'summary': entry.get('summary', ''),
            'link': entry.get('link', ''),
            'author': entry.get('author', 'Unknown'),
            'created_utc': created_utc,
            'timestamp': int(time.time() * 1000),
            'human_time': human_time,
            'tags': [tag.term for tag in entry.get('tags', [])]
        }

    def send_to_kafka(self, data):
        """Send processed article to Kafka."""
        try:
            self.kafka_producer.produce(
                topic=self.topic,
                key=data['source'],
                value=json.dumps(data),
                callback=self.delivery_report
            )
            self.kafka_producer.poll(0)
            self.message_count += 1

        except Exception as e:
            print(f"Error sending to Kafka: {e}")

    def fetch_newsapi_articles(self, query):
        """Fetch articles from NewsAPI."""
        try:
            from_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            url = (
                f'https://newsapi.org/v2/everything?'
                f'q={query}&'
                f'from={from_date}&'
                f'sortBy=publishedAt&'
                f'language=en&'
                f'apiKey={self.newsapi_key}'
            )

            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data['status'] == 'ok':
                return data.get('articles', [])
            else:
                print(f"NewsAPI error: {data.get('message', 'Unknown error')}")
                return []

        except Exception as e:
            print(f"Error fetching NewsAPI ({query}): {e}")
            return []

    def process_newsapi_article(self, article):
        """Process NewsAPI article into standard format."""
        pub_time = article.get('publishedAt', '')
        if pub_time:
            try:
                dt = datetime.strptime(pub_time, '%Y-%m-%dT%H:%M:%SZ')
                created_utc = dt.timestamp()
                human_time = dt.isoformat()
            except:
                created_utc = time.time()
                human_time = datetime.now().isoformat()
        else:
            created_utc = time.time()
            human_time = datetime.now().isoformat()

        article_id = hashlib.md5(article.get('url', '').encode()).hexdigest()

        return {
            'type': 'news_article',
            'id': article_id,
            'source': article.get('source', {}).get('name', 'NewsAPI'),
            'title': article.get('title', ''),
            'summary': article.get('description', ''),
            'link': article.get('url', ''),
            'author': article.get('author', 'Unknown'),
            'created_utc': created_utc,
            'timestamp': int(time.time() * 1000),
            'human_time': human_time,
            'tags': []
        }

    def fetch_and_process_newsapi(self):
        """Fetch and process articles from NewsAPI."""
        new_articles = 0

        for query in self.newsapi_queries:
            articles = self.fetch_newsapi_articles(query)

            for article in articles:
                data = self.process_newsapi_article(article)
                article_id = data['id']

                if article_id in self.seen_ids:
                    continue

                self.send_to_kafka(data)
                self.seen_ids.add(article_id)
                new_articles += 1

                try:
                    print(f"[NewsAPI] {data['title'][:70]}...")
                except UnicodeEncodeError:
                    print(f"[NewsAPI] {data['title'][:70].encode('ascii', 'ignore').decode()}...")

        if new_articles > 0:
            print(f"  -> {new_articles} new articles from NewsAPI\n")

    def fetch_and_process_feed(self, source, url):
        """Fetch and process RSS feed."""
        try:
            feed = feedparser.parse(url)

            new_articles = 0
            for entry in feed.entries:
                article_id = self.generate_article_id(entry)

                if article_id in self.seen_ids:
                    continue

                data = self.process_entry(entry, source)
                self.send_to_kafka(data)
                self.seen_ids.add(article_id)
                new_articles += 1

                try:
                    print(f"[{source}] {data['title'][:70]}...")
                except UnicodeEncodeError:
                    print(f"[{source}] {data['title'][:70].encode('ascii', 'ignore').decode()}...")

            if new_articles > 0:
                print(f"  -> {new_articles} new articles from {source}\n")

        except Exception as e:
            print(f"Error fetching {source}: {e}")

    def start(self):
        """Start streaming news from NewsAPI + RSS feeds to Kafka."""
        print(f"Starting News (NewsAPI + RSS) -> Kafka pipeline")
        print(f"NewsAPI queries: {', '.join(self.newsapi_queries)}")
        print(f"RSS Sources: {', '.join(self.feeds.keys())}")
        print(f"Kafka topic: {self.topic}")
        print(f"Poll interval: 60 seconds\n")

        try:
            while True:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Polling news sources...")

                print("\n--- Fetching from NewsAPI ---")
                self.fetch_and_process_newsapi()

                print("--- Fetching from RSS Feeds ---")
                for source, url in self.feeds.items():
                    self.fetch_and_process_feed(source, url)

                elapsed = time.time() - self.start_time
                rate = self.message_count / elapsed if elapsed > 0 else 0
                print(f"\n--- Stats: {self.message_count} total articles, "
                      f"{rate:.2f} articles/min ---\n")

                print(f"Waiting 60 seconds before next poll...\n")
                time.sleep(60)

        except KeyboardInterrupt:
            print(f"\n\n[OK] Stopping. Total: {self.message_count} articles streamed.")
            self.kafka_producer.flush()


if __name__ == "__main__":
    producer = RSSFeedProducer()
    producer.start()
