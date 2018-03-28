import feedparser, urllib2

feed = feedparser.parse('http://news.google.com/news?ned=us&topic=apple&output=rss')

for entry in feed['entries']:
	print entry 
