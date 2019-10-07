# python_test-for-client

Create a web crawler/scraper that uses socket connections (No Selenium) to gather links from webpages and add them to a process queue. The queue will be processed by P number of processes (However many cores on the machine). Each process will use aiohttp (Async) with max T number of threads/tasks (Variable default: 100) to scrape from the queue and add to the queue. Store the title of all scraped HTML pages along with their URLs in an SQLITE database file.
