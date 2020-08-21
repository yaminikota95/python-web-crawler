# python-web-crawler
### cfg.py contains the configurable parameters: root, sleep_time, time_gap and max_lim.

root: The Root URL must be manually entered into the database before the process starts.
sleep_time: Between each cycle, the process sleeps for 'sleep_time' number of seconds.
time_gap: The process should not scrape links that are scraped already in the last 'time_gap' period.
max_lim: Once the database has got 'max_lim' number of links, the cycle is considered complete.
