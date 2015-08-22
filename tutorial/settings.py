# -*- coding: utf-8 -*-

# Scrapy settings for tutorial project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'tutorial'

SPIDER_MODULES = ['tutorial.spiders']
NEWSPIDER_MODULE = 'tutorial.spiders'

RETRY_ENABLED = False
COOKIES_ENABLED = False
DEPTH_LIMIT = 1
WEBSERVICE_ENABLED=False
TELNETCONSOLE_ENABLED=False
REDIRECT_ENABLED = False
HTTPERROR_ALLOW_ALL=True
DOWNLOAD_TIMEOUT = 15
CONCURRENT_REQUESTS = 100
REACTOR_THREADPOOL_MAXSIZE = 20
AJAXCRAWL_ENABLED = True
# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'tutorial (+http://www.yourdomain.com)'
