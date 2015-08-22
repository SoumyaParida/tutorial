#--- Our Script
 
# get parent folder path
import os,sys,inspect
import re
import csv
from urlparse import urlparse
from scrapy.http import Request, HtmlResponse
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.contrib.linkextractors import LinkExtractor as sle
from tutorial.items import Page
# currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
# parentdir = os.path.dirname(currentdir)
# sys.path.insert(0,parentdir)
 
#import config
#import start_scrapy

#--- Third-Party Libraries
 
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import csv
import dns.resolver
import dns.query
import dns.flags
import codecs
import sys
from multiprocessing import Process, Lock
from multiprocessing import Process, Value, Lock
import time
#from multiprocessing.sharedctypes import Value
from datetime import datetime
#from pybloomfilter import BloomFilter
import pygeoip
#import fcntl
import line_profiler
import Queue
import cProfile,pstats, StringIO
import collections

 
#--- Python Libraries 
 
import re
class Counter(object):
    def __init__(self, initval=0):
        self.val = Value('i', initval)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1
            time.sleep(0.0001)
            #print "self.val.value",self.val.value
    def value(self):
        with self.lock:
            return self.val.value

class alexaSpider(Spider):
    name = 'alexa'
    listOfLists=[]
    urllist=[]
    global counter
    global resultFile
    global tagType
    global logFile
    global dest_server_ip
    global _extract_object_count
    global getsecondleveldomain
    global logwr
    global dest_ASN
    global lock
    global start_urls
    global cookies_seen
    global allowed_domains
    global urlIndexlist
    #global spider_queue
    dest_ASN=[]
    counter=Counter(0)
    tagType='A'
    dest_server_ip=[]
    urlIndexlist=dict()
    #spider_queue=Queue.Queue()
    #lock = Lock()
    start_urls = []
    allowed_domains=[]
    #cookies_seen = set()
    logFile = codecs.open("log.csv",'wbr+')
    fieldnames = ['url', 'counter','ExternalImageCount','InternalImageCount','ExternalscriptCount','InternalscriptCount','ExternallinkCount','InternallinkCount','ExternalembededCount','InternalembededCount','UniqueExternalSites','ExternalSites','secondlevelurl']
    logwr = csv.DictWriter(logFile, fieldnames=fieldnames)
    resultFile = codecs.open("output6.csv",'wbr+')

    with open('top-1m.csv') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|') 
        for row in spamreader:
            rowValue=', '.join(row)
            rowValues=rowValue.split(",")
            if not rowValues[1].startswith('http://') and not rowValues[1].startswith('https://'):
                rowValues[1] = 'http://%s' % rowValues[1]
            urlIndexlist[rowValues[1]]=rowValues[0]
            #listOfLists.append(rowValues[1])
    # start_urls=listOfLists
    # for url in listOfLists:
    #     if not url.startswith('http://') and not url.startswith('https://'):
    #         url = 'http://%s' % url
    #         urllist.append(url)
    # allowed_domains=urllist
    # # allowed_domains = list format (like domain1.com, domain2.com)
    # allowed_domains = config.domains_list 
    # # start_urls - crawling single or multiple websites , list formate
    # start_urls = config.urls_list

    def __init__(self, **kw ):
        super(alexaSpider, self).__init__(**kw )
        urllistfile = kw.get('part')
        print urllistfile
        
 
    def start_requests(self):
        self.link_extractor = sle()
        self.cookies_seen = set()
        #yield Request(self.url,callback=self.parse,meta={'counter': counter.value()},dont_filter=True)
        #print start_urls
        for url in urlIndexlist:
            #self.counter=counter.increment()
            indexUnique=urlIndexlist.get(url)
            yield Request(url, meta={'counter': indexUnique},method='GET',callback=self.parse,dont_filter=True)
            # if tagType is 'I':
            #     yield Request(url, meta={'counter': indexUnique},method='HEAD',callback=self.parse,dont_filter=True)     
            # else:
            #     yield Request(url, meta={'counter': indexUnique},method='GET',callback=self.parse,dont_filter=True)
                

    def parse(self,response):
        # scrapy Init function 
        # response = get HTML content 
        
        # split only domain name from crawl url.
 
        #domain_str = response.url.split(config.url_domain_split_start)[config.url_domain_split_start_num].split(config.url_domain_split_end)[config.url_domain_split_end_num]
        print response.url
        urlList=[]
        r=[]
        global resultFile
        global spider_queue
        # pr = cProfile.Profile()
        # pr.enable()
        page = self._get_item(response)
        # lock = Lock()
        # lock.acquire()
        r = [page]
        depth = page['depth_level']
        depth_value=depth.get('depth')
        if depth_value:
            page['depth_level'] = depth_value
        else:
            page['depth_level']='0'

        #counter=response.meta.get('counter')
        
        counter=response.meta.get('counter')
       
        # spider_queue=response.meta.get('spider_queue')
        #print "spider_queue",spider_queue
        
        if counter:
            page['index']=counter
        else :
            page['index']='1'
        tagType=response.meta.get('tagType')  
        if tagType:
            page['tagType']=tagType      
        else:
            page['tagType']='A'

        

        
        # try:
        #     r.extend(self._extract_requests(response,counter)) #external site link
        # except:
        #     pass
        # try:
        #     r.extend(self._extract_img_requests(response,tagType,counter)) #link to img files
        # except:
        #     pass
        # try:
        #     r.extend(self._extract_script_requests(response,tagType,counter)) #link to script files like java script etc
        # except:
        #     pass
        # try:
        #     r.extend(self._extract_external_link_requests(response,tagType,counter)) #link to css or any other external linked files
        # except:
        #     pass
        # try:
        #     r.extend(self._extract_embed_requests(response,tagType,counter)) #link to addresses of the external file to embed
        # except:
        #     pass
        
        urlList.append(page['index'])
        urlList.append(page['depth_level'])
        urlList.append(page['httpResponseStatus'])
        urlList.append(page['content_length'])
        urlList.append(page['url'].strip())
        setCookies=';'.join(page['newcookies'])
        if setCookies:
            page['newcookies']=setCookies
        else:
            page['newcookies']='-'
        urlList.append(page['newcookies'])
        urlList.append(page['tagType'])
        setCnameChain=';'.join(page['CNAMEChain'])
        if setCnameChain:
            page['CNAMEChain']=setCnameChain
        else:
            page['CNAMEChain']='-'
        urlList.append(page['CNAMEChain'])
        dest_server_ip_values=';'.join(dest_server_ip)
        if dest_server_ip_values:
            page['destIP']=(dest_server_ip_values)
        else:
            page['destIP']='-'
        urlList.append(page['destIP'])
        asn_no=';'.join(dest_ASN)
        if asn_no:
            page['ASN_Number']=(asn_no)
        else :
            page['ASN_Number']='-'
        urlList.append(page['ASN_Number'])
        urlList.append(page['start_time'])
        page['end_time']=datetime.now().time()
        urlList.append(page['end_time'])

        wr = csv.writer(resultFile, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
        newUrlList=[]
        for item in urlList:
            if isinstance(item, unicode):
                item=item.encode('utf-8')
                newUrlList.append(item)
            elif isinstance(item,str):
                item=item
                newUrlList.append(item)
            else:
                item=item
                newUrlList.append(item)
        
        #spider_queue.put(r)
        wr.writerow(newUrlList)
        # print "spider_queue.get",results
        
        #pr.disable()
        #pr.dump_stats("out.txt")
        #s = StringIO.StringIO()
        #sortby = 'cumulative'
        #ps = pstats.Stats(pr,"out.txt").sort_stats(sortby)
        #ps.print_stats()
        #p = pstats.Stats(pr,'out.txt')
        #p.sort_stats('cumulative')
        #ps.dump_stats("out.txt")
        #print s.getvalue()
        #resultFile.close()
        r.extend(self._extract_requests(response,counter)) #external site link
        r.extend(self._extract_img_requests(response,tagType,counter)) #link to img files
        r.extend(self._extract_script_requests(response,tagType,counter)) #link to script files like java script etc
        r.extend(self._extract_external_link_requests(response,tagType,counter)) #link to css or any other external linked files
        r.extend(self._extract_embed_requests(response,tagType,counter)) #link to addresses of the external file to embed
        #lock.release()
        return r


    # def getKey(page):
    #     return page['index']

    """[Author:Som ,last modified:16th April 2015]
    def _get_item:used to crawl items.
    Future requiremnets (items) will be passed here.

    @returns item
    @scrapes title which will stored in csv file
    """
    #@profile
    def _get_item(self, response):
        item = Page(url=response.url,content_length=str(len(response.body)),depth_level=response.meta,
            httpResponseStatus=response.status,start_time= datetime.now().time())
        self._set_new_cookies(item,response)
        self._set_DNS_info(item,response)
        return item

    """[Author:Som ,last modified:16th April 2015]
    def _extract_requests:used to crawl urls.

    @returns urls
    """
    #@profile
    def _extract_requests(self,response,counter):
        r = []
        counterValue=counter
        if isinstance(response, HtmlResponse):
            links = self.link_extractor.extract_links(response)
            r.extend(Request(x.url, callback=self.parse,meta={'counter': counterValue})for x in links if x.url != response.url)
        return r
    #@profile
    def _extract_object_count(siteList):
        InternalSitesCount=0
        externalSitesCount=0
        uniqueExternalSites=set()
        externalSites=[]
        for site in siteList: 
            if site.startswith("http://") or site.startswith("https://"):
                externalSitesCount+=1
                uniqueExternalSites.add(getsecondleveldomain(site))
                externalSites.append(site)
            else:
                InternalSitesCount+=1
        return (externalSitesCount,InternalSitesCount,len(uniqueExternalSites),externalSites,uniqueExternalSites)
    #@profile
    def _extract_img_requests(self,response,tag,counter):
        r = []
        siteList=[]
        ObjectList=dict()
        externalSites=[]
        #uniqueExternalSites=[]
        if isinstance(response, HtmlResponse):
            tag='I'
            #imgcount=0
            counterValueImg=counter
            sites = Selector(response).xpath("//img/@src").extract()
            # for site in sites:
            #     imgcount=imgcount+1
            #logging.info('imgcount',imgcount)

            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            #wr.writerow(siteList)
            
            externalImageCount,InternalImageCount,uniqueExternalSites,externalSites,secondlevelurl =_extract_object_count(siteList)
            Imagecount=len(siteList)
            #lock.acquire()
            
            # ObjectList['url']=response.url
            # ObjectList['counter']=counterValueImg
            # ObjectList['Imagecount']=Imagecount
            # ObjectList['InternalImageCount']=InternalImageCount
            # ObjectList['ExternalImageCount']=externalImageCount

            # logwr.writeheader()
            logwr.writerow({'url': response.url, 'counter': counterValueImg,'InternalImageCount':InternalImageCount,'ExternalImageCount':externalImageCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites,'secondlevelurl':secondlevelurl})
            #logwr.writerow([ObjectList])
            #lock.release()
            #wr.writerow([Imagecount])
            #logwr.writerow([imgcount])
            #Imagecount=str(len(siteList))
            #logwr.writerow([siteList])
            r.extend(Request(site, callback=self.parse,method='HEAD',meta={'tagType': tag,'counter': counterValueImg})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r
    #@profile
    def _extract_script_requests(self,response,tag,counter):
        r=[]
        siteList=[]
        ObjectList=dict()
        externalSites=[]
        #uniqueExternalSites=[]
        if isinstance(response, HtmlResponse):
            tag='S'
            #scriptcount=0
            counterValueScript=counter
            sites = Selector(response).xpath("//script/@src").extract()
            # for site in sites:
            #     scriptcount=scriptcount+1
            #logging.info('scriptcount',scriptcount)

            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)

            externalscriptCount,InternalscriptCount,uniqueExternalSites,externalSites,secondlevelurl=_extract_object_count(siteList)
            scriptcount=len(siteList)
            #lock.acquire()
            # ObjectList['url']=response.url
            # ObjectList['counter']=counterValueScript
            # ObjectList['scriptcount']=scriptcount
            # ObjectList['InternalscriptCount']=InternalscriptCount
            # ObjectList['ExternalscriptCount']=externalscriptCount
            # logwr.writerow([ObjectList])

            # logwr.writeheader()
            logwr.writerow({'url': response.url, 'counter': counterValueScript,'InternalscriptCount':InternalscriptCount,'ExternalscriptCount':externalscriptCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites,'secondlevelurl':secondlevelurl})
            #lock.release()
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueScript})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r
    #@profile
    def _extract_external_link_requests(self,response,tag,counter):
        r=[]
        siteList=[]
        ObjectList=dict()
        externalSites=[]
        #uniqueExternalSites=[]
        if isinstance(response, HtmlResponse):
            tag='L'
            #linkcount=0
            counterValueLink=counter
            sites = Selector(response).xpath("//link/@href").extract()
            # for site in sites:
            #     linkcount=linkcount+1
            #logging.info('linkcount',linkcount)

            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            sites.append(counterValueLink)
            externallinkCount,InternallinkCount,uniqueExternalSites,externalSites,secondlevelurl=_extract_object_count(siteList)
            #wr.writerow(siteList)
            linkcount=len(siteList)
            #lock.acquire()
            # ObjectList['url']=response.url
            # ObjectList['counter']=counterValueLink
            # ObjectList['linkcount']=linkcount
            # ObjectList['InternallinkCount']=InternallinkCount
            # ObjectList['ExternallinkCount']=externallinkCount
            # #lock.acquire()
            # logwr.writerow([ObjectList])
            logwr.writerow({'url': response.url, 'counter': counterValueLink,'InternallinkCount':InternallinkCount,'ExternallinkCount':externallinkCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites,'secondlevelurl':secondlevelurl})
            #lock.acquire()
            #lock.release()
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueLink})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r
    #@profile
    def _extract_embed_requests(self,response,tag,counter):
        r=[]
        siteList=[]
        ObjectList=dict()
        externalSites=[]
        #uniqueExternalSites=set()
        if isinstance(response, HtmlResponse):
            tag='E'
            #embededcount=0
            counterValueEmded=counter
            sites = Selector(response).xpath("//embed/@src").extract()
            # for site in sites:
            #     embededcount=embededcount+1
            #logging.info('embededcount',embededcount)

            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)

            externalembededCount,InternalembededCount,uniqueExternalSites,externalSites,secondlevelurl=_extract_object_count(siteList)
            embededcount=len(siteList)
            #lock.acquire()
            # ObjectList['url']=response.url
            # ObjectList['counter']=counterValueEmded
            # ObjectList['embededcount']=embededcount
            # ObjectList['InternalembededCount']=InternalembededCount
            # ObjectList['ExternalembededCount']=externalembededCount
            # #lock.acquire()
            # logwr.writerow([ObjectList])
            logwr.writerow({'url': response.url, 'counter': counterValueEmded,'InternalembededCount':InternalembededCount,'ExternalembededCount':externalembededCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites,'secondlevelurl':secondlevelurl})
            #lock.acquire()
            #lock.release()
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueEmded})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r
    #@profile
    def _set_title(self, page, response):
        if isinstance(response, HtmlResponse):
            title = Selector(response).xpath("//title/text()").extract()
            if title:
                page['title'] = title[0]

    """[Author:Som ,last modified:16th April 2015]
    def _set_http_header_info:used to crawl response status of
    any website like 200 : ok ,404 :not found etc.

    @returns responseStatus
    """
    # def _set_http_header_info(self, page, response):
    #     if isinstance(response, HtmlResponse):
    #         responseStatus = response.status
    #         #print "responseStatus",responseStatus
    #         if responseStatus:
    #             page['httpResponseStatus']=responseStatus
    #         else :
    #             page['httpResponseStatus']="-"
    """[Author:Som ,last modified:16th April 2015]
    def _set_new_cookies:used to crawl cookies of
    any website.
    """
    #@profile
    def _set_new_cookies(self, page, response):
        cookies = []
        for cookie in [x.split(';', 1)[0] for x in response.headers.getlist('Set-Cookie')]:
            if cookie not in self.cookies_seen:
                self.cookies_seen.add(cookie)
                cookies.append(cookie.replace(" [",""))
        # cookie=response.headers.getlist('Set-Cookie')
        # for cookieValue in cookie:
        #     cookies.append(cookieValue.strip())
        #     cookies.append('|')
        if cookies:
            page['newcookies'] = cookies
        else:
            page['newcookies'] = "-"

    """[Author:Som ,last modified:22th April 2015]
        def _set_DNS_info:used to retrieve CNAME chain.
    """
    #@profile
    def _set_DNS_info(self, page,response):
        # start_time = time.clock()
        CNAMEList=[]
        # dest_server_ip=[]
        # dest_ASN=[]
        dest_server_ip[:] = []
        dest_ASN[:]=[]
        domain=response.url
        global dns_lookup_time
        #urlparse :This module defines a standard interface to break URL strings up 
        #in components (addressing scheme, network location, path etc.), to combine
        #the components back into a URL string, and to convert a relative URL to 
        #an absolute URL given a base URL.
        domain=urlparse(domain).netloc
        if domain.startswith('http://'):
            domain=domain.replace("http://","",1)
        elif domain.startswith('https://'):
            domain=domain.replace("https://","",1)

        if domain.endswith('/'):
            domain=domain.replace("/","",1)

        if not domain.startswith('www.'):
            domain = 'www.%s' % domain
        try:
            answers = dns.resolver.query(domain, 'CNAME')
            destServerIPs = dns.resolver.query(domain, 'A')
            #page['destIP']='1'
            for rdata in answers:
                try:
                    CNAMEList.append(str(rdata))
                    while (rdata.target):
                        value=dns.resolver.query(rdata.target, 'CNAME')
                        for rdata in value:
                            CNAMEList.append(str(rdata))
                except dns.resolver.NXDOMAIN:
                    continue
                except dns.resolver.Timeout:
                    continue
                except dns.exception.DNSException:
                    continue
                except dns.resolver.NoAnswer:
                    continue

            for ip_address in destServerIPs:
                try:
                    if not str(ip_address) in dest_server_ip:
                         dest_server_ip.append(str(ip_address))
                    #asn_info=IPWhois(str(IPs))
                    gir = pygeoip.GeoIP('GeoIPASNum.dat',
                       flags=pygeoip.const.GEOIP_STANDARD)
                    #gi.asn_by_name(IPs)
                    # if str(ip_address):
                    #     dest_ASN.append(str(ip_address))
                    # else:
                    #     dest_ASN.append('-')
                    asNum=gir.asn_by_name(str(ip_address))
                    if asNum:
                        asNumSplit=asNum.split(' ')
                        asn=''.join(x for x in asNumSplit[0] if x.isdigit())
                        if not asn in dest_ASN:
                            dest_ASN.append(asn)
                    else:
                        dest_ASN.append('-')

                    
                    # if isinstance(asn_info, unicode):
                    #     asn_info=asn_info.encode('utf-8')
                    
                    # try:
                    #     results = asn_info.lookup()
                    #     if not results['asn'] in dest_ASN: 
                    #         dest_ASN.append(results['asn'])
                    # except:
                    #     dest_ASN.append('-')
                    #mydict.keys()[mydict.values().index(16)]
                    
                    #dest_ASN.append(results)
                except dns.resolver.NXDOMAIN:
                    continue
                except dns.resolver.Timeout:
                    continue
                except dns.exception.DNSException:
                    continue
                except dns.resolver.NoAnswer:
                    continue

            if dest_ASN:
                page['ASN_Number']=dest_ASN
            else:
                page['ASN_Number']='-'

            if CNAMEList:
                page['CNAMEChain']=CNAMEList
            else:
                page['CNAMEChain']="-"

            if dest_server_ip:
                page['destIP']=dest_server_ip
            else:
                page['destIP']='-'
        except dns.resolver.NXDOMAIN:
            # CNAME.append('NONE')
            # page['CNAMEChain']=CNAME 
            page['CNAMEChain']="-"   
            #page['destIP']='-'    
        except dns.resolver.Timeout:
            page['CNAMEChain']="-"
            #page['destIP']='-'
        except dns.exception.DNSException:
            page['CNAMEChain']="-"
            #page['destIP']='-'
        except dns.resolver.NoAnswer:
            page['CNAMEChain']="-"
            #page['destIP']='-'
        # dns_lookup_time=dns_lookup_time+(time.clock() -start_time)
        # print dns_lookup_time, "dns"
    #@profile
    def getsecondleveldomain(url):
        with open("effective_tld_names.dat") as tld_file:
            tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]
        url_elements = urlparse(url)[1].split('.')
        for i in range(-len(url_elements), 0):
            last_i_elements = url_elements[i:]
            candidate = ".".join(last_i_elements) # abcde.co.uk, co.uk, uk
            wildcard_candidate = ".".join(["*"] + last_i_elements[1:]) # *.co.uk, *.uk, *
            exception_candidate = "!" + candidate

            # match tlds: 
            if (exception_candidate in tlds):
                return ".".join(url_elements[i:]) 
            if (candidate in tlds or wildcard_candidate in tlds):
                return ".".join(url_elements[i-1:])