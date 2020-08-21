# -*- coding: utf-8 -*-
"""
Created on Thu Aug 20 05:05:46 2020

@author: Yamini
"""

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from time import sleep
from cfg import *
import threading


def link_alt(a):
    '''
    
    getting alternate links to avoid redundant entries in database
    
    Parameters
    ----------
    a : string
        urljoined href link address from the <a> tags.

    Returns
    -------
    a_href : string
        link address after removing the fragments etc.
    alt_hrefs : set
        set of all the combinations possible for the given a_href to avoid redundancy.

    '''
    a_href = requests.compat.urlparse(a).scheme + "://" + requests.compat.urlparse(a).netloc + requests.compat.urlparse(a).path
    alt_hrefs = set()   
    # check if link is valid
    if (bool(requests.compat.urlparse(a_href).netloc) and bool(requests.compat.urlparse(a_href).scheme) and requests.compat.urlparse(a_href).scheme !="javascript"):
        
        if requests.compat.urlparse(a).scheme[:4] == 'http':
            if len(requests.compat.urlparse(a).netloc.split('.')) == 2 or requests.compat.urlparse(a).netloc.split('.')[0]=='www':
                alt_hrefs.update([a_href,
                          requests.compat.urlparse(a).scheme.rstrip('s') + "://" + requests.compat.urlparse(a).netloc.strip('www.') + requests.compat.urlparse(a).path.rstrip('/'),
                          requests.compat.urlparse(a).scheme.rstrip('s') + "://" + requests.compat.urlparse(a).netloc.strip('www.') + requests.compat.urlparse(a).path.rstrip('/') + '/',
                          requests.compat.urlparse(a).scheme.rstrip('s') + "://" + "www." + requests.compat.urlparse(a).netloc.strip('www.') + requests.compat.urlparse(a).path.rstrip('/'),
                          requests.compat.urlparse(a).scheme.rstrip('s') + "://" + "www." + requests.compat.urlparse(a).netloc.strip('www.') + requests.compat.urlparse(a).path.rstrip('/') + '/',
                          requests.compat.urlparse(a).scheme.rstrip('s') + 's' + "://" + requests.compat.urlparse(a).netloc.strip('www.') + requests.compat.urlparse(a).path.rstrip('/'),
                          requests.compat.urlparse(a).scheme.rstrip('s') + 's' + "://" + requests.compat.urlparse(a).netloc.strip('www.') + requests.compat.urlparse(a).path.rstrip('/') + '/',
                          requests.compat.urlparse(a).scheme.rstrip('s') + 's' + "://" + "www." + requests.compat.urlparse(a).netloc.strip('www.') + requests.compat.urlparse(a).path.rstrip('/'),
                          requests.compat.urlparse(a).scheme.rstrip('s') + 's' + "://" + "www." + requests.compat.urlparse(a).netloc.strip('www.') + requests.compat.urlparse(a).path.rstrip('/') + '/'
                          ])
            else:
                alt_hrefs.update([a_href,
                          requests.compat.urlparse(a).scheme.rstrip('s') + "://" + requests.compat.urlparse(a).netloc + requests.compat.urlparse(a).path.rstrip('/'),
                          requests.compat.urlparse(a).scheme.rstrip('s') + "://" + requests.compat.urlparse(a).netloc + requests.compat.urlparse(a).path.rstrip('/') + '/',
                          requests.compat.urlparse(a).scheme.rstrip('s') + 's' + "://" + requests.compat.urlparse(a).netloc + requests.compat.urlparse(a).path.rstrip('/'),
                          requests.compat.urlparse(a).scheme.rstrip('s') + 's' + "://" + requests.compat.urlparse(a).netloc + requests.compat.urlparse(a).path.rstrip('/') + '/'
                          ])
        else:
            alt_hrefs.update([a_href,
                          requests.compat.urlparse(a).scheme + "://" + requests.compat.urlparse(a).netloc + requests.compat.urlparse(a).path.rstrip('/'),
                          requests.compat.urlparse(a).scheme + "://" + requests.compat.urlparse(a).netloc + requests.compat.urlparse(a).path.rstrip('/') + '/'
                          ])
        
    return a_href,alt_hrefs

def crawl(soup,links,urlink):
    '''
    find all the valid links from <a> tags and inserts them into the database
    Parameters
    ----------
    soup : Beautiful Soup object
        soup object of 'urlink'
    links : MongoDB Collection
        Collection (database) of all the links.
    urlink : string
        source link.i.e., link address of the page from which the <a> tag links are to be obtained.

    Returns
    -------
    None.

    '''
    for a_tag in soup.find_all('a'):
        # if the max limit ('max_lim' from cfg.py) of the database is reached, cycle is completed
        if links.count_documents({}) > max_lim - 1:
            print("Maximum limit reached")
            print(links.count_documents({"Is Crawled":False}))
            # sleep for 'sleep_time' seconds. 'sleep_time' from cfg.py
            sleep(sleep_time)
            return
        a = a_tag.get('href')
        a = requests.compat.urljoin(urlink, a)
        insert_new(a,links,urlink)
        # sleep for 'sleep_time' seconds. 'sleep_time' from cfg.py
        sleep(sleep_time)
    return

def insert_new(a,links,urlink = None):
    '''
    inserts a new entry (document) into the MongoDB collection(database) after checking redundancies

    Parameters
    ----------
    a : string
        urljoined href link address from the <a> tags.
    links : MongoDB Collection
        Collection (database) of all the links.
    urlink : string, optional
        source link.i.e., link address of the page from which the <a> tag links are to be obtained. The default is None.

    Returns
    -------
    None.

    '''
    a_href,alt_hrefs = link_alt(a)
    if (bool(requests.compat.urlparse(a_href).netloc) and bool(requests.compat.urlparse(a_href).scheme) and requests.compat.urlparse(a_href).scheme !="javascript"):
        
        if links.count_documents({"Link": {'$in': list(alt_hrefs)}}) == 0:
            links_dict = {"Link":a_href, "Source Link":urlink, 
                      "Is Crawled":False, "Last Crawl Dt":None, 
                      "Response Status":None, "Content type":None, 
                      "Content length":None, "File path":None, 
                      "Created at":datetime.datetime.now()}
            links.insert_one(links_dict)
    return

def find_link(links):
    '''
    find the first link that hasn't been crawled in the last 'time_gap' period. 
    'time_gap' from cfg.py
    
    Parameters
    ----------
    links : MongoDB Collection
        Collection (database) of all the links.

    Returns
    -------
    urlink : string, optional
        source link.i.e., link address of the page from which the <a> tag links are to be obtained. The default is None.


    '''
    link_dict = links.find_one({'$or':[{"Is Crawled":False},{'Last Crawl Dt': { "$lt" : datetime.datetime.now()-time_gap}}]})
    if link_dict == None:
        # if all the links are crawled in the database in the last 'time_gap period, cycle ends
        print("All links crawled")
        # sleep for 'sleep_time' seconds. 'sleep_time' from cfg.py
        sleep(sleep_time)
        return None
    urlink = link_dict["Link"]
    
    try:
        requests.get(urlink)
        links.update_one({"Link":urlink},{'$set':{"Is Crawled":True,"Last Crawl Dt":datetime.datetime.now()}})
    except (requests.exceptions.SSLError,requests.exceptions.InvalidSchema,requests.exceptions.TooManyRedirects):
        links.update_one({"Link":urlink},{'$set':{"Is Crawled":True,"Last Crawl Dt":datetime.datetime.now()}})
        return None
    except requests.exceptions.ConnectionError:
        try:
            # check for internet connection
            if requests.get(root):
                links.update_one({"Link":urlink},{'$set':{"Is Crawled":True,"Last Crawl Dt":datetime.datetime.now()}})
                return None
        except (requests.exceptions.ConnectionError, OSError):
            print("Connection Error for " + urlink + "\n")
            return None
        return None
    return urlink


def update_doc(urlink,links):
    '''
    One cycle. crawls a valid source link, saves the response to disk as file and updates the database.

    Parameters
    ----------
    urlink : string, optional
        source link.i.e., link address of the page from which the <a> tag links are to be obtained. The default is None.

    links : MongoDB Collection
        Collection (database) of all the links.

    Returns
    -------
    None.

    '''
    if urlink != None:
        
        r = requests.get(urlink)
        link_dict = links.find_one({"Link":urlink})
        fname = str(link_dict["_id"])
        cont_len = r.headers.get('Content-Length')
        if cont_len == None:
            cont_len = len(r.content)
        try:
            fpath = "./"+fname+"."+r.headers.get('Content-Type').split(';')[0].split('/')[1].strip()
            file= open(fpath,"wb")
            file.write(r.content)
            file.close()
            links.update_one({"Link":urlink},
                         {'$set':{"Is Crawled":True, 
                              "Last Crawl Dt":datetime.datetime.now(), 
                              "Response Status":r.status_code, 
                              "Content type":r.headers.get('Content-Type'), 
                              "Content length":cont_len, 
                              "File path":fpath}})
        except AttributeError:
            pass
            # links.update_one({"Link":urlink},{'$set':{"Is Crawled":True,"Last Crawl Dt":datetime.datetime.now()}})
        soup = BeautifulSoup(r.content, 'html.parser')
        crawl(soup,links,urlink)
        
    return

if __name__ == "__main__":
    
    client = MongoClient()
    db = client['links_database']
    links = db['links']
    # 'root' from cfg.py
    insert_new(root,links )
   
    t1 = threading.Thread(target = update_doc,args = (None,links))
    t2 = threading.Thread(target = update_doc,args = (None,links))
    t3 = threading.Thread(target = update_doc,args = (None,links))
    t4 = threading.Thread(target = update_doc,args = (None,links))
    t5 = threading.Thread(target = update_doc,args = (None,links))
    while(True):
        
        if not t1.is_alive():
            url1 = find_link(links)
            if url1 != None:
                print("next1",url1)
                try:
                    t1 = threading.Thread(target = update_doc,args = (url1,links))
                    t1.start()
                except:
                    pass
                        
        if not t2.is_alive():
            url2 = find_link(links)
            if url2 != None:
                print("next2",url2)
                try:
                    t2 = threading.Thread(target = update_doc,args = (url2,links))
                    t2.start()
                except:
                    pass
    
        if not t3.is_alive():
            url3 = find_link(links)
            if url3 != None:
                print("next3",url3)
                try:
                    t3 = threading.Thread(target = update_doc,args = (url3,links))
                    t3.start()
                except:
                    pass
   
        if not t4.is_alive():
            url4 = find_link(links)
            if url4 != None:
                print("next4",url4)
                try:
                    t4 = threading.Thread(target = update_doc,args = (url4,links))
                    t4.start()
                except:
                    pass
   
        if not t5.is_alive():
            url5 = find_link(links)
            if url5 != None:
                print("next5",url5)
                try:
                    t5 = threading.Thread(target = update_doc,args = (url1,links))
                    t5.start()
                except:
                    pass
        
    
       
    