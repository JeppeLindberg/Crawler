import requests
from bs4 import BeautifulSoup
import time
from time import sleep
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
from urllib.error import URLError
import validators

useragent = "jwli"

base_folder = "C:\\Users\\Jeppe\\Documents\\Python\\Crawler"
pages = base_folder + "\\pages.txt"
queue = base_folder + "\\queue.txt"
cache = base_folder + "\\cache.txt"
already_visited = base_folder + "\\already_visited.txt"

rp = RobotFileParser()

def crawl():
    while True:
        url = get_next_url()
        if url == None:
            sleep(0.1)
            continue
        # print(url)

        sleep(0.01)

        try:
            r = requests.get(url)
        except:
            continue
        record_visit(url)
        r_parse = BeautifulSoup(r.text, 'html.parser')
        r_title = r_parse.find('title')
        if r_title == None:
            continue

        r_title_string = r_title.string

        for a in r_parse.find_all('a'):
            try:
                add_to_queue(a['href'])
            except KeyError:
                __ = False

        url_title = ("[" + url + "] " + r_title_string).replace("\n", "")
        append_to_file(pages, url_title)
        print(url_title)

        if stop_crawling():
            print("Crawl finished!")
            return

def get_next_url():
    f = open(queue)
    lines = f.readlines()
    f.close()

    for i in range(len(lines)):
        allow_crawl = get_allow_crawl(lines[i].replace("\n", ""))

        if allow_crawl == "-1":
            extract_line_from_file(queue, i)
            return None
        if allow_crawl == "1":
            return extract_line_from_file(queue, i)

    return None

def get_domain(url):
    netloc = urlparse(url).netloc
    split = netloc.split(".")
    domain = split[len(split) - 2] + "." + split[len(split) - 1]
    return domain

def get_allow_crawl(url):
    f = open(cache, "r", encoding="utf-8")
    lines = f.readlines()
    f.close()
    cache_hit = False
    last_visited = 0
    crawl_delay = 2

    allow_delay = False

    for l in lines:
        if cache_hit:
            if "\t" not in l:
                break
            if "Last_visited: " in l:
                l_sub = l[(l.find("Last_visited: ") + 14):]
                last_visited = float(l_sub.replace("\n", ""))
            if "Crawl_delay: " in l:
                l_sub = l[(l.find("Crawl_delay: ") + 13):]
                crawl_delay = float(l_sub.replace("\n", ""))

        if l.replace("\n", "") == get_domain(url):
            cache_hit = True

    if cache_hit:
        allow_delay = time.time() > last_visited + crawl_delay
        
    rp.set_url(url)
    try:
        rp.read()
        if not rp.can_fetch(useragent,url):
            return "-1"
    except:
        return "-1"

    if not cache_hit:
        crawl_delay = rp.crawl_delay(useragent)
        if crawl_delay == None:
            crawl_delay = 5

        append_to_file(cache, get_domain(url) + "\n\tLast_visited: " + str(0) + "\n\tCrawl_delay: " + str(crawl_delay))
        allow_delay = True

    if allow_delay == True:
        return "1"
    return "0"

def record_visit(url):
    f = open(cache)
    lines = f.readlines()
    f.close()
    cache_hit = False

    for i in range(len(lines)):
        if cache_hit:
            if "\t" not in lines[i]:
                return
            if "Last_visited: " in lines[i]:
                update_line_in_file(cache, i, "\tLast_visited: " + str(time.time()))

        if lines[i].replace("\n", "") == get_domain(url):
            cache_hit = True

def add_to_queue(url):
    if not url.endswith("/"):
        url = url + "/"

    if not validators.url(url):
        return

    f = open(already_visited)
    lines = f.readlines()
    f.close()

    for l in lines:
        if l.replace("\n", "") == url:
            return

    append_to_file(queue, url)
    append_to_file(already_visited, url)

def stop_crawling():
    f = open(pages, "r", encoding="utf-8")
    lines = f.readlines()
    f.close()

    if len(lines) >= 1000:
        return True
    else:
        return False

def append_to_file(file_path, string):
    f = open(file_path, "a", encoding="utf-8")
    f.write(string + "\n")
    f.close()

def extract_line_from_file(file_path, line):
    f = open(file_path)
    lines = f.readlines()
    f.close()

    result = lines.pop(line)
    new_content = "".join(lines)

    f = open(file_path, "w")
    f.write(new_content)
    f.close()

    return result.replace("\n", "")

def update_line_in_file(file_path, line_no, new_line):
    f = open(file_path)
    lines = f.readlines()
    f.close()

    lines[line_no] = new_line + "\n"
    new_content = "".join(lines)

    f = open(file_path, "w")
    f.write(new_content)
    f.close()

crawl()
