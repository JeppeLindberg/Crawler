import os
import sqlite3
import random

from io_handler import IOHandler
from io_handler import CacheData
from filelock import FileLock

database_file = "crawler.db"


class SQLHandler(IOHandler):
    def __init__(self, index):
        super().__init__(index)

        self._database_file = f"{self._base_folder}\\{database_file}"

        with FileLock(f"{self._database_file}.lock"):
            new_db = not os.path.exists(self._database_file)

            self._connection = sqlite3.connect(self._database_file)
            self._cursor = self._connection.cursor()
            
            if new_db:
                sql_command = """
                CREATE TABLE queue (
                url VARCHAR(300) UNIQUE,
                already_crawled BOOL DEFAULT False NOT NULL);"""
                self._execute(sql_command)
                
                sql_command = """
                CREATE TABLE cache (
                domain VARCHAR(100) PRIMARY KEY,
                last_visited REAL,
                crawl_delay REAL) WITHOUT ROWID;"""
                self._execute(sql_command)
                
                sql_command = """
                CREATE TABLE pages (
                url VARCHAR(300) UNIQUE,
                title VARCHAR(300));"""
                self._execute(sql_command)

                sql_command = """
                INSERT INTO queue (url)
                VALUES ("https://www.youtube.com/");"""
                self._execute(sql_command)

                self._connection.commit()

            self._connection.close()


    def read_queue(self) -> []:
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = """
        SELECT COUNT(*) FROM queue
        WHERE already_crawled = False;"""
        self._execute(sql_command)
        rows = self._cursor.fetchall()

        count = rows[0][0]
        offset = count - 100
        if offset > 0:
            offset = random.randrange(0,offset)
        else:
            offset = 0

        sql_command = f"""
        SELECT * FROM queue
        WHERE already_crawled = False
        LIMIT 100 OFFSET {offset};"""
        self._execute(sql_command)
        rows = self._cursor.fetchall()

        self._connection.close()

        return [r[0] for r in rows]


    def add_to_queue(self, urls):
        if len(urls) == 0:
            return

        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = f"""
        INSERT OR IGNORE INTO queue (url)
        VALUES {self._format_as_values_str(urls)};"""
        self._execute(sql_command)

        self._connection.commit()
        self._connection.close()
        

    def set_queue(self, urls):
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = """DELETE FROM queue;"""
        self._execute(sql_command)

        if len(urls) > 0:
            sql_command = f"""
            INSERT INTO queue (url)
            VALUES {self._format_as_values_str(urls)};"""
            self._execute(sql_command)
        
        self._connection.commit()
        self._connection.close()


    def erase_from_queue(self, url):
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = f"""
        UPDATE queue
        SET already_crawled = True
        WHERE url = "{url}";"""
        self._execute(sql_command)
        
        self._connection.commit()
        self._connection.close()


    def read_cache(self, domain) -> CacheData:
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = f"""
        SELECT * FROM cache 
        WHERE domain = "{domain}"
        LIMIT 1;
        """
        self._execute(sql_command)

        rows = self._cursor.fetchall()

        self._connection.close()

        if len(rows) == 0:
            return CacheData(False, 0, 0)
        r = rows[0]
        return CacheData(True, r[1], r[2])


    def write_cache(self, url, last_visited, crawl_delay):
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = f"""
        INSERT OR REPLACE INTO cache
        VALUES ("{url}", {last_visited}, {crawl_delay});"""
        self._execute(sql_command)
        
        self._connection.commit()
        self._connection.close()

    
    def record_visit_in_cache(self, domain, last_visited):
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = f"""
        UPDATE cache 
        SET last_visited = {last_visited}
        WHERE domain = "{domain}";"""
        self._execute(sql_command)
        
        self._connection.commit()
        self._connection.close()


    def add_to_pages(self, url, title):
        if title is None:
            title = "NULL"
        else:
            title = "\"" + title "\""

        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = f"""
        INSERT OR REPLACE INTO pages
        VALUES ("{url}", "{title}");"""
        self._execute(sql_command)

        self._connection.commit()
        self._connection.close()


    def count_pages(self):
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = """
        SELECT COUNT(*) FROM pages;"""
        self._execute(sql_command)

        rows = self._cursor.fetchall()
        count = rows[0][0]

        self._connection.commit()
        self._connection.close()

        return count


    def dump(self):
        pass


    def _execute(self,sql_command):
        try:
            self._cursor.execute(sql_command)
        except:
            print(f"\nError during SQL command:\n{sql_command}\n\n")


    def _format_as_values_str(self, values: []) -> str:
        return f"""({"),(".join(['"' + v + '"' for v in values])})"""
        
