import os
import sqlite3

from io_handler import IOHandler
from file_io_handler import FileHandler
from io_handler import CacheData
from filelock import FileLock

database_file = "crawler.db"


class SQLHandler(IOHandler):
    def __init__(self, index):
        super().__init__(index)

        self._file_io_handler = FileHandler(index)
        self._database_file = f"{self._base_folder}\\{database_file}"

        with FileLock(f"{self._database_file}.lock"):
            new_db = not os.path.exists(self._database_file)

            self._connection = sqlite3.connect(self._database_file)
            self._cursor = self._connection.cursor()
            
            if new_db:
                sql_command = """
                CREATE TABLE queue (
                url VARCHAR(300));"""
                self._cursor.execute(sql_command)
                
                sql_command = """
                CREATE TABLE cache (
                domain VARCHAR(300),
                last_visited REAL,
                crawl_delay REAL);"""
                self._cursor.execute(sql_command)

                sql_command = """
                INSERT INTO queue 
                VALUES ("https://www.youtube.com/");"""
                self._cursor.execute(sql_command)

                self._connection.commit()

            self._connection.close()


    def read_queue(self) -> []:
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = "SELECT * FROM queue;"
        self._cursor.execute(sql_command)
        rows = self._cursor.fetchall()

        self._connection.close()

        return [r[0] for r in rows]


    def add_to_queue(self, url):
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = f"""
        INSERT INTO queue 
        VALUES ("{url}");"""
        self._cursor.execute(sql_command)

        self._connection.commit()
        self._connection.close()


    def set_queue(self, urls):
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = """DELETE FROM queue;"""
        self._cursor.execute(sql_command)

        if len(urls) > 0:
            sql_command = f"""
            INSERT INTO queue 
            VALUES {self._format_as_values_str(urls)};"""
            self._cursor.execute(sql_command)
        
        self._connection.commit()
        self._connection.close()


    def erase_from_queue(self, url):
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = f"""
        DELETE FROM queue
        WHERE url = "{url}";"""
        self._cursor.execute(sql_command)
        
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
        self._cursor.execute(sql_command)

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
        INSERT INTO cache 
        VALUES ("{url}", {last_visited}, {crawl_delay});"""
        self._cursor.execute(sql_command)
        
        self._connection.commit()
        self._connection.close()

    
    def record_visit_in_cache(self, domain, last_visited):
        self._connection = sqlite3.connect(self._database_file)
        self._cursor = self._connection.cursor()

        sql_command = f"""
        UPDATE cache 
        SET last_visited = {last_visited}
        WHERE domain = "{domain}";"""
        self._cursor.execute(sql_command)
        
        self._connection.commit()
        self._connection.close()


    def add_to_pages(self, url_title):
        self._file_io_handler.add_to_pages(url_title)


    def read_pages(self) -> []:
        return self._file_io_handler.read_pages()


    def dump(self):
        pass


    def _format_as_values_str(self, values: []) -> str:
        return f"""({"),(".join(['"' + v + '"' for v in values])})"""
        
