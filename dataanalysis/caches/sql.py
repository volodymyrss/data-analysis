import pickle
import os
import sqlite3 as lite
import time

from dataanalysis.bcolors import render
from dataanalysis.caches.cache_core import Cache
from dataanalysis.printhook import log


class CacheSqlite(Cache):
    cache = {}

    def statistics(self):
        if self.con is None:
            log("NOT connected")
        else:
            log("connected to", self.con)

    def connect(self):
        if self.con is None:
            log("connecting to", self.filecacheroot + '/index.db')
            self.con = lite.connect(self.filecacheroot + '/index.db', 1000)
        return self.con

    def __init__(self, *a, **aa):
        log(a, aa)
        super(CacheSqlite, self).__init__(*a, **aa)
        self.con = None
        # self.connect()

    def list(self, select=None, nlast=None):

        con = self.connect()
        log("listing cache")

        selection_string = ""
        if select is not None:
            selection_string = " WHERE " + select  # must be string

        nlast_string = ""
        if nlast is not None:
            nlast_string = " ORDER BY rowid DESC LIMIT %i" % nlast  # must be int

        with con:
            cur = con.cursor()

            log("SELECT * FROM cacheindex" + selection_string + nlast_string)

            t0 = time.time()
            self.retry_execute(cur, "SELECT * FROM cacheindex" + selection_string + nlast_string)
            rows = cur.fetchall()
            log("mysql request took", time.time() - t0, "{log:top}")

            log("found rows", len(rows))
            for h, c in rows:
                try:
                    c = pickle.loads(str(c))
                    log(str(h), str(c))
                except Exception as e:
                    log("exception while loading:", e)
                    raise

        return len(rows)

    def retry_execute(self, cur, *a, **aa):
        timeout = aa['timeout'] if 'timeout' in aa else 10
        e = Exception("undefined exception during retry")
        for x in range(timeout):
            try:
                return cur.execute(*a)
            except Exception as e:
                log(render("{RED}sqlite execute failed, try again{/}: " + repr(e)), x)
                time.sleep(1)
        raise e

    def find(self, hashe):

        con = self.connect()

        log("requested to find", hashe)

        with con:
            cur = con.cursor()
            log("now rows", cur.rowcount)

            try:
                self.retry_execute(cur, "SELECT content FROM cacheindex WHERE hashe=?", (self.hashe2signature(hashe),))
            except Exception as e:
                log("failed:", e)
                return None
            # cur.execute("SELECT content FROM cacheindex WHERE hashe=?",(self.hashe2signature(hashe),))
            try:
                rows = cur.fetchall()
            except Exception as e:
                log("exception while fetching", e)
                return None

        if len(rows) == 0:
            log("found no cache")
            return None

        if len(rows) > 1:
            log("multiple entries for same cache!")
            # raise Exception("confused cache! mupltile entries! : "+str(rows))
            log("confused cache! mupltile entries! : " + str(rows), "{log:reflections}")
            log("confused cache will run it again", "{log:reflections}")
            return None

        return pickle.loads(str(rows[0][0]))

    def make_record(self, hashe, content):

        log("will store", hashe, content)

        # con = lite.connect(self.filecacheroot+'/index.db')
        con = self.connect()

        c = pickle.dumps(content)
        log("content as", c)

        with con:
            cur = con.cursor()
            self.retry_execute(cur, "CREATE TABLE IF NOT EXISTS cacheindex(hashe TEXT, content TEXT)")
            self.retry_execute(cur, "INSERT INTO cacheindex VALUES(?,?)", (self.hashe2signature(hashe), c))

            log("now rows", cur.rowcount)

    def load_content(self, hashe, c):
        log("restoring from sqlite")
        log("content", c['content'])
        return c['content']


class CacheMySQL(CacheSqlite):
    cache = {}

    # also to object
    total_attempts = 0
    failed_attempts = 0

    def statistics(self):
        if self.con is None:
            log("NOT connected")
        else:
            log("connected to", self.con)
        log("operations total/failed", self.total_attempts, self.failed_attempts)

    def connect(self):
        if not hasattr(self, 'mysql_enabled'):
            raise Exception("mysql disabled")
        else:
            import MySQLdb
            if self.db is None:
                log("connecting to mysql")
                self.db = MySQLdb.connect(host="apcclwn12",  # your host, usually localhost
                                          user="root",  # your username
                                          port=42512,
                                          # unix_socket="/workdir/savchenk/mysql/var/mysql.socket",
                                          passwd=open(os.environ['HOME'] + "/.secret_mysql_password").read().strip(),
                                          # your password
                                          db="ddacache")  # name of the data base

        return self.db

    def __init__(self, *a, **aa):
        log(a, aa)
        super(CacheMySQL, self).__init__(*a, **aa)
        self.db = None
        # self.connect()

    def list(self, select=None, nlast=None):

        con = self.connect()
        log("listing cache")

        selection_string = ""
        if select is not None:
            selection_string = " WHERE " + select  # must be string

        nlast_string = ""
        if nlast is not None:
            nlast_string = " ORDER BY rowid DESC LIMIT %i" % nlast  # must be int

        with con:
            cur = con.cursor()

            log("SELECT * FROM cacheindex" + selection_string + nlast_string)

            self.retry_execute(cur, "SELECT * FROM cacheindex" + selection_string + nlast_string)
            rows = cur.fetchall()

            log("found rows", len(rows))
            for h, fh, c in rows:
                try:
                    c = pickle.loads(str(c))
                    log(str(h), str(c))
                except Exception as e:
                    log("exception while loading:", e)
                    raise

        return len(rows)

    def retry_execute(self, cur, *a, **aa):
        timeout = aa['timeout'] if 'timeout' in aa else 10

        e=Exception("while retry_execute")
        for x in range(timeout):
            try:
                log(a)
                self.total_attempts += 1
                return cur.execute(*a)
            except Exception as e:
                self.failed_attempts += 1
                log(render("{RED}mysql execute failed, try again{/}: " + repr(e)), x)
                time.sleep(1)
        raise e

    def find(self, hashe):

        log("requested to find", hashe)
        log("hashed", hashe, "as", self.hashe2signature(hashe))

        db = self.connect()

        if True:
            cur = db.cursor()
            log("now rows", cur.rowcount)

            try:
                t0 = time.time()
                self.retry_execute(cur, "SELECT content FROM cacheindex WHERE hashe=%s", (self.hashe2signature(hashe),))
                log("mysql request took", time.time() - t0, "{log:top}")
            except Exception as e:
                log("failed:", e)
                return None
            # cur.execute("SELECT content FROM cacheindex WHERE hashe=?",(self.hashe2signature(hashe),))
            rows = cur.fetchall()

        if len(rows) == 0:
            log("found no cache")
            return None

        if len(rows) > 1:
            log("multiple entries for same cache!")
            log(rows)
            return None
            # raise Exception("confused cache! mupltile entries!")

        return pickle.loads(str(rows[0][0]))

    def make_record(self, hashe, content):
        import json

        log("will store", hashe, content)

        # con = lite.connect(self.filecacheroot+'/index.db')
        db = self.connect()

        c = pickle.dumps(content)
        log("content as", c)

        if "_da_cached_path" in content:
            aux1 = content['_da_cached_path']
        else:
            aux1 = ""

        with db:
            cur = db.cursor()
            self.retry_execute(cur, "CREATE TABLE IF NOT EXISTS cacheindex(hashe TEXT, fullhashe TEXT, content TEXT)")
            self.retry_execute(cur,
                               "INSERT INTO cacheindex (hashe,fullhashe,content,timestamp,refdir) VALUES(%s,%s,%s,%s,%s)",
                               (self.hashe2signature(hashe), json.dumps(hashe), c, time.time(), aux1))

            log("now rows", cur.rowcount)

    def load_content(self, hashe, c):
        log("restoring from sqlite")
        log("content", c['content'])
        return c['content']

    def make_delegation_record(self, hashe, module_description, dependencies):
        import json

        log("will store", hashe, module_description)

        # con = lite.connect(self.filecacheroot+'/index.db')
        db = self.connect()

        shorthashe = self.hashe2signature(hashe)

        if dependencies is not None and dependencies != []:  # two??..
            status = "waiting for:" + ",".join(dependencies)  # comas?
        else:
            status = "ready to run"

        with db:
            cur = db.cursor()
            self.retry_execute(cur,
                               "CREATE TABLE IF NOT EXISTS delegationindex(id MEDIUMINT NOT NULL AUTO_INCREMENT, timestamp DOUBLE, hashe TEXT, fullhashe TEXT, modules TEXT, status TEXT, PRIMARY KEY (id))")
            self.retry_execute(cur,
                               "INSERT INTO delegationindex (timestamp,hashe,fullhashe,modules,status) VALUES(%s,%s,%s,%s,%s)",
                               (time.time(), shorthashe, json.dumps(hashe), json.dumps(module_description), status))

            log("now rows", cur.rowcount)

        return shorthashe
