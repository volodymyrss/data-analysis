import socket

from dataanalysis.printhook import get_local_log

log=get_local_log(__name__)

def report_runtime(report_runtime_destination, runtime_id, message, version):
    try:
        if not report_runtime_destination.startswith("mysql://"):
            return
        dbname, table = report_runtime_destination[8:].split(".")
        log("state goes to", dbname, table)

        db = None

        if runtime_id is None:
            import random

            runtime_id = random.randint(0, 10000000)

        cur = db.cursor()
        cur.execute(
            "INSERT INTO "
            + table
            + " (analysis,host,date,message,id) VALUES (%s,%s,NOW(),%s,%s)",
            (version, socket.gethostname(), message, runtime_id),
        )

        db.commit()
        db.close()

    except Exception as e:
        log("failed:", e)
