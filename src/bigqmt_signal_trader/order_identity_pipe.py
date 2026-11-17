"""Order attribution stored by an external process over a Windows named pipe.

Only PipeIdentityJournal runs inside QMT. SQLite is imported lazily by the
external service; no QMT-side file access or TCP connection is required.
"""

import collections
import math
import time
import uuid

class IdentityPersistenceError(RuntimeError):
    """Durable attribution was not confirmed; do not submit an order."""


class PipeIdentityJournal(object):
    required = True

    def __init__(self, account_id, account_type, pipe_name="bigqmt_identity",
                 timeout_seconds=3.0, **kwargs):
        from .transports.pipe_transport import NamedPipeTransport

        self.account_id = str(account_id)
        self.account_type = str(account_type).upper()
        self.timeout = float(timeout_seconds)
        self.transport = NamedPipeTransport(
            account_id=self.account_id, pipe_name=pipe_name,
            connect_timeout_seconds=self.timeout)
        self.directory = self.transport.path
        self.error = ""

    def _call(self, method, **params):
        params["account_type"] = self.account_type
        request = dict(schema_version=1, request_id=uuid.uuid4().hex,
                       account_id=self.account_id, method=method, params=params)
        try:
            response = self.transport.send_request(request, self.timeout)
            if not response.get("ok"):
                raise RuntimeError(response.get("error", "persistence failed"))
            self.error = ""
            return response["data"]
        except Exception as exc:
            self.error = "%s: %s" % (type(exc).__name__, exc)
            raise IdentityPersistenceError(self.error)

    def load(self):
        rows = self._call("load")
        return collections.OrderedDict(
            ((self.account_id, row[0]), (row[1], row[2])) for row in rows)

    def remember(self, account_id, remark, ts, name):
        if str(account_id) != self.account_id:
            raise IdentityPersistenceError("account mismatch")
        return self._call("remember", remark=remark, ts=ts, strategy_name=name)

    def close(self):
        self.transport.stop()


class IdentityDatabase(object):
    """External-only, transactional store; acknowledgements follow commit."""

    def __init__(self, path, account_id, ttl_seconds=7 * 86400.0, limit=5000):
        import sqlite3
        import threading

        self.account_id = str(account_id)
        self.ttl = float(ttl_seconds)
        self.limit = int(limit)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS identities ("
                        "account TEXT, kind TEXT, remark TEXT, ts REAL, name TEXT, "
                        "PRIMARY KEY(account, kind, remark))")
        self.db.commit()

    def handle(self, request):
        response = dict(schema_version=1, request_id=request.get("request_id"),
                        account_id=self.account_id)
        try:
            if request.get("account_id") != self.account_id:
                raise ValueError("account mismatch")
            params = request.get("params") or {}
            kind = str(params.get("account_type") or "").strip().upper()
            if not kind or len(kind) > 32:
                raise ValueError("account_type is required")
            method = request.get("method")
            if method not in ("load", "remember", "ping"):
                raise ValueError("unsupported persistence method")
            with self.lock, self.db:
                cutoff = time.time() - self.ttl
                self.db.execute("DELETE FROM identities WHERE ts < ?", (cutoff,))
                if method == "remember":
                    remark = params.get("remark")
                    name = params.get("strategy_name")
                    ts = float(params.get("ts", 0))
                    if (not isinstance(remark, str) or not remark.strip()
                            or len(remark) > 1024 or not isinstance(name, str)
                            or len(name) > 1024 or not math.isfinite(ts) or ts <= 0):
                        raise ValueError("invalid identity record")
                    remark = remark.strip()
                    key = (self.account_id, kind, remark)
                    old = self.db.execute(
                        "SELECT ts, name FROM identities WHERE account=? AND kind=? AND remark=?",
                        key).fetchone()
                    # First nonempty ownership wins across clients and restarts.
                    canonical = old[1] if old and old[1] else name
                    stamp = time.time()
                    self.db.execute("INSERT OR REPLACE INTO identities VALUES (?, ?, ?, ?, ?)",
                                    key + (stamp, canonical))
                    data = dict(ts=stamp, strategy_name=canonical)
                elif method == "load":
                    data = self.db.execute(
                        "SELECT remark, ts, name FROM (SELECT remark, ts, name FROM identities "
                        "WHERE account=? AND kind=? ORDER BY ts DESC LIMIT ?) ORDER BY ts",
                        (self.account_id, kind, self.limit)).fetchall()
                else:
                    data = {"pong": True}
            # The transaction has committed before publishing success.
            response.update(ok=True, data=data)
        except Exception as exc:
            response.update(ok=False, error="%s: %s" % (type(exc).__name__, exc))
        return response

    def close(self):
        with self.lock:
            self.db.close()


def main(argv=None):
    import argparse
    from pathlib import Path
    from .transports.pipe_transport import NamedPipeTransport

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--pipe-name", default="bigqmt_identity")
    args = parser.parse_args(argv)
    path = Path(args.database).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    # Prevent a second service for this database from accepting the same pipe.
    # The OS releases this advisory lock when the process exits or crashes.
    import msvcrt
    service_lock = open(str(path) + ".lock", "a+b")
    service_lock.seek(0)
    if not service_lock.read(1):
        service_lock.write(b"0")
        service_lock.flush()
    service_lock.seek(0)
    try:
        msvcrt.locking(service_lock.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        service_lock.close()
        raise RuntimeError("Identity service already running for " + str(path))
    database = IdentityDatabase(path, args.account_id)
    transport = NamedPipeTransport(account_id=args.account_id, pipe_name=args.pipe_name)
    transport.start_receiving(database.handle, background_threads=True)
    print("Identity persistence ready: %s database=%s" % (transport.path, path), flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        transport.stop()
        database.close()
        service_lock.close()


if __name__ == "__main__":
    main()
