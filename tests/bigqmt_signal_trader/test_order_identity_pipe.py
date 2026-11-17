"""External persistence durability, isolation and fail-closed submission."""

import os
import time
import uuid
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bigqmt_signal_trader.order_identity_pipe import (
    IdentityDatabase, IdentityPersistenceError, PipeIdentityJournal,
)
from bigqmt_signal_trader.transports.pipe_transport import NamedPipeTransport
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from bigqmt_signal_trader.adapters.order_dryrun import DryRunOrderGateway


def request(db, method, account="test", kind="STOCK", **params):
    return db.handle(dict(request_id=uuid.uuid4().hex, account_id=account,
                          method=method, params=dict(account_type=kind, **params)))


def test_durable_restart_first_owner_and_account_isolation(tmp_path):
    path = tmp_path / "identity.sqlite"
    db = IdentityDatabase(path, "test")
    assert request(db, "remember", remark="r", ts=time.time(), strategy_name="alpha")["ok"]
    db.close()
    db = IdentityDatabase(path, "test")
    try:
        assert request(db, "load")["data"][0][2] == "alpha"
        assert request(db, "remember", remark="r", ts=time.time(),
                       strategy_name="beta")["data"]["strategy_name"] == "alpha"
        assert request(db, "load", kind="CREDIT")["data"] == []
        assert not request(db, "load", account="other")["ok"]
        assert not request(db, "execute_order")["ok"]
    finally:
        db.close()


def test_expiry_and_invalid_record(tmp_path):
    db = IdentityDatabase(tmp_path / "identity.sqlite", "test")
    try:
        db.db.execute("INSERT INTO identities VALUES (?, ?, ?, ?, ?)",
                      ("test", "STOCK", "expired", time.time() - 8 * 86400, "alpha"))
        db.db.commit()
        assert request(db, "load")["data"] == []
        assert not request(db, "remember", remark="r", ts=float("nan"), strategy_name="a")["ok"]
    finally:
        db.close()


def test_database_failure_does_not_acknowledge(tmp_path):
    db = IdentityDatabase(tmp_path / "identity.sqlite", "test")
    db.db.execute("PRAGMA query_only=ON")
    try:
        assert not request(db, "remember", remark="r", ts=time.time(), strategy_name="a")["ok"]
    finally:
        db.close()


@pytest.mark.skipif(os.name != "nt", reason="Windows named pipes")
def test_real_pipe_recovery_and_unavailable_service_blocks_submit(tmp_path):
    pipe = "identity_test_" + uuid.uuid4().hex
    db = IdentityDatabase(tmp_path / "identity.sqlite", "test")
    server = NamedPipeTransport(account_id="test", pipe_name=pipe)
    server.start_receiving(db.handle, background_threads=True)
    options = dict(pipe_name=pipe, timeout_seconds=3.0)
    first = PipeIdentityJournal("test", "STOCK", **options)
    second = PipeIdentityJournal("test", "STOCK", **options)
    gateway = DryRunOrderGateway()
    gateway.account_type = "STOCK"
    gateway.submit = Mock(wraps=gateway.submit)
    handlers = BigQmtRpcHandlers("test", SimpleNamespace(), SimpleNamespace(),
                                order_gateway=gateway, allow_order_methods=True)
    try:
        assert first.remember("test", "r", time.time(), "alpha")["strategy_name"] == "alpha"
        first.close()
        assert second.load()[("test", "r")][1] == "alpha"
        handlers.enable_order_identity_persistence(account_types={"test": "STOCK"}, pipe_config=options)
        server.stop()
        with pytest.raises(IdentityPersistenceError):
            handlers._handle_submit_order(dict(stock_code="600036.SH", action="BUY", volume=100,
                                              price=10, remark="blocked", strategy_name="alpha",
                                              wait_settlement=False))
        gateway.submit.assert_not_called()
        assert ("test", "blocked") not in handlers._order_identity_local
        unavailable = BigQmtRpcHandlers("test", SimpleNamespace(), SimpleNamespace(),
                                        order_gateway=gateway, allow_order_methods=True)
        with pytest.raises(IdentityPersistenceError):
            unavailable.enable_order_identity_persistence(
                account_types={"test": "STOCK"}, pipe_config=options)
        server = NamedPipeTransport(account_id="test", pipe_name=pipe)
        server.start_receiving(db.handle, background_threads=True)
        handlers._remember_order_identity_local("test", "recovered", "alpha")
        assert second.load()[("test", "recovered")][1] == "alpha"
    finally:
        first.close()
        second.close()
        for store in getattr(handlers, "_order_identity_journals", {}).values():
            store.close()
        server.stop()
        db.close()
