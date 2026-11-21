import types
import sys

import pytest


@pytest.fixture(autouse=True)
def forbid_database_connections(monkeypatch):
    import portfolio_manager

    original_connection_factory = portfolio_manager.get_db_connection

    def blocked_connection(*args, **kwargs):
        raise AssertionError("Tests must mock database access; real database connections are forbidden.")

    monkeypatch.setattr(portfolio_manager, "get_db_connection", blocked_connection)
    for module in tuple(sys.modules.values()):
        if (
            isinstance(module, types.ModuleType)
            and vars(module).get("get_db_connection") is original_connection_factory
        ):
            monkeypatch.setattr(module, "get_db_connection", blocked_connection)
