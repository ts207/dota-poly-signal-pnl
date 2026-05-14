import asyncio

import pytest

from book_refresh import fetch_fresh_book


class _Resp:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return {
            "asks": [
                {"price": "0.61", "size": "10"},
                {"price": "0.57", "size": "4"},
                {"price": "0.59", "size": "0"},
            ],
            "bids": [
                {"price": "0.51", "size": "3"},
                {"price": "0.55", "size": "2"},
                {"price": "0.54", "size": "0"},
            ],
        }


class _Session:
    def get(self, *args, **kwargs):
        return _Resp()


def test_fetch_fresh_book_selects_best_levels_not_first_levels():
    book = asyncio.run(fetch_fresh_book(_Session(), "token"))

    assert book["best_ask"] == pytest.approx(0.57)
    assert book["ask_size"] == pytest.approx(4)
    assert book["best_bid"] == pytest.approx(0.55)
    assert book["bid_size"] == pytest.approx(2)
    assert book["request_start_ns"] <= book["received_at_ns"]
