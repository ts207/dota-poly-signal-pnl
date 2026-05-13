from poly_ws import BookStore, ingest_ws_event


def test_book_snapshot_top_of_book():
    store = BookStore()
    ingest_ws_event({
        "event_type": "book",
        "asset_id": "A",
        "bids": [{"price": "0.41", "size": "10"}, {"price": "0.42", "size": "5"}],
        "asks": [{"price": "0.45", "size": "7"}, {"price": "0.44", "size": "3"}],
    }, store)
    book = store.get("A")
    assert book["best_bid"] == 0.42
    assert book["bid_size"] == 5
    assert book["best_ask"] == 0.44
    assert book["ask_size"] == 3


def test_price_change_updates_top():
    store = BookStore()
    ingest_ws_event({
        "asset_id": "A",
        "bids": [{"price": "0.40", "size": "10"}],
        "asks": [{"price": "0.50", "size": "10"}],
    }, store)
    ingest_ws_event({
        "event_type": "price_change",
        "changes": [{"asset_id": "A", "side": "BUY", "price": "0.43", "size": "2"}],
    }, store)
    assert store.get("A")["best_bid"] == 0.43
