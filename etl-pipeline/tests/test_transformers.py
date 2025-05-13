from src.transformers.data_cleaner import DataCleaner

def test_data_cleaner_removes_duplicates():
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
    config = {"rules": {"remove_duplicates": True}}
    cleaner = DataCleaner(config)
    cleaned = cleaner.transform(data)
    assert len(cleaned) == 2
