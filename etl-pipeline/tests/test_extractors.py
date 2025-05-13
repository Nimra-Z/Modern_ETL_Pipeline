import os
from extractors.csv_extractor import CSVExtractor

def test_csv_extractor_reads_data():
    config = {
        "path": "data/input/sample_data.csv",
        "delimiter": ",",
        "encoding": "utf-8"
    }
    extractor = CSVExtractor(config)
    data = extractor.extract()
    assert isinstance(data, list)
    assert len(data) > 0
    assert "name" in data[0]
