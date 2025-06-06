from datetime import datetime

import pandas as pd

from app import add_student, load_data, save_data


def test_add_student(tmp_path):
    test_file = tmp_path / "test.parquet"
    save_data.__globals__["DATA_FILE"] = str(test_file)
    add_student("Тестов Т.Т.", "ИКТ-999", 5, 4, 3)
    df = pd.read_parquet(test_file)
    assert not df.empty
    assert df.iloc[0]["ФИО"] == "Тестов Т.Т."
