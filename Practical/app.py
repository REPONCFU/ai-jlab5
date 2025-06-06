import os
from datetime import datetime

import pandas as pd

DATA_FILE = "data/students.parquet"


def load_data():
    if os.path.exists(DATA_FILE):
        return pd.read_parquet(DATA_FILE)
    else:
        return pd.DataFrame(
            columns=[
                "ФИО",
                "Группа",
                "Математика",
                "Физика",
                "Информатика",
                "Дата ввода",
            ]
        )


def save_data(df):
    df.to_parquet(DATA_FILE, index=False)


def add_student(fio, group, math, physics, informatics):
    df = load_data()
    new_row = {
        "ФИО": fio,
        "Группа": group,
        "Математика": int(math),
        "Физика": int(physics),
        "Информатика": int(informatics),
        "Дата ввода": datetime.now(),
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.sort_values("ФИО", inplace=True)
    save_data(df)
    print(" Студент добавлен.")


def show_bad_students():
    df = load_data()
    mask = (df[["Математика", "Физика", "Информатика"]] == 2).any(axis=1)
    bad_students = df[mask][["ФИО", "Группа"]]
    if bad_students.empty:
        print(" Студентов с оценкой 2 нет.")
    else:
        print(" Студенты с хотя бы одной двойкой:")
        print(bad_students.to_string(index=False))


def delete_column(column_name):
    df = load_data()
    if column_name in df.columns:
        df.drop(columns=[column_name], inplace=True)
        save_data(df)
        print(f" Колонка '{column_name}' удалена.")
    else:
        print(" Колонка не найдена.")
