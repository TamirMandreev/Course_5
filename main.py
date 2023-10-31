from utils import get_data_employees, get_vacancies_employer
from config import config
from class_engine import DBManager


def create_and_fill_tables(db, employees_ids):
    employees_column = 'employer_id int PRIMARY KEY, company_name varchar(100) NOT NULL, vacancies_url varchar NOT NULL'
    db.create_tables('employees', employees_column)
    db.clear_table('employees')

    vacancies_column = """vacancy_id int PRIMARY KEY, employer_id int NOT NULL,
                            salary int, vacancy_name varchar(150) NOT NULL, vacancy_url varchar NOT NULL"""
    db.create_tables('vacancies', vacancies_column)
    db.clear_table('vacancies')

    employees_data = get_data_employees(employees_ids)
    data_company = [(item['employer_id'], item['name_company'], item['vacancies_url']) for item in employees_data]
    for data in data_company:
        db.insert_data_into_db('employees', data)

    vacancies_data = get_vacancies_employer(employees_ids)
    data_vacancies = [(item['vacancy_id'], item['employer_id'], item['salary'], item['vacancy_name'],
                       item['vacancy_url']) for item in vacancies_data]
    for data in data_vacancies:
        db.insert_data_into_db('vacancies', data)


def main():
    employees_ids = [
        5998412,  # Surf IT
        1062788,  # Napoleon IT
        5591530,  # IT-hunters
        9697721,  # Ventra IT Solutions
        105904,  # Yota
        8893,  # АБСОЛЮТ, Группа
        26250,  # Технопарк
        816,  # MERLION
        10422,  # Роза Хутор
        3529  # Сбер
    ]

    params = config()
    db = DBManager(**params)

    create_and_fill_tables(db, employees_ids)

    print(db.get_companies_and_vacancies_count())
    print(db.get_all_vacancies())
    print(db.get_avg_salary())
    print(db.get_vacancies_with_higher_salary())
    print(db.get_vacancies_with_keyword('junior'))


if __name__ == '__main__':
    main()